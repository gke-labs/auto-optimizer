// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package run

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/revlist"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

type mockProcessServer struct {
	processv1.UnimplementedProcessServiceServer

	mu       sync.Mutex
	commands [][]string
	exitCode int32
	stderr   string
	stdout   string
	err      error
}

func (m *mockProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.Config != nil {
		m.commands = append(m.commands, req.Config.Command)
	}
	if m.err != nil {
		return nil, m.err
	}

	return &processv1.ExecuteResponse{
		ExitCode: m.exitCode,
		Stdout:   []byte(m.stdout),
		Stderr:   []byte(m.stderr),
	}, nil
}

func setupTestHarness(t *testing.T, procSrv processv1.ProcessServiceServer) (*session.Harness, *fakeagents.Clientset, *fakek8s.Clientset) {
	return setupTestHarnessWithLLM(t, procSrv, nil)
}

func setupTestHarnessWithLLM(t *testing.T, procSrv processv1.ProcessServiceServer, llmClient llm.ChatClient) (*session.Harness, *fakeagents.Clientset, *fakek8s.Clientset) {
	t.Helper()

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	sandboxClient := session.NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	if procSrv != nil {
		lis := bufconn.Listen(1024 * 1024)
		grpcServer := grpc.NewServer()
		processv1.RegisterProcessServiceServer(grpcServer, procSrv)
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		t.Cleanup(func() {
			grpcServer.Stop()
			_ = lis.Close()
		})

		sandboxClient.SetProcessClientFactory(func(ctx context.Context, sb *session.Sandbox) (processv1.ProcessServiceClient, func(), error) {
			conn, err := grpc.NewClient("passthrough://bufnet",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return lis.Dial()
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				return nil, nil, err
			}
			return processv1.NewProcessServiceClient(conn), func() { _ = conn.Close() }, nil
		})
	}

	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	opts := session.RunOptions{
		Namespace:             "ao-sandboxes",
		ControlPlaneNamespace: "ao-system",
		SigningKey:            privKey,
	}

	var toolsRegistry *tools.Registry
	if llmClient != nil {
		toolsRegistry = tools.NewRegistry()
		toolsRegistry.Add(&tools.RunCommand{})
	}

	harness := session.NewHarness(llmClient, sandboxClient, toolsRegistry, opts)
	return harness, agentsCS, coreCS
}

func reconcileWorkingRun(ctx context.Context, orch *Orchestrator, req Request) (reconcile.Result, error) {
	// First pass starts the worker
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		return res, err
	}
	// Wait for worker to finish
	deadline := time.Now().Add(5 * time.Second)
	for orch.isRunActive(req.RunID) {
		if time.Now().After(deadline) {
			return res, fmt.Errorf("timed out waiting for worker on run %s", req.RunID)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Second pass consumes the outcome
	return orch.Reconcile(ctx, req)
}

func TestOrchestrator_PendingToWorking(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-abcd"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	const author = "auto-optimizer <auto-optimizer@users.noreply.github.com>"
	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: author,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile Pending run.
	res, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// 1. After the first step the run has a SandboxName, is in Preparing phase, and has ReadyDeadline set.
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}
	if gotRun.SandboxName != "run-20261030t120000z-abcd" {
		t.Errorf("expected sandbox name 'run-20261030t120000z-abcd', got %q", gotRun.SandboxName)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline to be set after step 1")
	}

	// 2. Verify fake Sandbox was created with the run label and repo annotation
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected sandbox to exist in Kubernetes, got err: %v", err)
	}
	if sb.Labels[LabelRun] != runID {
		t.Errorf("expected sandbox label %s=%q, got %q", LabelRun, runID, sb.Labels[LabelRun])
	}
	if sb.Annotations[AnnotationRepo] != repo.Key().String() {
		t.Errorf("expected sandbox annotation %s=%q, got %q", AnnotationRepo, repo.Key().String(), sb.Annotations[AnnotationRepo])
	}

	// 3. Worker is free: no clone or checkout commands have executed yet
	procSrv.mu.Lock()
	cmds := procSrv.commands
	procSrv.mu.Unlock()
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands executed in step 1, got %d: %+v", len(cmds), cmds)
	}

	// 4. Update the fake Sandbox's status to Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating fake sandbox to Ready: %v", err)
	}

	// Step 2: Second reconcile step executes clone and moves run to Working.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}
	if gotRun.ReadyDeadline != nil {
		t.Errorf("expected ReadyDeadline to be cleared in Working, got %v", gotRun.ReadyDeadline)
	}

	// 5. Verify sandbox still exists in Kubernetes (not deleted)
	sb, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected sandbox to exist in Kubernetes, got err: %v", err)
	}
	if sb.Name != gotRun.SandboxName {
		t.Errorf("expected sandbox name %s, got %s", gotRun.SandboxName, sb.Name)
	}

	// 6. Verify clone, checkout, and git config commands were issued directly without shell
	procSrv.mu.Lock()
	cmds = procSrv.commands
	procSrv.mu.Unlock()

	if len(cmds) != 4 {
		t.Fatalf("expected exactly 4 commands executed in sandbox (clone, checkout, 2 git config), got %d: %+v", len(cmds), cmds)
	}

	cloneCmdStr := strings.Join(cmds[0], " ")
	if cloneCmdStr != "git clone -- "+repo.URL+" /workspace/repo" {
		t.Errorf("expected clone command 'git clone -- %s /workspace/repo', got: %s", repo.URL, cloneCmdStr)
	}

	checkoutCmdStr := strings.Join(cmds[1], " ")
	if checkoutCmdStr != "git -C /workspace/repo checkout commit-base123" {
		t.Errorf("expected checkout command 'git -C /workspace/repo checkout commit-base123', got: %s", checkoutCmdStr)
	}

	configNameCmdStr := strings.Join(cmds[2], " ")
	if configNameCmdStr != "git -C /workspace/repo config user.name auto-optimizer" {
		t.Errorf("expected config user.name command 'git -C /workspace/repo config user.name auto-optimizer', got: %s", configNameCmdStr)
	}

	configEmailCmdStr := strings.Join(cmds[3], " ")
	if configEmailCmdStr != "git -C /workspace/repo config user.email auto-optimizer@users.noreply.github.com" {
		t.Errorf("expected config user.email command 'git -C /workspace/repo config user.email auto-optimizer@users.noreply.github.com', got: %s", configEmailCmdStr)
	}

	// 7. Verify events.jsonl contains expected events
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var phases []state.RunPhase
	var foundIdentityEvent bool
	for _, e := range events {
		phases = append(phases, e.Phase)
		if e.Kind == "git_identity_configured" && strings.Contains(e.Message, author) {
			foundIdentityEvent = true
		}
	}
	if len(events) < 6 {
		t.Fatalf("expected at least 6 events (preparing phase, sandbox ready, clone command, checkout command, git identity configured, working phase), got %d: %+v", len(events), events)
	}
	if !foundIdentityEvent {
		t.Errorf("expected git identity configured event in events: %+v", events)
	}
	if phases[0] != state.PhasePreparing || phases[len(phases)-1] != state.PhaseWorking {
		t.Errorf("unexpected event phases: %v", phases)
	}
}

func TestOrchestrator_ConfigureGitIdentityFailure(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-gitfail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "ok\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: "invalid-author<",
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}

	// Mark fake sandbox as Ready
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Reconcile Preparing run; clone/checkout succeed, but git identity fails.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("orch.Reconcile completed (errors handled)")
	}

	// 1. Verify run is in Failed phase
	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "invalid author") {
		t.Errorf("expected error to mention 'invalid author', got: %s", gotRun.Error)
	}

	// 2. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but it still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 3. Verify events recorded failure event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFailureEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && strings.Contains(e.Message, "invalid author") {
			foundFailureEvent = true
			break
		}
	}
	if !foundFailureEvent {
		t.Errorf("expected a PhaseFailed event mentioning invalid author in events.jsonl, got: %+v", events)
	}
}

func TestOrchestrator_CloneFailure(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-fail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "bad-commit-sha",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	// Simulate clone failure with exit code 128
	procSrv := &mockProcessServer{
		exitCode: 128,
		stderr:   "fatal: reference is not a tree: bad-commit-sha\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected phase %s after step 1, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Mark fake sandbox as Ready
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: reconcile Preparing run with Ready sandbox; clone fails.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("orch.Reconcile completed (errors logged or returned)")
	}

	// 1. Verify run is in Failed phase
	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "128") {
		t.Errorf("expected error to mention exit code 128, got: %s", gotRun.Error)
	}

	// 2. Verify sandbox was deleted from Kubernetes
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but it still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error for deleted sandbox, got: %v", err)
	}

	// 3. Verify events recorded command failure and transition to Failed
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var hasFailureEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed {
			hasFailureEvent = true
			break
		}
	}
	if !hasFailureEvent {
		t.Errorf("expected a PhaseFailed event in events.jsonl, got: %+v", events)
	}
}

func TestOrchestrator_Preparing_DeleteSandboxFailsRun(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-deleted"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile creates sandbox and moves to Preparing
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected Preparing phase, got %s", gotRun.Phase)
	}

	// Delete sandbox before readiness
	if err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, gotRun.SandboxName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete sandbox: %v", err)
	}

	// Step 2: Reconcile detects sandbox is gone and transitions run to Failed
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected Failed phase, got %s", gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "deleted before becoming ready") {
		t.Errorf("expected error to mention deleted before becoming ready, got: %q", gotRun.Error)
	}

	// Verify events contain failure event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var failureEventFound bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && strings.Contains(e.Message, "deleted before becoming ready") {
			failureEventFound = true
			break
		}
	}
	if !failureEventFound {
		t.Errorf("expected failure event mentioning deleted before becoming ready in events: %+v", events)
	}
}

func TestOrchestrator_Preparing_NeverReadyDeadlinePasses(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-timeout"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	currentTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		Harness:      harness,
		Clock:        func() time.Time { return currentTime },
		ReadyTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile creates sandbox and sets ReadyDeadline = currentTime + 10s
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected Preparing phase, got %s", gotRun.Phase)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline to be recorded on run")
	}

	// Advance clock past the deadline
	currentTime = currentTime.Add(15 * time.Second)

	// Step 2: Reconcile when deadline has passed fails the run
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected Failed phase, got %s", gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "sandbox not ready before deadline") {
		t.Errorf("expected error to mention sandbox not ready before deadline, got: %q", gotRun.Error)
	}

	// Verify sandbox was cleaned up
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted after timeout, but it still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// Verify events contain failure event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var timeoutEventFound bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && strings.Contains(e.Message, "sandbox not ready before deadline") {
			timeoutEventFound = true
			break
		}
	}
	if !timeoutEventFound {
		t.Errorf("expected failure event mentioning sandbox not ready before deadline in events: %+v", events)
	}
}

func TestOrchestrator_Resume_PreparingEnqueued(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-preparing"
	deadline := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	run := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		GoalName:      "benchmark",
		Phase:         state.PhasePreparing,
		BaseCommit:    "commit-abc",
		SandboxName:   "run-20261030t120000z-preparing",
		ReadyDeadline: &deadline,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	// Pre-create the sandbox in Kubernetes
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      run.SandboxName,
			Namespace: "ao-sandboxes",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 5, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("orch.Resume: %v", err)
	}

	// 1. Verify run remains in Preparing phase (not failed)
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// 2. Verify sandbox still exists
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Errorf("expected sandbox %s to exist, got: %v", run.SandboxName, err)
	}

	// 3. Verify notification was sent
	select {
	case evt := <-orch.notifyCh:
		if evt.Object.RunID != runID {
			t.Errorf("expected enqueued run %s, got %s", runID, evt.Object.RunID)
		}
	default:
		t.Error("expected Preparing run to be enqueued on Resume")
	}
}

func TestOrchestrator_Working_StartsSession_ModelStops_NoImprovement(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-worksess"
	sandboxName := "run-20261030t120000z-worksess"
	goalBody := "read the README and summarise it"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "readme-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-123",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "readme-goal",
			Body: goalBody,
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "# My Repo\nThis is a readme.\n",
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, &llm.ElizaClient{})

	// Pre-create the sandbox in fake k8s
	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify run is marked NoImprovement
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.SessionID == "" {
		t.Fatal("expected run.SessionID to be populated")
	}

	// 2. Verify events recorded "session ended without a pull request"
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundNoImpEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Message == "session ended without a pull request" {
			foundNoImpEvent = true
			break
		}
	}
	if !foundNoImpEvent {
		t.Errorf("expected event 'session ended without a pull request', got events: %+v", events)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 4. Verify session transcript in sessionStore contains the goal prompt, tool call, and tool result
	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected session messages, got 0")
	}
	firstMsg := msgs[0]
	if firstMsg.Role != "system" {
		t.Errorf("first message role = %q, want system", firstMsg.Role)
	}
	if !strings.Contains(*firstMsg.Content, goalBody) {
		t.Errorf("expected system prompt to contain goal body %q, got %q", goalBody, *firstMsg.Content)
	}
	if !strings.Contains(*firstMsg.Content, repo.URL) || !strings.Contains(*firstMsg.Content, "/workspace/repo") {
		t.Errorf("expected system prompt to contain repo facts, got %q", *firstMsg.Content)
	}
	if !strings.Contains(*firstMsg.Content, "Never edit .goals/ or the metric script") {
		t.Errorf("expected system prompt to contain rules, got %q", *firstMsg.Content)
	}
}

type loopingLLM struct {
	mu        sync.Mutex
	callCount int
}

func (l *loopingLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	l.mu.Lock()
	l.callCount++
	callIdx := l.callCount
	l.mu.Unlock()

	args, _ := json.Marshal(map[string]string{"command": "echo loop"})
	return &llm.ChatCompletionResponse{
		ID: "looping-llm",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   fmt.Sprintf("loop-%d", callIdx),
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "run_command",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_MaxTurns(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-maxturns"
	sandboxName := "run-20261030t120000z-maxturns"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "loop-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-loop",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "loop-goal",
			Body: "run forever",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "loop\n",
	}
	mockLLM := &loopingLLM{}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, mockLLM)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     3,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify LLM stopped after exactly MaxTurns
	mockLLM.mu.Lock()
	calls := mockLLM.callCount
	mockLLM.mu.Unlock()
	if calls != 3 {
		t.Errorf("expected exactly 3 LLM calls before max turns stopped it, got %d", calls)
	}

	// 2. Verify run is marked NoImprovement
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}
}

func TestOrchestrator_Working_ExpiredSandbox_AtStart(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-expired"
	sandboxName := "run-20261030t120000z-expired"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "expired-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-exp",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, _, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})
	// Sandbox is intentionally NOT created in fake k8s (already expired/deleted)

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("reconcileWorkingRun completed (errors handled)")
	}

	// Verify run transitioned to Failed
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "expired or deleted") && !strings.Contains(gotRun.Error, "not found") {
		t.Errorf("expected error to mention expired or deleted, got: %s", gotRun.Error)
	}

	// Verify failure event was recorded
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFailEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && (strings.Contains(e.Message, "expired or deleted") || strings.Contains(e.Message, "not found")) {
			foundFailEvent = true
			break
		}
	}
	if !foundFailEvent {
		t.Errorf("expected failure event mentioning expired or deleted sandbox, got events: %+v", events)
	}
}

type deleteSandboxLLM struct {
	agentsCS *fakeagents.Clientset
	sbName   string
}

func (d *deleteSandboxLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	// Simulate sandbox expiring/being deleted in k8s during turn
	_ = d.agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, d.sbName, metav1.DeleteOptions{})

	args, _ := json.Marshal(map[string]string{"command": "echo after deletion"})
	return &llm.ChatCompletionResponse{
		ID: "delete-llm",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   "call-after-del",
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "run_command",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_ExpiredSandbox_DuringSession(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-expduring"
	sandboxName := "run-20261030t120000z-expduring"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "expired-during",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-exp2",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{}
	delLLM := &deleteSandboxLLM{sbName: sandboxName}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, delLLM)
	delLLM.agentsCS = agentsCS

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("reconcileWorkingRun completed (errors handled)")
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "expired or was deleted") {
		t.Errorf("expected error to mention expired or deleted, got: %s", gotRun.Error)
	}
}

func TestOrchestrator_PendingToWorkingToNoImprovement(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-fullflow"
	goalBody := "read the README and summarise it"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "readme-goal",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
		Goal: &goals.Goal{
			Name: "readme-goal",
			Body: goalBody,
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "# My Repo\nThis is a readme.\n",
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, &llm.ElizaClient{})

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Pending -> Preparing
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile 1 (step 1): %v", err)
	}

	// Mark sandbox Ready
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, "run-20261030t120000z-fullflow", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Preparing -> Working
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile 2 (step 2): %v", err)
	}

	// Step 3: Working -> NoImprovement
	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("reconcileWorkingRun 3: %v", err)
	}

	// 1. Verify run reached NoImprovement
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.SessionID == "" {
		t.Fatal("expected run.SessionID to be populated")
	}

	// 2. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted after session, but still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 3. Verify event recorded
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundNoImpEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Message == "session ended without a pull request" {
			foundNoImpEvent = true
			break
		}
	}
	if !foundNoImpEvent {
		t.Errorf("expected event 'session ended without a pull request', got events: %+v", events)
	}
}

type scriptedFinishLLM struct {
	summary string
}

func (s *scriptedFinishLLM) CreateChatCompletion(_ context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	args, _ := json.Marshal(map[string]string{"summary": s.summary})
	return &llm.ChatCompletionResponse{
		ID: "finish-llm",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   "call-finish-1",
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "finish",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_FinishTool_EndsSession_RecordsSummary(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-finishrun"
	sandboxName := "run-20261030t120000z-finishrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "finish-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-finish",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "finish-goal",
			Body: "achieve optimization and finish",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	finishSummary := "Successfully investigated and finished with no PR needed."
	finishLLM := &scriptedFinishLLM{summary: finishSummary}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, finishLLM)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify run is in NoImprovement and has the summary recorded
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.Summary != finishSummary {
		t.Errorf("expected run.Summary %q, got %q", finishSummary, gotRun.Summary)
	}

	// 2. Verify events recorded "finished" event with the summary as message
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFinishedEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Kind == "finished" && e.Message == finishSummary {
			foundFinishedEvent = true
			break
		}
	}
	if !foundFinishedEvent {
		t.Errorf("expected finished event with message %q, got events: %+v", finishSummary, events)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 4. Verify session transcript in sessionStore contains system prompt, user prompt, finish call, and tool result
	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages in 1 turn, got %d: %+v", len(msgs), msgs)
	}
	if len(msgs[2].ToolCalls) == 0 || msgs[2].ToolCalls[0].Function.Name != "finish" {
		t.Errorf("tool call = %+v, want finish", msgs[2].ToolCalls)
	}
	if msgs[3].Role != "tool" || msgs[3].Content == nil || *msgs[3].Content != finishSummary {
		t.Errorf("tool message = %+v, want role tool and content %q", msgs[3], finishSummary)
	}
}

type scriptedErrorThenFinishLLM struct {
	mu        sync.Mutex
	callCount int
	summary   string
}

func (s *scriptedErrorThenFinishLLM) CreateChatCompletion(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	s.mu.Lock()
	s.callCount++
	count := s.callCount
	s.mu.Unlock()

	if count == 1 {
		args, _ := json.Marshal(map[string]string{"command": "failing-cmd"})
		return &llm.ChatCompletionResponse{
			ID: "err-llm-1",
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-err-1",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
					FinishReason: "tool_calls",
				},
			},
		}, nil
	}

	// Verify we received the tool error in message history, then call finish
	var sawToolError bool
	for _, m := range req.Messages {
		if m.Role == "tool" && m.Content != nil && strings.Contains(*m.Content, "failed") {
			sawToolError = true
			break
		}
	}
	if !sawToolError {
		return nil, fmt.Errorf("expected tool error in messages on second turn, got: %+v", req.Messages)
	}

	args, _ := json.Marshal(map[string]string{"summary": s.summary})
	return &llm.ChatCompletionResponse{
		ID: "err-llm-2",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   "call-finish-2",
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "finish",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_OrdinaryToolError_ContinuesSession(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-errfinishrun"
	sandboxName := "run-20261030t120000z-errfinishrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "errfinish-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-errfinish",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "errfinish-goal",
			Body: "recover from error and finish",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	finishSummary := "Finished after recovering from command error."
	errLLM := &scriptedErrorThenFinishLLM{summary: finishSummary}
	procSrv := &mockProcessServer{err: errors.New("command execution failed")}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, errLLM)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify session completed and run has the finish summary
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.Summary != finishSummary {
		t.Errorf("expected run.Summary %q, got %q", finishSummary, gotRun.Summary)
	}

	// 2. Verify events recorded "finished" event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFinishedEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Kind == "finished" && e.Message == finishSummary {
			foundFinishedEvent = true
			break
		}
	}
	if !foundFinishedEvent {
		t.Errorf("expected finished event with message %q, got events: %+v", finishSummary, events)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 4. Verify session transcript in sessionStore contains 6 messages:
	// turn 1: system, user, call-err-1, tool error result
	// turn 2: call-finish-2, tool success result
	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) != 6 {
		t.Fatalf("expected 6 messages in 2 turns, got %d: %+v", len(msgs), msgs)
	}
	if len(msgs[2].ToolCalls) == 0 || msgs[2].ToolCalls[0].Function.Name != "run_command" {
		t.Errorf("first tool call = %+v, want run_command", msgs[2].ToolCalls)
	}
	if msgs[3].Role != "tool" || msgs[3].Content == nil || !strings.Contains(*msgs[3].Content, "command execution failed") {
		t.Errorf("first tool result = %+v, want command execution failed", msgs[3])
	}
	if len(msgs[4].ToolCalls) == 0 || msgs[4].ToolCalls[0].Function.Name != "finish" {
		t.Errorf("second tool call = %+v, want finish", msgs[4].ToolCalls)
	}
	if msgs[5].Role != "tool" || msgs[5].Content == nil || *msgs[5].Content != finishSummary {
		t.Errorf("second tool result = %+v, want finishSummary", msgs[5])
	}
}

func TestOrchestrator_ReconciledAfterNotify_WithoutTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	storeDir := t.TempDir()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	store := state.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		BaseCommit: "commit-base123",
		Goal: &goals.Goal{
			Name: "optimize-latency",
			Body: "reduce latency by 10%",
		},
		Hash: "hash-v1",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	scheduler, err := NewScheduler(SchedulerOptions{
		Store:        store,
		Orchestrator: orch,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			runs, err := store.ListRuns(ctx, repo.Key())
			if err == nil && len(runs) > 0 && runs[0].SandboxName != "" {
				sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, runs[0].SandboxName, metav1.GetOptions{})
				if err == nil && len(sb.Status.Conditions) == 0 {
					sb.Status.Conditions = []metav1.Condition{
						{
							Type:   string(sandboxv1beta1.SandboxConditionReady),
							Status: metav1.ConditionTrue,
						},
					}
					_, _ = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{})
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// The scheduler creates the pending run and calls Orchestrator.Notify.
	// There is no orchestrator timer.
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("scheduler.Tick: %v", err)
	}

	// Verify the run is reconciled to Working without any timer
	var reachedWorking bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := store.ListRuns(ctx, repo.Key())
		if err == nil && len(runs) > 0 {
			if runs[0].Phase == state.PhaseWorking {
				reachedWorking = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !reachedWorking {
		t.Fatal("expected run created by scheduler to reach Working after Notify without timer")
	}
}

type blockingConcurrentProcessServer struct {
	processv1.UnimplementedProcessServiceServer
	onCommand func([]string)
}

func (b *blockingConcurrentProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	if req.Config != nil && b.onCommand != nil {
		b.onCommand(req.Config.Command)
	}
	return &processv1.ExecuteResponse{
		ExitCode: 0,
		Stdout:   []byte("ok"),
	}, nil
}

func TestOrchestrator_ConcurrentRuns_DifferentRepos(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	storeDir := t.TempDir()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	store := state.NewFileStore(storeDir)

	repo1 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo1",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo1",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo1); err != nil {
		t.Fatalf("PutRepo 1: %v", err)
	}

	repo2 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo2",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo2",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo2); err != nil {
		t.Fatalf("PutRepo 2: %v", err)
	}

	run1 := &state.Run{
		ID:         "20261030T120000Z-run1",
		Repo:       repo1.Key(),
		GoalName:   "goal1",
		Phase:      state.PhasePending,
		BaseCommit: "commit1",
	}
	if err := store.PutRun(ctx, run1); err != nil {
		t.Fatalf("PutRun 1: %v", err)
	}

	run2 := &state.Run{
		ID:         "20261030T120000Z-run2",
		Repo:       repo2.Key(),
		GoalName:   "goal2",
		Phase:      state.PhasePending,
		BaseCommit: "commit2",
	}
	if err := store.PutRun(ctx, run2); err != nil {
		t.Fatalf("PutRun 2: %v", err)
	}

	// Use channels to verify both runs execute their clone command concurrently.
	run1Started := make(chan struct{})
	run2Started := make(chan struct{})
	var closeOnce1, closeOnce2 sync.Once

	procSrv := &blockingConcurrentProcessServer{
		onCommand: func(cmd []string) {
			cmdStr := strings.Join(cmd, " ")
			if strings.Contains(cmdStr, "repo1") {
				closeOnce1.Do(func() { close(run1Started) })
				select {
				case <-run2Started:
				case <-time.After(5 * time.Second):
				}
			} else if strings.Contains(cmdStr, "repo2") {
				closeOnce2.Do(func() { close(run2Started) })
				select {
				case <-run1Started:
				case <-time.After(5 * time.Second):
				}
			}
		},
	}

	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			for _, sbName := range []string{"run-20261030t120000z-run1", "run-20261030t120000z-run2"} {
				sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{})
				if err == nil && len(sb.Status.Conditions) == 0 {
					sb.Status.Conditions = []metav1.Condition{
						{
							Type:   string(sandboxv1beta1.SandboxConditionReady),
							Status: metav1.ConditionTrue,
						},
					}
					_, _ = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{})
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:             store,
		Harness:           harness,
		MaxConcurrentRuns: 4,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()

	orch.Notify(repo1.Key(), run1.ID)
	orch.Notify(repo2.Key(), run2.ID)

	// Wait for both to reach Working
	deadline := time.Now().Add(5 * time.Second)
	var run1OK, run2OK bool
	for time.Now().Before(deadline) {
		r1, err1 := store.GetRun(ctx, repo1.Key(), run1.ID)
		r2, err2 := store.GetRun(ctx, repo2.Key(), run2.ID)
		if err1 == nil && r1.Phase == state.PhaseWorking {
			run1OK = true
		}
		if err2 == nil && r2.Phase == state.PhaseWorking {
			run2OK = true
		}
		if run1OK && run2OK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !run1OK || !run2OK {
		t.Fatalf("expected both runs to reach Working concurrently: run1OK=%v, run2OK=%v", run1OK, run2OK)
	}
}

type cancellingChatClient struct {
	cancelFn context.CancelFunc
}

func (c *cancellingChatClient) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	c.cancelFn()
	return nil, context.Canceled
}

func TestOrchestrator_ContextCancelled_LeavesWorking(t *testing.T) {
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(t.Context(), repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-cancel"
	sandboxName := "run-20261030t120000z-cancel"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "optimize",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base123",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(t.Context(), run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	testCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cancellingLLM := &cancellingChatClient{
		cancelFn: cancel,
	}

	harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, cancellingLLM)

	// Create sandbox in fake k8s
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(t.Context(), &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: metav1.ConditionTrue,
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	go func() {
		_ = orch.Start(testCtx)
	}()

	// Reconcile with the cancellable context. The LLM will cancel the context during the session.
	_, _ = orch.Reconcile(testCtx, Request{Repo: repo.Key(), RunID: runID})

	deadline := time.Now().Add(5 * time.Second)
	for orch.isRunActive(runID) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to stop after context cancellation")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Check with test context: run MUST stay in Working phase
	gotRun, err := store.GetRun(t.Context(), repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s after context cancellation, got %s", state.PhaseWorking, gotRun.Phase)
	}
	if gotRun.Error != "" {
		t.Errorf("expected empty error on context cancellation, got %q", gotRun.Error)
	}

	// Verify no failure event was written
	events, err := store.ReadEvents(t.Context(), repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for _, e := range events {
		if e.Phase == state.PhaseFailed {
			t.Errorf("unexpected failed event recorded on context cancellation: %+v", e)
		}
	}
}

func TestOrchestrator_WorkingRunEnqueuedOnStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	storeDir := t.TempDir()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	workingRunID := "20261030T120000Z-working"
	workingRun := &state.Run{
		ID:          workingRunID,
		Repo:        repo.Key(),
		GoalName:    "task",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base",
		SandboxName: "run-working",
	}
	if err := store.PutRun(ctx, workingRun); err != nil {
		t.Fatalf("PutRun working: %v", err)
	}

	prepRunID := "20261030T120000Z-prep"
	prepRun := &state.Run{
		ID:          prepRunID,
		Repo:        repo.Key(),
		GoalName:    "task",
		Phase:       state.PhasePreparing,
		BaseCommit:  "commit-base",
		SandboxName: "run-prep",
	}
	if err := store.PutRun(ctx, prepRun); err != nil {
		t.Fatalf("PutRun prep: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "ok",
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, &llm.ElizaClient{})

	// Create sandboxes in fake k8s
	for _, sbName := range []string{"run-working", "run-prep"} {
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbName,
				Namespace: "ao-sandboxes",
			},
			Status: sandboxv1beta1.SandboxStatus{
				Conditions: []metav1.Condition{
					{
						Type:   string(sandboxv1beta1.SandboxConditionReady),
						Status: metav1.ConditionTrue,
					},
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("creating sandbox %s: %v", sbName, err)
		}
	}

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()
	deadline := time.Now().Add(5 * time.Second)
	var workingReconciled bool
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), workingRunID)
		if err == nil && r.Phase == state.PhaseNoImprovement {
			workingReconciled = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !workingReconciled {
		t.Fatal("expected working run to be enqueued on start and reach NoImprovement")
	}

	// Verify Preparing run was enqueued on start and reached Working or NoImprovement
	var prepReconciled bool
	for time.Now().Before(deadline) {
		rPrep, err := store.GetRun(ctx, repo.Key(), prepRunID)
		if err == nil && (rPrep.Phase == state.PhaseWorking || rPrep.Phase == state.PhaseNoImprovement) {
			prepReconciled = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !prepReconciled {
		t.Fatal("expected preparing run to be enqueued on start and reach Working or NoImprovement")
	}
}

func TestBuildSystemPrompt(t *testing.T) {
	repo := &state.Repo{
		URL:           "https://github.com/example/repo",
		DefaultBranch: "main",
	}
	run := &state.Run{
		ID:         "20261104T120000Z-0001",
		GoalName:   "throughput",
		BaseCommit: "abcdef123456",
	}
	prompt := buildSystemPrompt("Base prompt", repo, run, "Improve throughput by 10%")

	expectedSubstrings := []string{
		"Base prompt",
		"# Goal",
		"Improve throughput by 10%",
		"# Repository Facts",
		"- URL: https://github.com/example/repo",
		"- Default Branch: main",
		"- Branch: ao/throughput/20261104T120000Z-0001",
		"- Base Commit: abcdef123456",
		"- Working Directory: /workspace/repo",
		"Commit your changes with git before calling open_pull_request; only commits on the branch are pushed, nothing uncommitted is included.",
		"# Rules",
	}
	for _, expected := range expectedSubstrings {
		if !strings.Contains(prompt, expected) {
			t.Errorf("buildSystemPrompt missing %q, got:\n%s", expected, prompt)
		}
	}
}

func TestOrchestrator_GitAuthor(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	const author = "auto-optimizer <auto-optimizer@users.noreply.github.com>"
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		GitAuthor: author,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if orch.gitAuthor != author {
		t.Errorf("orch.gitAuthor = %q, want %q", orch.gitAuthor, author)
	}
}

func TestOrchestrator_PublicURL(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		PublicURL: "https://ao.example.com/",
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if want := "https://ao.example.com"; orch.publicURL != want {
		t.Errorf("orch.publicURL = %q, want %q", orch.publicURL, want)
	}
}

func TestOrchestrator_RunPageURL(t *testing.T) {
	repo := &state.Repo{Host: "github.com", Owner: "testowner", Name: "testrepo"}
	run := &state.Run{ID: "20261105T120000Z-abcd", Repo: repo.Key()}

	tests := []struct {
		name      string
		publicURL string
		orchNil   bool
		run       *state.Run
		want      string
	}{
		{
			name:      "configured",
			publicURL: "https://ao.example.com",
			run:       run,
			want:      "https://ao.example.com/runs/20261105T120000Z-abcd",
		},
		{
			name:      "trailing slash stripped",
			publicURL: "https://ao.example.com/",
			run:       run,
			want:      "https://ao.example.com/runs/20261105T120000Z-abcd",
		},
		{
			name: "unset",
			run:  run,
			want: "",
		},
		{
			name:    "nil orchestrator",
			orchNil: true,
			run:     run,
			want:    "",
		},
		{
			name:      "nil run",
			publicURL: "https://ao.example.com",
			run:       nil,
			want:      "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var orch *Orchestrator
			if !tc.orchNil {
				orch = &Orchestrator{publicURL: strings.TrimRight(tc.publicURL, "/")}
			}
			if got := orch.RunPageURL(repo, tc.run); got != tc.want {
				t.Errorf("RunPageURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOrchestrator_Preparing_WatchStatusReadyEnqueuesRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	storeDir := t.TempDir()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	store := state.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-watchready"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "goal-watch",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()
	orch.Notify(repo.Key(), runID)

	// 1. Wait until the run enters Preparing and has a Sandbox created
	deadline := time.Now().Add(5 * time.Second)
	var sb *sandboxv1beta1.Sandbox
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if s, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sb = s
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sb == nil {
		t.Fatal("timed out waiting for run to enter Preparing with Sandbox created")
	}

	// 2. Verify worker is free (no clone executed yet)
	procSrv.mu.Lock()
	cmdsCount := len(procSrv.commands)
	procSrv.mu.Unlock()
	if cmdsCount != 0 {
		t.Fatalf("expected 0 commands executed while unready, got %d", cmdsCount)
	}

	// 3. Update the fake Sandbox's status to Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating fake sandbox to Ready: %v", err)
	}

	// 4. Verify watch detects status change, enqueues run, clones, and moves to Working
	var reachedWorking bool
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhaseWorking {
			reachedWorking = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reachedWorking {
		t.Fatal("expected run to reach Working after Sandbox status changed to Ready via watch")
	}

	procSrv.mu.Lock()
	finalCmds := len(procSrv.commands)
	procSrv.mu.Unlock()
	if finalCmds != 2 {
		t.Fatalf("expected 2 commands executed after Ready, got %d", finalCmds)
	}
}

func TestOrchestrator_Preparing_WatchDeleteEnqueuesAndFailsRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	storeDir := t.TempDir()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	store := state.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-watchdel"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "goal-del",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()

	orch.Notify(repo.Key(), runID)

	// Wait until run enters Preparing and sandbox exists in Kubernetes
	deadline := time.Now().Add(5 * time.Second)
	var sbName string
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if _, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sbName = r.SandboxName
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sbName == "" {
		t.Fatal("timed out waiting for run to enter Preparing with SandboxName")
	}

	// Delete fake sandbox before readiness
	if err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, sbName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting fake sandbox: %v", err)
	}

	// Verify watch detects deletion, enqueues run, and marks it Failed
	var reachedFailed bool
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhaseFailed {
			reachedFailed = true
			if !strings.Contains(r.Error, "deleted before becoming ready") {
				t.Errorf("expected error to mention deleted before becoming ready, got: %q", r.Error)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reachedFailed {
		t.Fatal("expected run to reach Failed after Sandbox was deleted via watch")
	}
}

func TestOrchestrator_Preparing_WatchFailureConditionFailsRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	storeDir := t.TempDir()
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-done
	})
	store := state.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-watchfailcond"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "goal-failcond",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()

	orch.Notify(repo.Key(), runID)

	// Wait until run enters Preparing and sandbox exists in Kubernetes
	deadline := time.Now().Add(5 * time.Second)
	var sb *sandboxv1beta1.Sandbox
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if s, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sb = s
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sb == nil {
		t.Fatal("timed out waiting for run to enter Preparing with Sandbox created")
	}

	// Update fake sandbox with failure condition (InvalidConfiguration)
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:    string(sandboxv1beta1.SandboxConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  sandboxv1beta1.SandboxReasonInvalidConfiguration,
			Message: "Sandbox name exceeds maximum length",
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating fake sandbox with failure condition: %v", err)
	}

	// Verify watch detects status change, enqueues run, and marks it Failed
	var reachedFailed bool
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhaseFailed {
			reachedFailed = true
			if !strings.Contains(r.Error, "Sandbox name exceeds maximum length") {
				t.Errorf("expected error to mention failure message, got: %q", r.Error)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reachedFailed {
		t.Fatal("expected run to reach Failed after Sandbox reported failure condition")
	}
}

func TestOrchestrator_RequestsForSandbox(t *testing.T) {
	ctx := t.Context()

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	const ns = "ao-sandboxes"
	isController := true

	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "herorun-1",
			Namespace: ns,
			Labels: map[string]string{
				LabelRun: "run-hero",
			},
			Annotations: map[string]string{
				AnnotationRepo: "github.com/gke-labs/auto-optimizer",
			},
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: "https://github.com/gke-labs/auto-optimizer",
			Goal:       "optimize",
			RunID:      "run-hero",
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(hr).Build()
	orch := &Orchestrator{kubeClient: fakeClient}

	t.Run("owned sandbox maps through HeroRun", func(t *testing.T) {
		sb := &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sb-owned",
				Namespace: ns,
				Labels: map[string]string{
					LabelRun: "sb-run-id",
				},
				Annotations: map[string]string{
					AnnotationRepo: "local/_/sb-repo",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: herorunv1alpha1.GroupVersion.String(),
						Kind:       "HeroRun",
						Name:       "herorun-1",
						UID:        "uid-1",
						Controller: &isController,
					},
				},
			},
		}

		got := orch.requestsForSandbox(ctx, sb)
		if len(got) != 1 {
			t.Fatalf("expected 1 request, got %d", len(got))
		}
		wantRepo := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
		if got[0].RunID != "run-hero" {
			t.Errorf("got RunID = %q, want %q", got[0].RunID, "run-hero")
		}
		if got[0].Repo != wantRepo {
			t.Errorf("got Repo = %+v, want %+v", got[0].Repo, wantRepo)
		}
	})

	t.Run("sandbox without owner maps from own label and annotation", func(t *testing.T) {
		tests := []struct {
			name     string
			repoStr  string
			runID    string
			wantRepo state.RepoKey
			wantNil  bool
		}{
			{name: "remote repo key", repoStr: "github.com/gke-labs/auto-optimizer", runID: "run-1", wantRepo: state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}},
			{name: "local repo key with underscore owner", repoStr: "local/_/hello-repo", runID: "run-2", wantRepo: state.RepoKey{Host: "local", Owner: "_", Name: "hello-repo"}},
			{name: "invalid repo key format", repoStr: "invalid-format", runID: "run-3", wantNil: true},
			{name: "missing annotation", repoStr: "", runID: "run-4", wantNil: true},
			{name: "missing run label", repoStr: "github.com/gke-labs/auto-optimizer", runID: "", wantNil: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				sb := &sandboxv1beta1.Sandbox{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sb-unowned",
						Namespace: ns,
					},
				}
				if tt.runID != "" {
					sb.Labels = map[string]string{LabelRun: tt.runID}
				}
				if tt.repoStr != "" {
					sb.Annotations = map[string]string{AnnotationRepo: tt.repoStr}
				}
				got := orch.requestsForSandbox(ctx, sb)
				if tt.wantNil {
					if len(got) != 0 {
						t.Fatalf("expected empty requests, got %+v", got)
					}
					return
				}
				if len(got) != 1 {
					t.Fatalf("expected 1 request, got %d", len(got))
				}
				if got[0].RunID != tt.runID {
					t.Errorf("got RunID = %q, want %q", got[0].RunID, tt.runID)
				}
				if got[0].Repo != tt.wantRepo {
					t.Errorf("got Repo = %+v, want %+v", got[0].Repo, tt.wantRepo)
				}
			})
		}
	})

	t.Run("owner present but HeroRun missing maps to nothing", func(t *testing.T) {
		sb := &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sb-missing-owner",
				Namespace: ns,
				Labels: map[string]string{
					LabelRun: "run-sb",
				},
				Annotations: map[string]string{
					AnnotationRepo: "github.com/gke-labs/auto-optimizer",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: herorunv1alpha1.GroupVersion.String(),
						Kind:       "HeroRun",
						Name:       "herorun-nonexistent",
						UID:        "uid-missing",
						Controller: &isController,
					},
				},
			},
		}

		got := orch.requestsForSandbox(ctx, sb)
		if len(got) != 0 {
			t.Fatalf("expected 0 requests for missing HeroRun owner, got %+v", got)
		}
	})

	t.Run("nil client falls back", func(t *testing.T) {
		nilClientOrch := &Orchestrator{kubeClient: nil}
		sb := &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sb-nil-client",
				Namespace: ns,
				Labels: map[string]string{
					LabelRun: "run-nil-client",
				},
				Annotations: map[string]string{
					AnnotationRepo: "github.com/gke-labs/auto-optimizer",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: herorunv1alpha1.GroupVersion.String(),
						Kind:       "HeroRun",
						Name:       "herorun-1",
						UID:        "uid-1",
						Controller: &isController,
					},
				},
			},
		}

		got := nilClientOrch.requestsForSandbox(ctx, sb)
		if len(got) != 1 {
			t.Fatalf("expected 1 request with nil client fallback, got %d", len(got))
		}
		wantRepo := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
		if got[0].RunID != "run-nil-client" {
			t.Errorf("got RunID = %q, want %q", got[0].RunID, "run-nil-client")
		}
		if got[0].Repo != wantRepo {
			t.Errorf("got Repo = %+v, want %+v", got[0].Repo, wantRepo)
		}
	})
}

func TestOrchestrator_PublishHeroRun_LeavesPending(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-abcd"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		Clock:      func() time.Time { return fixedTime },
		KubeClient: fakeClient,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Reconcile Pending run -> leaves Pending to Preparing
	res, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// Verify run left Pending
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected phase Preparing, got %s", gotRun.Phase)
	}

	// Verify one HeroRun was created with expected spec, labels, and phase
	var hrList herorunv1alpha1.HeroRunList
	if err := fakeClient.List(ctx, &hrList, client.InNamespace("ao-sandboxes")); err != nil {
		t.Fatalf("fakeClient.List: %v", err)
	}
	if len(hrList.Items) != 1 {
		t.Fatalf("expected 1 HeroRun, got %d", len(hrList.Items))
	}

	hr := hrList.Items[0]
	expectedName := "run-20261030t120000z-abcd"
	if hr.Name != expectedName {
		t.Errorf("expected HeroRun name %q, got %q", expectedName, hr.Name)
	}
	if hr.Namespace != "ao-sandboxes" {
		t.Errorf("expected HeroRun namespace 'ao-sandboxes', got %q", hr.Namespace)
	}
	if hr.Spec.Repository != repo.URL {
		t.Errorf("expected spec.repository %q, got %q", repo.URL, hr.Spec.Repository)
	}
	if hr.Spec.Goal != "optimize-search" {
		t.Errorf("expected spec.goal %q, got %q", "optimize-search", hr.Spec.Goal)
	}
	if hr.Spec.RunID != runID {
		t.Errorf("expected spec.runID %q, got %q", runID, hr.Spec.RunID)
	}
	if hr.Spec.BaseCommit != "commit-base123" {
		t.Errorf("expected spec.baseCommit %q, got %q", "commit-base123", hr.Spec.BaseCommit)
	}
	if hr.Labels[LabelRun] != runID {
		t.Errorf("expected label %s=%q, got %q", LabelRun, runID, hr.Labels[LabelRun])
	}
	if hr.Annotations[AnnotationRepo] != repo.Key().String() {
		t.Errorf("expected annotation %s=%q, got %q", AnnotationRepo, repo.Key().String(), hr.Annotations[AnnotationRepo])
	}

	// Verify the Sandbox was created with exactly one owner reference to the HeroRun
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, expectedName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agentsCS.Sandboxes.Get: %v", err)
	}
	if len(sb.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference on sandbox, got %d", len(sb.OwnerReferences))
	}
	ref := sb.OwnerReferences[0]
	if ref.APIVersion != herorunv1alpha1.GroupVersion.String() {
		t.Errorf("expected APIVersion %q, got %q", herorunv1alpha1.GroupVersion.String(), ref.APIVersion)
	}
	if ref.Kind != "HeroRun" {
		t.Errorf("expected Kind 'HeroRun', got %q", ref.Kind)
	}
	if ref.Name != hr.Name {
		t.Errorf("expected owner ref name %q, got %q", hr.Name, ref.Name)
	}
	if ref.UID != hr.UID {
		t.Errorf("expected owner ref UID %q, got %q", hr.UID, ref.UID)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("expected owner ref controller to be true")
	}
}

func TestOrchestrator_PublishHeroRun_Resume(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	// Run 0: non-terminal (Pending), no HeroRun exists -> Resume must NOT publish Pending runs
	runPending := &state.Run{
		ID:         "20261030T120000Z-runpending",
		Repo:       repo.Key(),
		GoalName:   "goal-pending",
		Phase:      state.PhasePending,
		BaseCommit: "",
	}
	if err := store.PutRun(ctx, runPending); err != nil {
		t.Fatalf("PutRun runPending: %v", err)
	}

	// Run 1: non-terminal (Working), no HeroRun exists
	run1 := &state.Run{
		ID:          "20261030T120000Z-run1",
		Repo:        repo.Key(),
		GoalName:    "goal1",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit1",
		SandboxName: "run-20261030t120000z-run1",
	}
	if err := store.PutRun(ctx, run1); err != nil {
		t.Fatalf("PutRun run1: %v", err)
	}

	// Run 2: non-terminal (Preparing), already has existing HeroRun
	run2 := &state.Run{
		ID:          "20261030T120000Z-run2",
		Repo:        repo.Key(),
		GoalName:    "goal2",
		Phase:       state.PhasePreparing,
		BaseCommit:  "commit2",
		SandboxName: "run-20261030t120000z-run2",
	}
	if err := store.PutRun(ctx, run2); err != nil {
		t.Fatalf("PutRun run2: %v", err)
	}

	// Run 3: terminal (NoImprovement), no HeroRun exists
	run3 := &state.Run{
		ID:          "20261030T120000Z-run3",
		Repo:        repo.Key(),
		GoalName:    "goal3",
		Phase:       state.PhaseNoImprovement,
		BaseCommit:  "commit3",
		SandboxName: "run-20261030t120000z-run3",
	}
	if err := store.PutRun(ctx, run3); err != nil {
		t.Fatalf("PutRun run3: %v", err)
	}

	harness, _, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Pre-create HeroRun for run2 with existing marker
	existingHR := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-20261030t120000z-run2",
			Namespace: "ao-sandboxes",
			Annotations: map[string]string{
				"custom-annotation": "untouched",
			},
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: "https://github.com/gke-labs/original",
			Goal:       "original-goal",
			RunID:      run2.ID,
			BaseCommit: "original-commit",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: "ExistingPhase",
		},
	}
	if err := fakeClient.Create(ctx, existingHR); err != nil {
		t.Fatalf("fakeClient.Create existingHR: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("orch.Resume: %v", err)
	}

	// Check runPending: Pending run has no HeroRun created during Resume
	var hrPending herorunv1alpha1.HeroRun
	err = fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: "run-20261030t120000z-runpending"}, &hrPending)
	if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound for Pending run HeroRun in Resume, got: %v", err)
	}

	// Check run1: HeroRun created
	var hr1 herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: "run-20261030t120000z-run1"}, &hr1); err != nil {
		t.Fatalf("getting hr1: %v", err)
	}
	if hr1.Spec.Repository != repo.URL || hr1.Spec.Goal != "goal1" || hr1.Spec.RunID != run1.ID || hr1.Spec.BaseCommit != "commit1" {
		t.Errorf("hr1 spec mismatch: %+v", hr1.Spec)
	}
	if hr1.Labels[LabelRun] != run1.ID {
		t.Errorf("hr1 label %s = %q, want %q", LabelRun, hr1.Labels[LabelRun], run1.ID)
	}

	// Check run2: Existing HeroRun was untouched
	var hr2 herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: "run-20261030t120000z-run2"}, &hr2); err != nil {
		t.Fatalf("getting hr2: %v", err)
	}
	if hr2.Annotations["custom-annotation"] != "untouched" {
		t.Errorf("expected custom-annotation 'untouched', got %q", hr2.Annotations["custom-annotation"])
	}
	if hr2.Spec.Goal != "original-goal" {
		t.Errorf("expected hr2 original goal, got %q", hr2.Spec.Goal)
	}
	if hr2.Status.Phase != "ExistingPhase" {
		t.Errorf("expected hr2 original phase 'ExistingPhase', got %q", hr2.Status.Phase)
	}

	// Check run3: Terminal run has no HeroRun created
	var hr3 herorunv1alpha1.HeroRun
	err = fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: "run-20261030t120000z-run3"}, &hr3)
	if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound for terminal run HeroRun, got: %v", err)
	}
}

func TestOrchestrator_PublishHeroRun_NilKubeClient(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-abcd"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: nil,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Run leaves Pending in prepareRun with nil KubeClient
	res, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// Verify run proceeded to Preparing
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Verify Sandbox was created without owner references
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agentsCS.Sandboxes.Get: %v", err)
	}
	if len(sb.OwnerReferences) != 0 {
		t.Errorf("expected 0 owner references on sandbox with nil KubeClient, got %d", len(sb.OwnerReferences))
	}
}

func TestOrchestrator_PublishHeroRun_FailureDoesNotFailRun(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-abcd"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			return errors.New("simulated publish failure")
		},
	}).Build()

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	res, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile failed unexpectedly: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// Verify run proceeded to Preparing despite publish failure
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Verify Sandbox was created without owner references when publish failed
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agentsCS.Sandboxes.Get: %v", err)
	}
	if len(sb.OwnerReferences) != 0 {
		t.Errorf("expected 0 owner references on sandbox on publish failure, got %d", len(sb.OwnerReferences))
	}
}

func TestOrchestrator_SandboxOwnerReference_NilKubeClientReachesWorking(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-nowork"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: nil,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile Pending -> Preparing
	res, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected phase Preparing, got %s", gotRun.Phase)
	}

	// Sandbox has no owner references
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agentsCS.Sandboxes.Get: %v", err)
	}
	if len(sb.OwnerReferences) != 0 {
		t.Errorf("expected 0 owner references on sandbox, got %d", len(sb.OwnerReferences))
	}

	// Step 2: Mark sandbox Ready and reconcile Preparing -> Working
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating fake sandbox to Ready: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}
}

func TestOrchestrator_SandboxOwnerReference_PreExistingHeroRunHasUID(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-uidtest"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	expectedName := "run-20261030t120000z-uidtest"
	const expectedUID = types.UID("custom-herorun-uid-456")
	preExistingHR := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      expectedName,
			Namespace: "ao-sandboxes",
			UID:       expectedUID,
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: repo.URL,
			Goal:       "optimize-search",
			RunID:      runID,
			BaseCommit: "commit-base123",
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(preExistingHR).Build()

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, expectedName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agentsCS.Sandboxes.Get: %v", err)
	}
	if len(sb.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(sb.OwnerReferences))
	}
	ref := sb.OwnerReferences[0]
	if ref.APIVersion != herorunv1alpha1.GroupVersion.String() {
		t.Errorf("expected APIVersion %q, got %q", herorunv1alpha1.GroupVersion.String(), ref.APIVersion)
	}
	if ref.Kind != "HeroRun" {
		t.Errorf("expected Kind 'HeroRun', got %q", ref.Kind)
	}
	if ref.Name != expectedName {
		t.Errorf("expected owner ref name %q, got %q", expectedName, ref.Name)
	}
	if ref.UID != expectedUID {
		t.Errorf("expected owner ref UID %q, got %q", expectedUID, ref.UID)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("expected controller to be true")
	}
}

type scriptedOpenPullRequestLLM struct {
	repo, branch, title, body string
}

func (s *scriptedOpenPullRequestLLM) CreateChatCompletion(_ context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	args, _ := json.Marshal(map[string]string{
		"repository": s.repo, "branch": s.branch, "title": s.title, "body": s.body,
	})
	return &llm.ChatCompletionResponse{
		ID: "pr-llm",
		Choices: []llm.Choice{{
			Message: llm.Message{
				Role: "assistant",
				ToolCalls: []llm.ToolCall{{
					ID:   "call-open-pr-1",
					Type: "function",
					Function: llm.FunctionCall{
						Name:      "open_pull_request",
						Arguments: string(args),
					},
				}},
			},
			FinishReason: "tool_calls",
		}},
	}, nil
}

type scriptedExportServer struct {
	processv1.UnimplementedProcessServiceServer
	mu            sync.Mutex
	commands      map[string]string
	pack          []byte
	startHandlers map[string]func(stream processv1.ProcessService_StartServer) error
	startCalls    []string
}

func (s *scriptedExportServer) Execute(_ context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	cmd := strings.Join(req.GetConfig().GetCommand(), " ")
	s.mu.Lock()
	out, ok := s.commands[cmd]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unhandled command: %s", cmd)
	}
	return &processv1.ExecuteResponse{Stdout: []byte(out)}, nil
}

func (s *scriptedExportServer) Start(req *processv1.StartRequest, stream processv1.ProcessService_StartServer) error {
	cmd := strings.Join(req.GetConfig().GetCommand(), " ")
	s.mu.Lock()
	s.startCalls = append(s.startCalls, cmd)
	var handler func(stream processv1.ProcessService_StartServer) error
	if s.startHandlers != nil {
		handler = s.startHandlers[cmd]
	}
	pack := s.pack
	s.mu.Unlock()

	if handler != nil {
		return handler(stream)
	}

	if err := stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 1}}}); err != nil {
		return err
	}
	if len(pack) > 0 {
		_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stdout{Stdout: pack}})
	}
	return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{}}})
}

func (s *scriptedExportServer) WriteStdin(context.Context, *processv1.WriteStdinRequest) (*processv1.WriteStdinResponse, error) {
	return &processv1.WriteStdinResponse{}, nil
}

func createTestRemoteAndPackfile(t *testing.T) (string, *git.Repository, *git.Repository, string, string, []byte) {
	t.Helper()
	bareDir := filepath.Join(t.TempDir(), "remote.git")
	bare, err := git.PlainInit(bareDir, true)
	if err != nil {
		t.Fatalf("PlainInit bare: %v", err)
	}
	remoteURL := "file://" + bareDir

	work, err := git.PlainInit(filepath.Join(t.TempDir(), "work"), false)
	if err != nil {
		t.Fatalf("PlainInit work: %v", err)
	}
	wt, err := work.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	_ = os.WriteFile(filepath.Join(wt.Filesystem.Root(), "base.txt"), []byte("base"), 0644)
	_, _ = wt.Add("base.txt")
	baseCommit, err := wt.Commit("base", &git.CommitOptions{Author: &object.Signature{Name: "A", Email: "a@example.com", When: time.Now()}})
	if err != nil {
		t.Fatalf("Commit base: %v", err)
	}

	_, _ = work.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{remoteURL}})
	if err := work.Push(&git.PushOptions{RemoteName: "origin"}); err != nil {
		t.Fatalf("Push base: %v", err)
	}

	_ = os.WriteFile(filepath.Join(wt.Filesystem.Root(), "feat.txt"), []byte("feat"), 0644)
	_, _ = wt.Add("feat.txt")
	tipCommit, err := wt.Commit("feat", &git.CommitOptions{Author: &object.Signature{Name: "A", Email: "a@example.com", When: time.Now()}})
	if err != nil {
		t.Fatalf("Commit tip: %v", err)
	}
	hashes, err := revlist.Objects(work.Storer, []plumbing.Hash{tipCommit}, []plumbing.Hash{baseCommit})
	if err != nil {
		t.Fatalf("revlist.Objects: %v", err)
	}
	buf := new(bytes.Buffer)
	if _, err := packfile.NewEncoder(buf, work.Storer, false).Encode(hashes, 10); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return remoteURL, bare, work, baseCommit.String(), tipCommit.String(), buf.Bytes()
}

func setupSelectingRun(t *testing.T, phase state.RunPhase, baseSHA string, procSrv processv1.ProcessServiceServer, llmClient llm.ChatClient) (*Orchestrator, state.Store, *fakeagents.Clientset, *state.Repo, *state.Run) {
	t.Helper()
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	repo := &state.Repo{URL: "https://github.com/gke-labs/test-repo", Host: "github.com", Owner: "gke-labs", Name: "test-repo", DefaultBranch: "main"}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}
	runID := "20261030T120000Z-test"
	sandboxName := "run-20261030t120000z-test"
	run := &state.Run{
		ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: phase, BaseCommit: baseSHA, SandboxName: sandboxName,
		Goal: &goals.Goal{Name: "goal", Body: "goal body"},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, llmClient)
	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}
	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store: store, SessionStore: session.NewFileStore(t.TempDir()), Harness: harness, Clock: func() time.Time { return fixedTime }, MaxTurns: 30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	return orch, store, agentsCS, repo, run
}

func TestOrchestrator_Working_OpenPullRequest_TransitionsToSelecting(t *testing.T) {
	ctx := t.Context()
	prLLM := &scriptedOpenPullRequestLLM{
		repo:   "https://github.com/gke-labs/test-repo",
		branch: "ao/goal/20261030T120000Z-test",
		title:  "Optimize search index queries",
		body:   "Adds composite index on (repo, goal, run_id).",
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseWorking, "base-sha", &mockProcessServer{}, prLLM)

	if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSelecting || gotRun.PRTitle != prLLM.title || gotRun.PRBody != prLLM.body {
		t.Errorf("got run %+v, want Selecting with PRTitle/Body", gotRun)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var found bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "pull_request_requested" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected pull_request_requested event, got: %+v", events)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); err != nil {
		t.Errorf("expected sandbox to still exist, got: %v", err)
	}
}

func TestOrchestrator_Selecting_PushesBranch_Succeeds(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, bare, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-pushrun"
	sandboxName := "run-20261030t120000z-pushrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
		PRTitle:     "Add feature",
		PRBody:      "Implements feature",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded with Branch set
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("expected phase %s, got %s", state.PhaseSucceeded, gotRun.Phase)
	}
	if gotRun.PRURL != "" {
		t.Errorf("expected empty PRURL, got %q", gotRun.PRURL)
	}
	wantBranch := "ao/export-goal/" + runID
	if gotRun.Branch != wantBranch {
		t.Errorf("expected branch %q, got %q", wantBranch, gotRun.Branch)
	}

	// 2. Verify bare remote has branch at tipSHA
	refName := plumbing.ReferenceName("refs/heads/" + wantBranch)
	ref, err := bare.Reference(refName, true)
	if err != nil {
		t.Fatalf("bare.Reference(%s): %v", refName, err)
	}
	if ref.Hash().String() != tipSHA {
		t.Errorf("expected remote ref %s to be %s, got %s", refName, tipSHA, ref.Hash().String())
	}

	// 3. Verify events: commits_exported, branch_pushed, pull_request_not_opened
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundExported, foundPushed, foundPRNotOpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "commits_exported" {
			if m, ok := e.Data.(map[string]any); ok && m["tip"] == tipSHA {
				foundExported = true
			}
		}
		if e.Phase == state.PhaseSelecting && e.Kind == "branch_pushed" {
			if m, ok := e.Data.(map[string]any); ok && m["tip"] == tipSHA && m["branch"] == wantBranch {
				foundPushed = true
			}
		}
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_not_opened" && e.Message == "forge cannot open pull requests" {
			foundPRNotOpened = true
		}
	}
	if !foundExported {
		t.Errorf("expected commits_exported event carrying tip %s, got: %+v", tipSHA, events)
	}
	if !foundPushed {
		t.Errorf("expected branch_pushed event carrying tip %s and branch %s, got: %+v", tipSHA, wantBranch, events)
	}
	if !foundPRNotOpened {
		t.Errorf("expected pull_request_not_opened event, got: %+v", events)
	}

	// 4. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_BranchMoved_Fails(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, bare, work, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-movedrun"
	sandboxName := "run-20261030t120000z-movedrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
		PRTitle:     "Add feature",
		PRBody:      "Implements feature",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	// Advance ao/<goal>/<run-id> in the bare repository to another commit before Selecting
	wantBranch := "ao/export-goal/" + runID
	wt, err := work.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	_ = os.WriteFile(filepath.Join(wt.Filesystem.Root(), "other.txt"), []byte("other"), 0644)
	_, _ = wt.Add("other.txt")
	otherCommit, err := wt.Commit("other", &git.CommitOptions{Author: &object.Signature{Name: "A", Email: "a@example.com", When: time.Now()}})
	if err != nil {
		t.Fatalf("Commit other: %v", err)
	}
	refName := plumbing.ReferenceName("refs/heads/" + wantBranch)
	if err := work.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec(otherCommit.String() + ":" + refName.String())},
	}); err != nil {
		t.Fatalf("Push other to bare: %v", err)
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Failed
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if gotRun.Error != "remote branch is not at base commit" {
		t.Errorf("expected error %q, got %q", "remote branch is not at base commit", gotRun.Error)
	}

	// 2. Verify events: branch_moved
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundBranchMoved bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "branch_moved" && e.Message == "remote branch is not at base commit" {
			foundBranchMoved = true
			break
		}
	}
	if !foundBranchMoved {
		t.Errorf("expected branch_moved event, got: %+v", events)
	}

	// 3. Verify remote branch was left unchanged
	if ref, err := bare.Reference(refName, true); err != nil || ref.Hash() != otherCommit {
		t.Errorf("expected bare branch to remain at %s, got: %v (err: %v)", otherCommit, ref, err)
	}

	// 4. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_PushFailed(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	_, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	// Point repository URL at a nonexistent path to trigger real push failure
	nonexistentURL := "file://" + filepath.Join(t.TempDir(), "nonexistent.git")

	repoKey, err := state.ParseRepoURL(nonexistentURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           nonexistentURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-pushfailrun"
	sandboxName := "run-20261030t120000z-pushfailrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Failed
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}

	// 2. Verify events: push_failed
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundPushFailed bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "push_failed" {
			foundPushFailed = true
			break
		}
	}
	if !foundPushFailed {
		t.Errorf("expected push_failed event, got events: %+v", events)
	}

	// 3. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_NilForge_Fails(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-nilforgerun"
	sandboxName := "run-20261030t120000z-nilforgerun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor:     nil, // explicitly nil
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Failed
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}

	// 2. Verify events: push_failed
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundPushFailed bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "push_failed" {
			foundPushFailed = true
			break
		}
	}
	if !foundPushFailed {
		t.Errorf("expected push_failed event, got events: %+v", events)
	}

	// 3. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_Refusals_NoImprovement(t *testing.T) {
	const baseSHA = "1234567890abcdef1234567890abcdef12345678"
	tests := []struct {
		name, diffOutput, wantKind, wantMessage string
	}{
		{name: "no changes against base", diffOutput: "", wantKind: "no_changes", wantMessage: "no changes against base commit"},
		{name: "changes under .goals/ refused", diffOutput: ".goals/goal.md\x00", wantKind: "refused", wantMessage: "changes under .goals/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			procSrv := &scriptedExportServer{
				commands: map[string]string{
					"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
					"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       tt.diffOutput,
				},
			}
			orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, baseSHA, procSrv, nil)

			if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
				t.Fatalf("orch.Reconcile: %v", err)
			}

			gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if gotRun.Phase != state.PhaseNoImprovement {
				t.Errorf("got phase %s, want NoImprovement", gotRun.Phase)
			}

			events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
			var found bool
			for _, e := range events {
				if e.Phase == state.PhaseNoImprovement && e.Kind == tt.wantKind && e.Message == tt.wantMessage {
					found = true
				}
			}
			if !found {
				t.Errorf("expected event kind=%q message=%q, got: %+v", tt.wantKind, tt.wantMessage, events)
			}

			if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
				t.Errorf("expected sandbox deleted, got err: %v", err)
			}
		})
	}
}

func TestOrchestrator_Selecting_Verify_PassingProceedsToExport(t *testing.T) {
	ctx := t.Context()
	_, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)
	const verifyCmd = "go test ./..."
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stdout{Stdout: []byte("PASS\nok\n")}})
				return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{ExitCode: 0}}})
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, baseSHA, procSrv, nil)
	run.Goal.Verify = verifyCmd
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed || gotRun.Error != "no forge configured for repository "+repo.URL {
		t.Errorf("got phase %s error %q, want Failed/no forge configured", gotRun.Phase, gotRun.Error)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundStarted, foundPassed, foundExported bool
	var order []string
	for _, e := range events {
		if e.Kind == "verify_started" {
			foundStarted = true
			order = append(order, e.Kind)
			if m, ok := e.Data.(map[string]any); !ok || m["command"] != verifyCmd {
				t.Errorf("verify_started data command = %v, want %q", e.Data, verifyCmd)
			}
		}
		if e.Kind == "verify_passed" {
			foundPassed = true
			order = append(order, e.Kind)
		}
		if e.Kind == "commits_exported" {
			foundExported = true
			order = append(order, e.Kind)
		}
	}
	if !foundStarted || !foundPassed || !foundExported {
		t.Errorf("expected verify_started, verify_passed, and commits_exported events, got: %+v", events)
	}
	wantOrder := []string{"verify_started", "verify_passed", "commits_exported"}
	if !slices.Equal(order, wantOrder) {
		t.Errorf("events order = %v, want %v", order, wantOrder)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_Verify_FailingEndsInNoImprovement(t *testing.T) {
	ctx := t.Context()
	const verifyCmd = "make test"
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	const tailOutput = "--- FAIL: TestA (0.00s)\nFAIL\nexit status 2\n"
	procSrv := &scriptedExportServer{
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stderr{Stderr: []byte(tailOutput)}})
				return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{ExitCode: 2}}})
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, "base-sha", procSrv, nil)
	run.Goal.Verify = verifyCmd
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("got phase %s, want NoImprovement", gotRun.Phase)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundStarted, foundFailed, foundExported bool
	for _, e := range events {
		if e.Kind == "verify_started" {
			foundStarted = true
		}
		if e.Phase == state.PhaseNoImprovement && e.Kind == "verify_failed" {
			foundFailed = true
			if !strings.Contains(e.Message, "2") {
				t.Errorf("expected exit code in message, got: %q", e.Message)
			}
			m, ok := e.Data.(map[string]any)
			if !ok {
				t.Fatalf("expected map[string]any in e.Data, got %T", e.Data)
			}
			var exitCode int
			switch v := m["exitCode"].(type) {
			case int:
				exitCode = v
			case float64:
				exitCode = int(v)
			default:
				t.Fatalf("unexpected type for exitCode: %T", m["exitCode"])
			}
			if exitCode != 2 {
				t.Errorf("exitCode = %d, want 2", exitCode)
			}
			if m["tail"] != tailOutput {
				t.Errorf("tail = %q, want %q", m["tail"], tailOutput)
			}
		}
		if e.Kind == "commits_exported" {
			foundExported = true
		}
	}
	if !foundStarted || !foundFailed {
		t.Errorf("expected verify_started and verify_failed events, got: %+v", events)
	}
	if foundExported {
		t.Errorf("unexpected commits_exported event when verify failed")
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_Verify_TimeoutEndsInNoImprovement(t *testing.T) {
	ctx := t.Context()
	const verifyCmd = "sleep 10"
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	var handlerCalled bool
	var handlerMu sync.Mutex
	procSrv := &scriptedExportServer{
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				handlerMu.Lock()
				handlerCalled = true
				handlerMu.Unlock()
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
				<-stream.Context().Done()
				return stream.Context().Err()
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, "base-sha", procSrv, nil)
	orch.verifyTimeout = 50 * time.Millisecond
	run.Goal.Verify = verifyCmd
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	handlerMu.Lock()
	invoked := handlerCalled
	handlerMu.Unlock()
	if !invoked {
		t.Fatalf("expected timeout handler for %q to be invoked, but it was not", expectedVerifyShell)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("got phase %s, want NoImprovement", gotRun.Phase)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundTimeout bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Kind == "verify_failed" && strings.Contains(e.Message, "timed out") {
			foundTimeout = true
		}
	}
	if !foundTimeout {
		t.Errorf("expected verify_failed event mentioning timeout, got: %+v", events)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_Verify_ExecutionErrorFailsRun(t *testing.T) {
	ctx := t.Context()
	const verifyCmd = "make test"
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	procSrv := &scriptedExportServer{
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				return status.Errorf(codes.Unavailable, "sandbox connection lost")
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, "base-sha", procSrv, nil)
	run.Goal.Verify = verifyCmd
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("got phase %s, want Failed", gotRun.Phase)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundError bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "verify_error" {
			foundError = true
		}
	}
	if !foundError {
		t.Errorf("expected verify_error event, got: %+v", events)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_GoalWithoutVerify_RunsNoCommandBeforeExport(t *testing.T) {
	ctx := t.Context()
	_, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)
	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, baseSHA, procSrv, nil)
	run.Goal.Verify = ""
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// Explicit assertion: goal without verify runs no verify command before export.
	procSrv.mu.Lock()
	for _, call := range procSrv.startCalls {
		if strings.Contains(call, "sh -c") {
			t.Errorf("unexpected command run in sandbox for goal without verify: %s", call)
		}
	}
	const wantExportCall = "git -C /workspace/repo pack-objects --stdout --no-thin -q"
	if len(procSrv.startCalls) != 1 || procSrv.startCalls[0] != wantExportCall {
		t.Errorf("expected only pack-objects Start call for export (%q), got: %v", wantExportCall, procSrv.startCalls)
	}
	procSrv.mu.Unlock()

	// Verify no verify events were appended.
	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	for _, e := range events {
		if strings.HasPrefix(e.Kind, "verify_") {
			t.Errorf("unexpected verify event for goal without verify: %+v", e)
		}
	}

	// Verify commits_exported was recorded.
	var foundExported bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "commits_exported" {
			foundExported = true
			break
		}
	}
	if !foundExported {
		t.Errorf("expected commits_exported event, got: %+v", events)
	}

	// Verify sandbox is deleted.
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func setupOpeningPRTest(t *testing.T, forgeFor func(string) (forge.Forge, error)) (*Orchestrator, state.Store, *state.Repo, *state.Run) {
	t.Helper()
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL := "https://github.com/example/repo"
	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261106T120000Z-testpr"
	baseSHA := "1189a319f389802669884696409ba28db20216b0"
	branchName := "ao/optimize-speed/" + runID
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-speed",
		Phase:      state.PhaseOpeningPR,
		BaseCommit: baseSHA,
		Branch:     branchName,
		PRTitle:    "Speed up queries",
		PRBody:     "Replaced linear scan with hash map lookup.",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, _, _ := setupTestHarness(t, nil)
	fixedTime := time.Date(2026, 11, 6, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		PublicURL:    "https://ao.example.com",
		Clock:        func() time.Time { return fixedTime },
		ForgeFor:     forgeFor,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	return orch, store, repo, run
}

func TestOrchestrator_OpeningPR_OpensPullRequest_Succeeds(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded with PRURL set
	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("got phase %s, want Succeeded", gotRun.Phase)
	}

	// 2. Verify FakeForge recorded exactly one PR with expected head, base, title and body
	prs := fakeForge.PullRequests(repo.URL)
	if len(prs) != 1 {
		t.Fatalf("expected exactly 1 pull request, got %d", len(prs))
	}
	pr := prs[0]
	if pr.Head != run.Branch || pr.Base != "main" || pr.Title != run.PRTitle {
		t.Errorf("unexpected PR details: %+v", pr)
	}
	if gotRun.PRURL != pr.URL {
		t.Errorf("run.PRURL = %q, want %q", gotRun.PRURL, pr.URL)
	}

	// Body must contain goal, base commit (short), branch, run ID, link to run page, and agent's text under heading
	shortSHA := run.BaseCommit[:7]
	for _, substr := range []string{run.GoalName, shortSHA, run.Branch, run.ID, "https://ao.example.com/runs/" + run.ID, "What the agent says", run.PRBody} {
		if !strings.Contains(pr.Body, substr) {
			t.Errorf("expected body to contain %q, got:\n%s", substr, pr.Body)
		}
	}

	// 3. Verify event pull_request_opened
	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundOpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_opened" && e.Message == pr.URL {
			foundOpened = true
		}
	}
	if !foundOpened {
		t.Errorf("expected pull_request_opened event carrying %s, got: %+v", pr.URL, events)
	}
}

func TestOrchestrator_OpeningPR_ReusesOpenPullRequest(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	// Pre-create an open pull request for the branch
	existingPR, err := fakeForge.OpenPullRequest(ctx, "https://github.com/example/repo", "ao/optimize-speed/20261106T120000Z-testpr", "main", "Existing PR", "Existing body")
	if err != nil {
		t.Fatalf("OpenPullRequest: %v", err)
	}

	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded with existing PR URL recorded
	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("got phase %s, want Succeeded", gotRun.Phase)
	}
	if gotRun.PRURL != existingPR.URL {
		t.Errorf("got PRURL %q, want %q", gotRun.PRURL, existingPR.URL)
	}

	// 2. Verify no second PR was opened
	if len(fakeForge.PullRequests(repo.URL)) != 1 {
		t.Errorf("expected no second PR opened, got %d", len(fakeForge.PullRequests(repo.URL)))
	}

	// 3. Verify event pull_request_reused
	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundReused bool
	for _, e := range events {
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_reused" && e.Message == existingPR.URL {
			foundReused = true
		}
	}
	if !foundReused {
		t.Errorf("expected pull_request_reused event carrying %s, got: %+v", existingPR.URL, events)
	}
}

func TestOrchestrator_OpeningPR_GitRemote_CannotOpenPR(t *testing.T) {
	ctx := t.Context()
	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return forge.NewGitRemote(), nil
	})

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded without PRURL
	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("got phase %s, want Succeeded", gotRun.Phase)
	}
	if gotRun.PRURL != "" {
		t.Errorf("got PRURL %q, want empty", gotRun.PRURL)
	}

	// 2. Verify event pull_request_not_opened says forge cannot open pull requests
	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundNotOpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_not_opened" && e.Message == "forge cannot open pull requests" {
			foundNotOpened = true
		}
	}
	if !foundNotOpened {
		t.Errorf("expected pull_request_not_opened event with 'forge cannot open pull requests', got: %+v", events)
	}
}

func TestOrchestrator_OpeningPR_OpenPullRequestError_Fails(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	fakeForge.SetOpenPullRequestError(errors.New("API rate limit exceeded"))

	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: run.ID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed || gotRun.Error != "API rate limit exceeded" {
		t.Errorf("got phase %s error %q, want Failed/'API rate limit exceeded'", gotRun.Phase, gotRun.Error)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFailed bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "pull_request_failed" && e.Message == "API rate limit exceeded" {
			foundFailed = true
		}
	}
	if !foundFailed {
		t.Errorf("expected pull_request_failed event with 'API rate limit exceeded', got: %+v", events)
	}
}

func TestOrchestrator_Selecting_PushesAndOpensPullRequest_WithFakeForge(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, bare, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-pushandpr"
	sandboxName := "run-20261030t120000z-pushandpr"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
		PRTitle:     "Add feature",
		PRBody:      "Implements feature",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	// FakeForge implements Forge and ChangeRequestCreator. For file:// remotes,
	// PushPackfile pushes directly to the bare git repository without requiring PushCredentialer.
	fakeForge := forge.NewFake()

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return fakeForge, nil
		},
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded with Branch and PRURL set
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("expected phase %s, got %s", state.PhaseSucceeded, gotRun.Phase)
	}
	wantBranch := "ao/export-goal/" + runID
	if gotRun.Branch != wantBranch {
		t.Errorf("expected branch %q, got %q", wantBranch, gotRun.Branch)
	}
	prs := fakeForge.PullRequests(repo.URL)
	if len(prs) != 1 {
		t.Fatalf("expected 1 pull request, got %d", len(prs))
	}
	if gotRun.PRURL != prs[0].URL {
		t.Errorf("expected PRURL %q, got %q", prs[0].URL, gotRun.PRURL)
	}

	// 2. Verify bare remote has branch at tipSHA
	refName := plumbing.ReferenceName("refs/heads/" + wantBranch)
	ref, err := bare.Reference(refName, true)
	if err != nil {
		t.Fatalf("bare.Reference(%s): %v", refName, err)
	}
	if ref.Hash().String() != tipSHA {
		t.Errorf("expected remote ref %s to be %s, got %s", refName, tipSHA, ref.Hash().String())
	}

	// 3. Verify events: commits_exported, branch_pushed, pull_request_opened
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundExported, foundPushed, foundPROpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "commits_exported" {
			foundExported = true
		}
		if e.Phase == state.PhaseSelecting && e.Kind == "branch_pushed" {
			foundPushed = true
		}
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_opened" && e.Message == prs[0].URL {
			foundPROpened = true
		}
	}
	if !foundExported || !foundPushed || !foundPROpened {
		t.Errorf("expected commits_exported, branch_pushed, pull_request_opened events, got: %+v", events)
	}
}

type blockingProcessServer struct {
	processv1.UnimplementedProcessServiceServer
	onCommand func(cmd []string)
}

func (s *blockingProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	if s.onCommand != nil && req.Config != nil {
		s.onCommand(req.Config.Command)
	}
	return &processv1.ExecuteResponse{
		ExitCode: 0,
		Stdout:   []byte("ok\n"),
	}, nil
}

type blockingToolLLM struct {
	mu        sync.Mutex
	callCount int
}

func (b *blockingToolLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.callCount++
	if b.callCount == 1 {
		args, _ := json.Marshal(map[string]any{"command": "block-test"})
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-block-1",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
				},
			},
		}, nil
	}
	content := "done"
	return &llm.ChatCompletionResponse{
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role:    "assistant",
					Content: &content,
				},
			},
		},
	}, nil
}

func TestOrchestrator_TurnWorker_DoesNotBlockOtherRepoWorkingReconcile(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	repo1 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo1",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo1",
		DefaultBranch: "main",
	}
	repo2 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo2",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo2",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo1); err != nil {
		t.Fatalf("PutRepo 1: %v", err)
	}
	if err := store.PutRepo(ctx, repo2); err != nil {
		t.Fatalf("PutRepo 2: %v", err)
	}

	run1ID := "20261030T120000Z-run1"
	sb1Name := "run-20261030t120000z-run1"
	run1 := &state.Run{
		ID:          run1ID,
		Repo:        repo1.Key(),
		GoalName:    "goal1",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base1",
		SandboxName: sb1Name,
		Goal: &goals.Goal{
			Name: "goal1",
			Body: "run1 goal",
		},
	}
	if err := store.PutRun(ctx, run1); err != nil {
		t.Fatalf("PutRun 1: %v", err)
	}

	run2ID := "20261030T120000Z-run2"
	sb2Name := "run-20261030t120000z-run2"
	run2 := &state.Run{
		ID:          run2ID,
		Repo:        repo2.Key(),
		GoalName:    "goal2",
		Phase:       state.PhasePreparing,
		BaseCommit:  "commit-base2",
		SandboxName: sb2Name,
	}
	if err := store.PutRun(ctx, run2); err != nil {
		t.Fatalf("PutRun 2: %v", err)
	}

	toolEntered := make(chan struct{})
	toolRelease := make(chan struct{})
	var enteredOnce sync.Once

	procSrv := &blockingProcessServer{
		onCommand: func(cmd []string) {
			for _, arg := range cmd {
				if arg == "block-test" {
					enteredOnce.Do(func() { close(toolEntered) })
					<-toolRelease
					return
				}
			}
		},
	}

	blockingLLM := &blockingToolLLM{}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, blockingLLM)

	// Create sandboxes in fake k8s
	policy := sandboxv1beta1.ShutdownPolicyDelete
	for _, sbName := range []string{sb1Name, sb2Name} {
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: "ao-sandboxes"},
			Spec: sandboxv1beta1.SandboxSpec{
				Lifecycle: sandboxv1beta1.Lifecycle{
					ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
					ShutdownPolicy: &policy,
				},
			},
			Status: sandboxv1beta1.SandboxStatus{
				Conditions: []metav1.Condition{
					{
						Type:   string(sandboxv1beta1.SandboxConditionReady),
						Status: metav1.ConditionTrue,
					},
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("creating sandbox %s: %v", sbName, err)
		}
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// 1. Reconcile run1 (Working). This starts the turn worker, which runs in a goroutine.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo1.Key(), RunID: run1ID}); err != nil {
		t.Fatalf("orch.Reconcile run1: %v", err)
	}

	// Wait for run1's worker to enter the blocking tool call.
	select {
	case <-toolEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run1 to enter blocking tool call")
	}

	// 2. While run1's tool call is blocked, reconcile run2 from Preparing to Working.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo2.Key(), RunID: run2ID}); err != nil {
		t.Fatalf("orch.Reconcile run2: %v", err)
	}

	// Verify run2 reached Working phase even though run1 is still blocked in its turn.
	gotRun2, err := store.GetRun(ctx, repo2.Key(), run2ID)
	if err != nil {
		t.Fatalf("GetRun run2: %v", err)
	}
	if gotRun2.Phase != state.PhaseWorking {
		t.Fatalf("expected run2 to reach Working while run1 was blocked, got %s", gotRun2.Phase)
	}

	// 3. Release run1 and let it finish.
	close(toolRelease)

	deadline := time.Now().Add(5 * time.Second)
	for orch.isRunActive(run1ID) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for run1 worker to finish")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Reconcile run1 to consume outcome.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo1.Key(), RunID: run1ID}); err != nil {
		t.Fatalf("orch.Reconcile run1 outcome: %v", err)
	}

	gotRun1, err := store.GetRun(ctx, repo1.Key(), run1ID)
	if err != nil {
		t.Fatalf("GetRun run1: %v", err)
	}
	if gotRun1.Phase != state.PhaseNoImprovement {
		t.Errorf("expected run1 to reach NoImprovement, got %s", gotRun1.Phase)
	}
}

func TestOrchestrator_HandleMap_EmptyAfterTerminalTransition(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	// Case 1: Terminal transition to PhaseNoImprovement
	t.Run("NoImprovement", func(t *testing.T) {
		runID := "20261030T120000Z-term-noimp"
		sandboxName := "run-term-noimp"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "goal-noimp",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}

		harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})
		policy := sandboxv1beta1.ShutdownPolicyDelete
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
			Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("creating sandbox: %v", err)
		}

		orch, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}

		if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err != nil {
			t.Fatalf("reconcileWorkingRun: %v", err)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseNoImprovement {
			t.Fatalf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
		}

		// Handle map must be empty after terminal transition
		if count := harness.TrackedSandboxesCount(); count != 0 {
			t.Errorf("expected tracked sandboxes count 0 after terminal transition to NoImprovement, got %d", count)
		}
	})

	// Case 2: Terminal transition to PhaseFailed
	t.Run("Failed", func(t *testing.T) {
		runID := "20261030T120000Z-term-fail"
		sandboxName := "run-term-fail"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "goal-fail",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}

		// Intentionally do not create sandbox so it fails with expired/deleted
		harness, _, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})
		orch, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}

		if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err == nil {
			t.Logf("reconcileWorkingRun completed (errors handled)")
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Fatalf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
		}

		// Handle map must be empty after terminal transition
		if count := harness.TrackedSandboxesCount(); count != 0 {
			t.Errorf("expected tracked sandboxes count 0 after terminal transition to Failed, got %d", count)
		}
	})
}

func TestOrchestrator_TurnOutcome_ConsumedOnceNotTwice(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-consumed-once"
	sandboxName := "run-consumed-once"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "goal-consume",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})
	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Pre-store an outcome for the run
	orch.storeTurnOutcome(runID, turnOutcome{
		Err:      nil,
		Finished: fixedTime,
	})

	// Pass 1: Reconcile consumes the outcome and moves run to NoImprovement
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile pass 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Fatalf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	// Verify outcome was removed upon consumption
	if _, ok := orch.popTurnOutcome(runID); ok {
		t.Fatal("expected turn outcome to be deleted once consumed, but was still present")
	}

	// Pass 2: Reconcile again with no outcome stored
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile pass 2: %v", err)
	}

	// Outcome was not consumed twice (map is still empty)
	if _, ok := orch.popTurnOutcome(runID); ok {
		t.Fatal("outcome appeared again after pass 2")
	}
}

type scriptedMultiToolLLM struct {
	mu           sync.Mutex
	callCount    int
	step3Entered chan struct{}
	step3Allowed chan struct{}
}

func (s *scriptedMultiToolLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	s.mu.Lock()
	s.callCount++
	count := s.callCount
	s.mu.Unlock()

	switch count {
	case 1:
		args, _ := json.Marshal(map[string]any{"command": "echo step1"})
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-1",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
				},
			},
		}, nil
	case 2:
		args, _ := json.Marshal(map[string]any{"command": "echo step2"})
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-2",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
				},
			},
		}, nil
	default:
		if s.step3Entered != nil {
			select {
			case <-s.step3Entered:
			default:
				close(s.step3Entered)
			}
		}
		if s.step3Allowed != nil {
			<-s.step3Allowed
		}
		content := "done"
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role:    "assistant",
						Content: &content,
					},
				},
			},
		}, nil
	}
}

func findGoroutinesContaining(target string) []string {
	buf := make([]byte, 64*1024)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])
	var matches []string
	for _, g := range strings.Split(stacks, "\n\n") {
		if strings.Contains(g, target) {
			matches = append(matches, g)
		}
	}
	return matches
}

func TestOrchestrator_CancelWorker(t *testing.T) {
	orch := &Orchestrator{
		workerCancels: make(map[string]context.CancelFunc),
	}

	cancelled := false
	cancel := context.CancelFunc(func() {
		cancelled = true
	})

	orch.setWorkerCancel("run-1", cancel)
	orch.cancelWorker("run-1")

	if !cancelled {
		t.Errorf("expected worker to be cancelled")
	}

	// Calling cancelWorker on nonexistent run should not panic
	orch.cancelWorker("nonexistent")
}

func TestOrchestrator_PrepareRun_DeadlineAndShutdownTime(t *testing.T) {
	t.Run("goal with budget.maxDuration 30m sets deadline 30m out and shutdownTime with grace", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

		repo := &state.Repo{
			URL:           "https://github.com/gke-labs/test-repo",
			Host:          "github.com",
			Owner:         "gke-labs",
			Name:          "test-repo",
			DefaultBranch: "main",
		}
		if err := store.PutRepo(ctx, repo); err != nil {
			t.Fatalf("PutRepo: %v", err)
		}

		currentTime := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)
		runID := "20261110T100000Z-budget30m"
		run := &state.Run{
			ID:       runID,
			Repo:     repo.Key(),
			GoalName: "budget-30m",
			Phase:    state.PhasePending,
			Goal: &goals.Goal{
				Name: "budget-30m",
				Budget: &goals.Budget{
					MaxDuration: "30m",
				},
			},
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}

		harness, agentsCS, _ := setupTestHarness(t, nil)
		orch, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}

		if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhasePreparing {
			t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
		}

		wantDeadline := currentTime.Add(30 * time.Minute)
		if gotRun.Deadline == nil || !gotRun.Deadline.Equal(wantDeadline) {
			t.Errorf("expected Deadline %v, got %v", wantDeadline, gotRun.Deadline)
		}

		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get sandbox: %v", err)
		}
		wantShutdown := wantDeadline.Add(SandboxShutdownGrace)
		if sb.Spec.ShutdownTime == nil || !sb.Spec.ShutdownTime.Time.Equal(wantShutdown) {
			t.Errorf("expected sandbox shutdown time %v, got %v", wantShutdown, sb.Spec.ShutdownTime)
		}
	})

	t.Run("goal without budget.maxDuration uses default max duration", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

		repo := &state.Repo{
			URL:           "https://github.com/gke-labs/test-repo",
			Host:          "github.com",
			Owner:         "gke-labs",
			Name:          "test-repo",
			DefaultBranch: "main",
		}
		if err := store.PutRepo(ctx, repo); err != nil {
			t.Fatalf("PutRepo: %v", err)
		}

		currentTime := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)
		runID := "20261110T100000Z-nobudget"
		run := &state.Run{
			ID:       runID,
			Repo:     repo.Key(),
			GoalName: "no-budget",
			Phase:    state.PhasePending,
			Goal: &goals.Goal{
				Name: "no-budget",
			},
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}

		harness, agentsCS, _ := setupTestHarness(t, nil)
		orch, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}

		if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhasePreparing {
			t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
		}

		wantDeadline := currentTime.Add(DefaultMaxDuration)
		if gotRun.Deadline == nil || !gotRun.Deadline.Equal(wantDeadline) {
			t.Errorf("expected Deadline %v, got %v", wantDeadline, gotRun.Deadline)
		}

		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get sandbox: %v", err)
		}
		wantShutdown := wantDeadline.Add(SandboxShutdownGrace)
		if sb.Spec.ShutdownTime == nil || !sb.Spec.ShutdownTime.Time.Equal(wantShutdown) {
			t.Errorf("expected sandbox shutdown time %v, got %v", wantShutdown, sb.Spec.ShutdownTime)
		}
	})

	t.Run("goal with invalid budget.maxDuration fails run with clear event", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

		repo := &state.Repo{
			URL:           "https://github.com/gke-labs/test-repo",
			Host:          "github.com",
			Owner:         "gke-labs",
			Name:          "test-repo",
			DefaultBranch: "main",
		}
		if err := store.PutRepo(ctx, repo); err != nil {
			t.Fatalf("PutRepo: %v", err)
		}

		runID := "20261110T100000Z-invalid-budget"
		run := &state.Run{
			ID:       runID,
			Repo:     repo.Key(),
			GoalName: "bad-budget",
			Phase:    state.PhasePending,
			Goal: &goals.Goal{
				Name: "bad-budget",
				Budget: &goals.Budget{
					MaxDuration: "invalid-duration-value",
				},
			},
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}

		harness, agentsCS, _ := setupTestHarness(t, nil)
		orch, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}

		if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
		}
		if !strings.Contains(gotRun.Error, "invalid budget.maxDuration") {
			t.Errorf("expected error containing 'invalid budget.maxDuration', got %q", gotRun.Error)
		}

		events, err := store.ReadEvents(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		foundFailedEvent := false
		for _, e := range events {
			if e.Phase == state.PhaseFailed && strings.Contains(e.Message, "invalid budget.maxDuration") {
				foundFailedEvent = true
				break
			}
		}
		if !foundFailedEvent {
			t.Errorf("expected failure event with clear message, got events: %+v", events)
		}

		// Ensure sandbox was not created
		sbList, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("List sandboxes: %v", err)
		}
		if len(sbList.Items) != 0 {
			t.Errorf("expected 0 sandboxes created, got %d", len(sbList.Items))
		}
	})
}

func TestOrchestrator_Session_NeverUpdatesSandboxShutdownTime(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-no-extend-test"
	sandboxName := "run-no-extend-test"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "no-extend-test",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "no-extend-test",
			Body: "test goal",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	step1Finished := make(chan struct{})
	step2Entered := make(chan struct{})
	step2Allowed := make(chan struct{})
	step2Finished := make(chan struct{})
	step3Entered := make(chan struct{})
	step3Allowed := make(chan struct{})

	procSrv := &blockingProcessServer{
		onCommand: func(cmd []string) {
			for _, arg := range cmd {
				if strings.Contains(arg, "step1") {
					close(step1Finished)
					return
				}
				if strings.Contains(arg, "step2") {
					close(step2Entered)
					<-step2Allowed
					close(step2Finished)
					return
				}
			}
		},
	}

	scriptedLLM := &scriptedMultiToolLLM{
		step3Entered: step3Entered,
		step3Allowed: step3Allowed,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, scriptedLLM)

	currentTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	initialShutdown := currentTime.Add(1*time.Hour + SandboxShutdownGrace)
	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: initialShutdown},
				ShutdownPolicy: &policy,
			},
		},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: metav1.ConditionTrue,
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	clockMu := sync.Mutex{}
	getClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return currentTime
	}
	advanceClock := func(d time.Duration) time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		currentTime = currentTime.Add(d)
		return currentTime
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        getClock,
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// 1. First reconcile pass starts the turn worker. RequeueAfter should be 0.
	res, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile start: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected RequeueAfter 0, got %v", res.RequeueAfter)
	}

	// Wait for tool 1 to execute and finish, and tool 2 to be entered and blocked
	select {
	case <-step1Finished:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 1 to execute")
	}

	select {
	case <-step2Entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 2 to be entered")
	}

	// While blocked in step 2: advance clock and reconcile while worker is running
	advanceClock(5 * time.Minute)
	res1, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile after tool 1: %v", err)
	}
	if res1.RequeueAfter != 0 {
		t.Errorf("expected RequeueAfter 0, got %v", res1.RequeueAfter)
	}

	// Verify sandbox ShutdownTime was NEVER updated during session
	sb1, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	if sb1.Spec.ShutdownTime == nil || !sb1.Spec.ShutdownTime.Time.Equal(initialShutdown) {
		t.Errorf("expected sandbox shutdown time to remain %v, got %v", initialShutdown, sb1.Spec.ShutdownTime)
	}

	// 2. Allow tool 2 to complete and wait for step 3 to be entered
	close(step2Allowed)
	select {
	case <-step2Finished:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 2 to finish")
	}

	select {
	case <-step3Entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 3 to be entered")
	}

	// Advance clock again and reconcile while worker is waiting on step3Allowed
	advanceClock(10 * time.Minute)
	res2, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile after tool 2: %v", err)
	}
	if res2.RequeueAfter != 0 {
		t.Errorf("expected RequeueAfter 0, got %v", res2.RequeueAfter)
	}

	// Verify sandbox ShutdownTime still unchanged after second tool call
	sb2, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	if sb2.Spec.ShutdownTime == nil || !sb2.Spec.ShutdownTime.Time.Equal(initialShutdown) {
		t.Errorf("expected sandbox shutdown time to remain unchanged %v, got %v", initialShutdown, sb2.Spec.ShutdownTime)
	}

	// 3. Allow LLM call 3 to complete ("done")
	close(step3Allowed)

	// Wait for worker goroutine to exit
	deadline := time.Now().Add(5 * time.Second)
	for orch.isRunActive(runID) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to finish")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Reconcile to consume outcome
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile outcome: %v", err)
	}

	gotRunFinal, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRunFinal.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRunFinal.Phase)
	}

	// Verify sandbox was deleted upon run completion
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted upon run completion, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// Verify no goroutine other than test is left after turn ends
	leftoverWorkers := findGoroutinesContaining("runTurnWorker")
	if len(leftoverWorkers) != 0 {
		t.Errorf("expected 0 worker goroutines left after turn ends, got %d: %v", len(leftoverWorkers), leftoverWorkers)
	}
}

func TestOrchestrator_Working_SandboxExpiredAtStart_ProducesSandboxExpiredOutcome(t *testing.T) {
	t.Run("already expired sandbox at worker start", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

		repo := &state.Repo{
			URL:           "https://github.com/gke-labs/test-repo",
			Host:          "github.com",
			Owner:         "gke-labs",
			Name:          "test-repo",
			DefaultBranch: "main",
		}
		if err := store.PutRepo(ctx, repo); err != nil {
			t.Fatalf("PutRepo: %v", err)
		}

		runID := "20261030T120000Z-expired-start"
		sandboxName := "run-expired-start"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "expired-start",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
			Goal: &goals.Goal{
				Name: "expired-start",
				Body: "test goal",
			},
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}

		harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})

		currentTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
		// Shutdown time in the past
		pastShutdown := currentTime.Add(-5 * time.Minute)
		policy := sandboxv1beta1.ShutdownPolicyDelete
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sandboxName,
				Namespace: "ao-sandboxes",
			},
			Spec: sandboxv1beta1.SandboxSpec{
				Lifecycle: sandboxv1beta1.Lifecycle{
					ShutdownTime:   &metav1.Time{Time: pastShutdown},
					ShutdownPolicy: &policy,
				},
			},
			Status: sandboxv1beta1.SandboxStatus{
				Conditions: []metav1.Condition{
					{
						Type:   string(sandboxv1beta1.SandboxConditionReady),
						Status: metav1.ConditionTrue,
					},
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("creating sandbox: %v", err)
		}

		orch, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}

		if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err == nil {
			t.Logf("reconcileWorkingRun completed (errors handled)")
		}

		failedRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if failedRun.Phase != state.PhaseFailed {
			t.Errorf("expected phase %s, got %s", state.PhaseFailed, failedRun.Phase)
		}

		events, err := store.ReadEvents(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		foundExpiredEvent := false
		for _, e := range events {
			if e.Kind == "sandbox_expired" && e.Phase == state.PhaseFailed {
				foundExpiredEvent = true
				break
			}
		}
		if !foundExpiredEvent {
			t.Errorf("expected event kind sandbox_expired, got events: %+v", events)
		}
	})

	t.Run("missing sandbox at worker start", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

		repo := &state.Repo{
			URL:           "https://github.com/gke-labs/test-repo",
			Host:          "github.com",
			Owner:         "gke-labs",
			Name:          "test-repo",
			DefaultBranch: "main",
		}
		if err := store.PutRepo(ctx, repo); err != nil {
			t.Fatalf("PutRepo: %v", err)
		}

		runID := "20261030T120000Z-missing-start"
		sandboxName := "run-missing-start"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "missing-start",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
			Goal: &goals.Goal{
				Name: "missing-start",
				Body: "test goal",
			},
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}

		harness, _, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})

		orch, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}

		if _, err := reconcileWorkingRun(ctx, orch, Request{Repo: repo.Key(), RunID: runID}); err == nil {
			t.Logf("reconcileWorkingRun completed (errors handled)")
		}

		failedRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if failedRun.Phase != state.PhaseFailed {
			t.Errorf("expected phase %s, got %s", state.PhaseFailed, failedRun.Phase)
		}

		events, err := store.ReadEvents(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		foundExpiredEvent := false
		for _, e := range events {
			if e.Kind == "sandbox_expired" && e.Phase == state.PhaseFailed {
				foundExpiredEvent = true
				break
			}
		}
		if !foundExpiredEvent {
			t.Errorf("expected event kind sandbox_expired, got events: %+v", events)
		}
	})
}
