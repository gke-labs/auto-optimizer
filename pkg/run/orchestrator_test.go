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
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	informers "sigs.k8s.io/agent-sandbox/clients/k8s/informers/externalversions"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	crcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/portal"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

type mockProcessServer struct {
	processv1.UnimplementedProcessServiceServer

	mu              sync.Mutex
	commands        [][]string
	exitCode        int32
	stderr          string
	stdout          string
	err             error
	workspaceReused bool
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

	cmd := req.GetConfig().GetCommand()
	if len(cmd) >= 4 && cmd[0] == "git" && cmd[1] == "-C" && cmd[2] == "/workspace/repo" && cmd[3] == "rev-parse" {
		if m.workspaceReused {
			return &processv1.ExecuteResponse{
				ExitCode: 0,
				Stdout:   []byte(".git\n"),
			}, nil
		}
		if m.exitCode != 0 && m.exitCode != 128 {
			return &processv1.ExecuteResponse{
				ExitCode: m.exitCode,
				Stderr:   []byte(m.stderr),
			}, nil
		}
		return &processv1.ExecuteResponse{
			ExitCode: 128,
			Stderr:   []byte("fatal: not a git repository (or any of the parent directories): .git\n"),
		}, nil
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

func (o *Orchestrator) cancelWorkers() {
	o.activeRunsMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(o.workerCancels))
	for _, cancel := range o.workerCancels {
		if cancel != nil {
			cancels = append(cancels, cancel)
		}
	}
	o.activeRunsMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func newTestOrchestrator(t *testing.T, opts OrchestratorOptions) *Orchestrator {
	t.Helper()
	orch, err := NewOrchestrator(opts)
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	t.Cleanup(func() {
		orch.cancelWorkers()
		orch.workerWG.Wait()
	})
	return orch
}

func runRequest(orch *Orchestrator, repo state.RepoKey, runID string) reconcile.Request {
	name := sandboxNameForRun(runID)
	orch.recordRunIndex(name, repo, runID)
	var ns string
	if orch != nil && orch.harness != nil {
		ns = orch.harness.Namespace()
	}
	return reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: ns,
			Name:      name,
		},
	}
}

func reconcileWorkingRun(ctx context.Context, orch *Orchestrator, req reconcile.Request) (reconcile.Result, error) {
	_, runID, ok, err := orch.runForRequest(ctx, req)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !ok {
		return reconcile.Result{}, fmt.Errorf("could not resolve request %v to run", req)
	}
	// First pass starts the worker
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		return res, err
	}
	// Wait for worker to finish
	deadline := time.Now().Add(5 * time.Second)
	for orch.isRunActive(runID) {
		if time.Now().After(deadline) {
			return res, fmt.Errorf("timed out waiting for worker on run %s", runID)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: author,
	})

	// Step 1: Reconcile Pending run.
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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

	// 6. Verify rev-parse, clone, checkout, and git config commands were issued directly without shell
	procSrv.mu.Lock()
	cmds = procSrv.commands
	procSrv.mu.Unlock()

	if len(cmds) != 5 {
		t.Fatalf("expected exactly 5 commands executed in sandbox (rev-parse, clone, checkout, 2 git config), got %d: %+v", len(cmds), cmds)
	}

	revParseCmdStr := strings.Join(cmds[0], " ")
	if revParseCmdStr != "git -C /workspace/repo rev-parse --git-dir" {
		t.Errorf("expected rev-parse command 'git -C /workspace/repo rev-parse --git-dir', got: %s", revParseCmdStr)
	}

	cloneCmdStr := strings.Join(cmds[1], " ")
	if cloneCmdStr != "git clone -- "+repo.URL+" /workspace/repo" {
		t.Errorf("expected clone command 'git clone -- %s /workspace/repo', got: %s", repo.URL, cloneCmdStr)
	}

	checkoutCmdStr := strings.Join(cmds[2], " ")
	if checkoutCmdStr != "git -C /workspace/repo checkout commit-base123" {
		t.Errorf("expected checkout command 'git -C /workspace/repo checkout commit-base123', got: %s", checkoutCmdStr)
	}

	configNameCmdStr := strings.Join(cmds[3], " ")
	if configNameCmdStr != "git -C /workspace/repo config user.name auto-optimizer" {
		t.Errorf("expected config user.name command 'git -C /workspace/repo config user.name auto-optimizer', got: %s", configNameCmdStr)
	}

	configEmailCmdStr := strings.Join(cmds[4], " ")
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: "invalid-author<",
	})

	// Step 1: Reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})

	// Step 1: reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
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

func TestOrchestrator_WorkspaceReuse(t *testing.T) {
	setupReadyRun := func(t *testing.T, runID string, procSrv processv1.ProcessServiceServer) (*Orchestrator, state.Store, *fakeagents.Clientset, *state.Repo, *state.Run) {
		t.Helper()
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
		run := &state.Run{
			ID:         runID,
			Repo:       repo.Key(),
			GoalName:   "optimize",
			Phase:      state.PhasePending,
			BaseCommit: "base123",
		}
		if err := store.PutRun(ctx, run); err != nil {
			t.Fatalf("PutRun: %v", err)
		}
		harness, agentsCS, _ := setupTestHarness(t, procSrv)
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:     store,
			Harness:   harness,
			Clock:     func() time.Time { return time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC) },
			GitAuthor: "bot <bot@test.com>",
		})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("Reconcile step 1: %v", err)
		}
		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting sandbox: %v", err)
		}
		sb.Status.Conditions = []metav1.Condition{{Type: string(sandboxv1beta1.SandboxConditionReady), Status: metav1.ConditionTrue}}
		if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("updating sandbox to Ready: %v", err)
		}
		return orch, store, agentsCS, repo, gotRun
	}

	t.Run("empty workspace clones and checks out", func(t *testing.T) {
		procSrv := &mockProcessServer{}
		orch, store, _, repo, run := setupReadyRun(t, "run-empty", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err != nil {
			t.Fatalf("Reconcile step 2: %v", err)
		}
		gotRun, err := store.GetRun(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseWorking {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseWorking)
		}
		procSrv.mu.Lock()
		cmds := procSrv.commands
		procSrv.mu.Unlock()
		if len(cmds) != 5 {
			t.Fatalf("expected 5 commands (rev-parse, clone, checkout, 2 git config), got %d: %+v", len(cmds), cmds)
		}
		if !strings.Contains(cmds[0][4], "--git-dir") || cmds[1][1] != "clone" || cmds[2][3] != "checkout" {
			t.Errorf("unexpected commands: %+v", cmds)
		}
		events, err := store.ReadEvents(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		for _, e := range events {
			if e.Kind == "workspace_reused" {
				t.Errorf("unexpected workspace_reused event in empty workspace")
			}
		}
	})

	t.Run("existing workspace skips clone and checkout", func(t *testing.T) {
		procSrv := &mockProcessServer{workspaceReused: true}
		orch, store, _, repo, run := setupReadyRun(t, "run-reused", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err != nil {
			t.Fatalf("Reconcile step 2: %v", err)
		}
		gotRun, err := store.GetRun(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseWorking {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseWorking)
		}
		procSrv.mu.Lock()
		cmds := procSrv.commands
		procSrv.mu.Unlock()
		if len(cmds) != 3 {
			t.Fatalf("expected 3 commands (rev-parse, 2 git config), got %d: %+v", len(cmds), cmds)
		}
		if !strings.Contains(cmds[0][4], "--git-dir") {
			t.Errorf("expected rev-parse check, got: %+v", cmds[0])
		}
		for _, cmd := range cmds[1:] {
			cmdStr := strings.Join(cmd, " ")
			if strings.Contains(cmdStr, "clone") || strings.Contains(cmdStr, "checkout") {
				t.Errorf("clone or checkout should have been skipped, got command: %s", cmdStr)
			}
		}
		events, err := store.ReadEvents(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		var foundReused bool
		for _, e := range events {
			if e.Kind == "workspace_reused" {
				foundReused = true
				if m, ok := e.Data.(map[string]any); !ok || m["sandbox"] != run.SandboxName {
					t.Errorf("expected sandbox name in event data, got: %+v", e.Data)
				}
			}
		}
		if !foundReused {
			t.Errorf("expected workspace_reused event, found none")
		}
	})

	t.Run("prepareSandbox is safe to call again on the same sandbox", func(t *testing.T) {
		procSrv := &mockProcessServer{}
		orch, _, _, repo, run := setupReadyRun(t, "run-prepare-twice", procSrv)
		sb := orch.harness.GetOrAttachSandbox(run.SandboxName)
		if sb == nil {
			t.Fatalf("expected sandbox handle, got nil")
		}

		if err := orch.prepareSandbox(t.Context(), repo, run, sb); err != nil {
			t.Fatalf("first prepareSandbox: %v", err)
		}

		// Simulate the workspace now being populated
		procSrv.mu.Lock()
		procSrv.workspaceReused = true
		procSrv.commands = nil
		procSrv.mu.Unlock()

		if err := orch.prepareSandbox(t.Context(), repo, run, sb); err != nil {
			t.Fatalf("second prepareSandbox: %v", err)
		}

		procSrv.mu.Lock()
		cmds := procSrv.commands
		procSrv.mu.Unlock()
		if len(cmds) != 3 {
			t.Fatalf("expected 3 commands on second call (rev-parse, 2 git config), got %d: %+v", len(cmds), cmds)
		}
		for _, cmd := range cmds[1:] {
			cmdStr := strings.Join(cmd, " ")
			if strings.Contains(cmdStr, "clone") || strings.Contains(cmdStr, "checkout") {
				t.Errorf("clone or checkout should have been skipped on second call, got command: %s", cmdStr)
			}
		}
	})

	t.Run("check failure fails step", func(t *testing.T) {
		procSrv := &mockProcessServer{err: errors.New("sandbox process error")}
		orch, store, agentsCS, repo, run := setupReadyRun(t, "run-checkfail", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err == nil {
			t.Logf("Reconcile completed")
		}
		gotRun, err := store.GetRun(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseFailed)
		}
		if !strings.Contains(gotRun.Error, "sandbox process error") {
			t.Errorf("expected error to mention failure, got: %s", gotRun.Error)
		}
		_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(t.Context(), run.SandboxName, metav1.GetOptions{})
		if err == nil || !k8serrors.IsNotFound(err) {
			t.Errorf("expected sandbox to be deleted, got err: %v", err)
		}
	})

	t.Run("check exit code failure fails step", func(t *testing.T) {
		procSrv := &mockProcessServer{exitCode: 1, stderr: "fatal: git check failure\n"}
		orch, store, agentsCS, repo, run := setupReadyRun(t, "run-checkexitfail", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err == nil {
			t.Logf("Reconcile completed")
		}
		gotRun, err := store.GetRun(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseFailed)
		}
		if !strings.Contains(gotRun.Error, "exit code 1") {
			t.Errorf("expected error to mention exit code 1, got: %s", gotRun.Error)
		}
		_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(t.Context(), run.SandboxName, metav1.GetOptions{})
		if err == nil || !k8serrors.IsNotFound(err) {
			t.Errorf("expected sandbox to be deleted, got err: %v", err)
		}
	})
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})

	// Step 1: Reconcile creates sandbox and moves to Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		Harness:      harness,
		Clock:        func() time.Time { return currentTime },
		ReadyTimeout: 10 * time.Second,
	})

	// Step 1: Reconcile creates sandbox and sets ReadyDeadline = currentTime + 10s
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})

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
		if evt.Object.Name != sandboxNameForRun(runID) {
			t.Errorf("expected enqueued run name %s, got %s", sandboxNameForRun(runID), evt.Object.Name)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
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
	msgs, meta, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if meta == nil || meta.Repo != repo.Key().String() || meta.RunID != gotRun.ID {
		t.Errorf("session metadata = %+v, want Repo %q and RunID %q", meta, repo.Key().String(), gotRun.ID)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     3,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err == nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err == nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})

	// Step 1: Pending -> Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile 2 (step 2): %v", err)
	}

	// Step 3: Working -> NoImprovement
	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
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

type reconcileDriver struct {
	t      *testing.T
	orch   *Orchestrator
	queue  workqueue.TypedRateLimitingInterface[reconcile.Request]
	ctx    context.Context
	cancel context.CancelFunc
}

func startReconcileDriver(t *testing.T, orch *Orchestrator) *reconcileDriver {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	queue := workqueue.NewTypedRateLimitingQueue[reconcile.Request](
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)

	d := &reconcileDriver{
		t:      t,
		orch:   orch,
		queue:  queue,
		ctx:    ctx,
		cancel: cancel,
	}

	var workersWG sync.WaitGroup
	workers := orch.maxConcurrentRuns
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			d.worker()
		}()
	}

	t.Cleanup(func() {
		cancel()
		queue.ShutDown()
		workersWG.Wait()
		orch.cancelWorkers()
		orch.workerWG.Wait()
	})

	go func() {
		_ = orch.Start(ctx)
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-orch.notifyCh:
				queue.Add(ev.Object)
			}
		}
	}()

	if orch.harness != nil && orch.harness.SandboxClient() != nil && orch.harness.SandboxClient().AgentsClient() != nil {
		factory := informers.NewSharedInformerFactoryWithOptions(
			orch.harness.SandboxClient().AgentsClient(),
			0,
			informers.WithNamespace(orch.harness.Namespace()),
		)
		informer := factory.Agents().V1beta1().Sandboxes().Informer()
		enqueueSandbox := func(obj any) {
			sb, ok := obj.(*sandboxv1beta1.Sandbox)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				sb, ok = tombstone.Obj.(*sandboxv1beta1.Sandbox)
				if !ok || sb == nil {
					return
				}
			}

			owner := metav1.GetControllerOf(sb)
			if owner == nil || owner.Kind != "HeroRun" {
				return
			}

			queue.Add(reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: sb.Namespace,
					Name:      owner.Name,
				},
			})
		}

		_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    enqueueSandbox,
			UpdateFunc: func(_, newObj any) { enqueueSandbox(newObj) },
			DeleteFunc: enqueueSandbox,
		})
		if err != nil {
			t.Fatalf("adding event handler to sandbox informer: %v", err)
		}

		factory.StartWithContext(ctx)
		if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
			if ctx.Err() == nil {
				t.Fatal("timed out waiting for sandbox informer caches to sync")
			}
		}
	}

	return d
}

func (d *reconcileDriver) worker() {
	for {
		req, shutdown := d.queue.Get()
		if shutdown {
			return
		}
		res, err := d.orch.Reconcile(d.ctx, req)
		if err != nil {
			d.queue.AddRateLimited(req)
		} else if res.RequeueAfter > 0 {
			d.queue.Forget(req)
			d.queue.AddAfter(req, res.RequeueAfter)
		} else if res.Requeue {
			d.queue.AddRateLimited(req)
		} else {
			d.queue.Forget(req)
		}
		d.queue.Done(req)
	}
}

func (d *reconcileDriver) runUntil(cond func() bool) {
	d.t.Helper()
	if cond() {
		return
	}

	deadline := time.Now().Add(10 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cond() {
				return
			}
			if time.Now().After(deadline) {
				d.t.Fatal("timed out waiting for condition in runUntil")
			}
		case <-d.ctx.Done():
			d.t.Fatalf("context cancelled while waiting for condition in runUntil: %v", d.ctx.Err())
		}
	}
}

func TestOrchestrator_ReconciledAfterNotify_WithoutTimer(t *testing.T) {
	ctx := t.Context()

	storeDir := t.TempDir()
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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	scheduler, err := NewScheduler(SchedulerOptions{
		Store:        store,
		Orchestrator: orch,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	d := startReconcileDriver(t, orch)

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
	d.runUntil(func() bool {
		runs, err := store.ListRuns(ctx, repo.Key())
		return err == nil && len(runs) > 0 && runs[0].Phase == state.PhaseWorking
	})
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
	ctx := t.Context()

	storeDir := t.TempDir()
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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:             store,
		Harness:           harness,
		KubeClient:        fakeClient,
		MaxConcurrentRuns: 4,
	})

	d := startReconcileDriver(t, orch)

	orch.Notify(repo1.Key(), run1.ID)
	orch.Notify(repo2.Key(), run2.ID)

	// Wait for both to reach Working
	d.runUntil(func() bool {
		r1, err1 := store.GetRun(ctx, repo1.Key(), run1.ID)
		r2, err2 := store.GetRun(ctx, repo2.Key(), run2.ID)
		return err1 == nil && r1.Phase == state.PhaseWorking &&
			err2 == nil && r2.Phase == state.PhaseWorking
	})
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})

	go func() {
		_ = orch.Start(testCtx)
	}()

	// Reconcile with the cancellable context. The LLM will cancel the context during the session.
	_, _ = orch.Reconcile(testCtx, runRequest(orch, repo.Key(), runID))

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

	// Consuming the outcome must also leave the run in Working
	if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile after cancellation: %v", err)
	}

	// Restart: create a new Orchestrator with the scripted finish LLM and verify run finishes normally.
	finishLLM := &scriptedFinishLLM{summary: "restarted and finished"}
	restartedHarness, newAgentsCS, _ := setupTestHarnessWithLLM(t, nil, finishLLM)
	_, err = newAgentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(t.Context(), &sandboxv1beta1.Sandbox{
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
		t.Fatalf("creating sandbox in fake k8s on restart: %v", err)
	}

	restartedOrch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      restartedHarness,
	})
	if _, err := reconcileWorkingRun(t.Context(), restartedOrch, runRequest(restartedOrch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun on restart: %v", err)
	}

	resRun, err := store.GetRun(t.Context(), repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun on restart: %v", err)
	}
	if resRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s after restart, got %s", state.PhaseNoImprovement, resRun.Phase)
	}
	if resRun.Summary != "restarted and finished" {
		t.Errorf("expected summary 'restarted and finished', got %q", resRun.Summary)
	}
}

func TestOrchestrator_Working_GRPCCanceled_LeavesWorking(t *testing.T) {
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

	runID := "20261030T120000Z-grpccancel"
	sandboxName := "run-20261030t120000z-grpccancel"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "grpc-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, _, _ := setupTestHarnessWithLLM(t, nil, nil)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})

	// Storing a gRPC codes.Canceled outcome (like an in-flight sandbox call being canceled) must leave the run Working.
	grpcErr := status.Error(codes.Canceled, "in-flight sandbox rpc canceled")
	orch.storeTurnOutcome(runID, turnOutcome{
		Err:      fmt.Errorf("in-flight sandbox call: %w", grpcErr),
		Finished: time.Now(),
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s after gRPC codes.Canceled, got %s", state.PhaseWorking, gotRun.Phase)
	}
	if gotRun.Error != "" {
		t.Errorf("expected empty error, got %q", gotRun.Error)
	}
}

func TestOrchestrator_Working_UnansweredToolCall_ContinuesWithoutUserMessage(t *testing.T) {
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

	runID := "20261030T120000Z-unans"
	sandboxName := "run-20261030t120000z-unans"
	sessionID := generateSessionID(runID)
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "unanswered-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-unans",
		SandboxName: sandboxName,
		SessionID:   sessionID,
		Goal: &goals.Goal{
			Name: "unanswered-goal",
			Body: "run command and finish",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	sysPrompt := "system prompt"
	userPrompt := "Please work on the goal."
	if err := sessionStore.NewSession(ctx, sessionID, session.Metadata{}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sessionStore.AppendMessages(ctx, sessionID,
		llm.Message{Role: "system", Content: &sysPrompt},
		llm.Message{Role: "user", Content: &userPrompt},
		llm.Message{
			Role: "assistant",
			ToolCalls: []llm.ToolCall{
				{
					ID:   "call-unans-1",
					Type: "function",
					Function: llm.FunctionCall{
						Name:      "run_command",
						Arguments: `{"command":"uname -a"}`,
					},
				},
			},
		},
	); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Linux unans 6.12.0\n",
	}
	finishSummary := "Unanswered tool executed and finished."
	finishLLM := &scriptedFinishLLM{summary: finishSummary}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, finishLLM)

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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	procSrv.mu.Lock()
	cmds := append([][]string(nil), procSrv.commands...)
	procSrv.mu.Unlock()
	if len(cmds) != 1 {
		t.Fatalf("expected 1 command executed in sandbox, got %d: %v", len(cmds), cmds)
	}

	msgs, _, err := sessionStore.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	userCount := 0
	for _, m := range msgs {
		if m.Role == "user" {
			userCount++
		}
	}
	if userCount != 1 {
		t.Errorf("expected exactly 1 user message in transcript, got %d", userCount)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.Summary != finishSummary {
		t.Errorf("expected summary %q, got %q", finishSummary, gotRun.Summary)
	}
}

func TestOrchestrator_Working_SystemPromptOnly_RunsTurnWithGoal(t *testing.T) {
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

	runID := "20261030T120000Z-sysonly"
	sandboxName := "run-20261030t120000z-sysonly"
	sessionID := generateSessionID(runID)
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "sysonly-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-sysonly",
		SandboxName: sandboxName,
		SessionID:   sessionID,
		Goal: &goals.Goal{
			Name: "sysonly-goal",
			Body: "goal body",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	// Seed session transcript with ONLY a system prompt (no user message)
	sysPrompt := "system prompt"
	if err := sessionStore.NewSession(ctx, sessionID, session.Metadata{}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sessionStore.AppendMessages(ctx, sessionID, llm.Message{Role: "system", Content: &sysPrompt}); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}

	finishLLM := &scriptedFinishLLM{summary: "started from system prompt only"}
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	msgs, _, err := sessionStore.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	var userMsgs []llm.Message
	for _, m := range msgs {
		if m.Role == "user" {
			userMsgs = append(userMsgs, m)
		}
	}
	if len(userMsgs) != 1 {
		t.Fatalf("expected exactly 1 user message, got %d", len(userMsgs))
	}
	if userMsgs[0].Content == nil || *userMsgs[0].Content != "Please work on the goal." {
		t.Errorf("expected user message %q, got %v", "Please work on the goal.", userMsgs[0].Content)
	}
}

func TestOrchestrator_WorkingRunEnqueuedOnStart(t *testing.T) {
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})

	d := startReconcileDriver(t, orch)

	d.runUntil(func() bool {
		r, err := store.GetRun(ctx, repo.Key(), workingRunID)
		return err == nil && r.Phase == state.PhaseNoImprovement
	})

	// Verify Preparing run was enqueued on start and reached Working or NoImprovement
	d.runUntil(func() bool {
		rPrep, err := store.GetRun(ctx, repo.Key(), prepRunID)
		return err == nil && (rPrep.Phase == state.PhaseWorking || rPrep.Phase == state.PhaseNoImprovement)
	})
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		GitAuthor: author,
	})
	if orch.gitAuthor != author {
		t.Errorf("orch.gitAuthor = %q, want %q", orch.gitAuthor, author)
	}
}

func TestOrchestrator_PublicURL(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		PublicURL: "https://ao.example.com/",
	})
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
	ctx := t.Context()

	storeDir := t.TempDir()
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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	d := startReconcileDriver(t, orch)

	orch.Notify(repo.Key(), runID)

	// 1. Wait until the run enters Preparing and has a Sandbox created
	var sb *sandboxv1beta1.Sandbox
	d.runUntil(func() bool {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if s, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sb = s
				return true
			}
		}
		return false
	})

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
	d.runUntil(func() bool {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		return err == nil && r.Phase == state.PhaseWorking
	})

	procSrv.mu.Lock()
	finalCmds := len(procSrv.commands)
	procSrv.mu.Unlock()
	if finalCmds != 3 {
		t.Fatalf("expected 3 commands executed after Ready, got %d", finalCmds)
	}
}

func TestOrchestrator_Preparing_WatchDeleteEnqueuesAndFailsRun(t *testing.T) {
	ctx := t.Context()

	storeDir := t.TempDir()
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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	d := startReconcileDriver(t, orch)

	orch.Notify(repo.Key(), runID)

	// Wait until run enters Preparing and sandbox exists in Kubernetes
	var sbName string
	d.runUntil(func() bool {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if _, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sbName = r.SandboxName
				return true
			}
		}
		return false
	})

	// Delete fake sandbox before readiness
	if err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, sbName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting fake sandbox: %v", err)
	}

	// Verify watch detects deletion, enqueues run, and marks it Failed
	d.runUntil(func() bool {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		return err == nil && r.Phase == state.PhaseFailed
	})

	r, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if !strings.Contains(r.Error, "deleted before becoming ready") {
		t.Errorf("expected error to mention deleted before becoming ready, got: %q", r.Error)
	}
}

func TestOrchestrator_Preparing_WatchFailureConditionFailsRun(t *testing.T) {
	ctx := t.Context()

	storeDir := t.TempDir()
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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	d := startReconcileDriver(t, orch)

	orch.Notify(repo.Key(), runID)

	// Wait until run enters Preparing and sandbox exists in Kubernetes
	var sb *sandboxv1beta1.Sandbox
	d.runUntil(func() bool {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if s, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sb = s
				return true
			}
		}
		return false
	})

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
	d.runUntil(func() bool {
		r, err := store.GetRun(ctx, repo.Key(), runID)
		return err == nil && r.Phase == state.PhaseFailed
	})

	r, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if !strings.Contains(r.Error, "Sandbox name exceeds maximum length") {
		t.Errorf("expected error to mention failure message, got: %q", r.Error)
	}
}

func TestNewScheme(t *testing.T) {
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme() failed: %v", err)
	}

	hrGVK := herorunv1alpha1.GroupVersion.WithKind("HeroRun")
	if !scheme.Recognizes(hrGVK) {
		t.Errorf("scheme does not recognize %s", hrGVK)
	}

	sbGVK := sandboxv1beta1.GroupVersion.WithKind("Sandbox")
	if !scheme.Recognizes(sbGVK) {
		t.Errorf("scheme does not recognize %s", sbGVK)
	}
}

func TestNewManager(t *testing.T) {
	restConfig := &rest.Config{Host: "http://127.0.0.1:65534"}

	mgr, err := NewManager(restConfig, "test-namespace")
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}

	mgrEmpty, err := NewManager(restConfig, "")
	if err != nil {
		t.Fatalf("NewManager() with empty namespace failed: %v", err)
	}
	if mgrEmpty == nil {
		t.Fatal("expected non-nil manager with empty namespace")
	}
}

func TestOrchestrator_SetupWithManager(t *testing.T) {
	restConfig := &rest.Config{Host: "http://127.0.0.1:65534"}
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	skipNameValidation := true
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Logger: klog.NewKlogr(),
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Cache: crcache.Options{
			DefaultNamespaces: map[string]crcache.Config{
				"ao-sandboxes": {},
			},
		},
		Controller: crconfig.Controller{
			SkipNameValidation: &skipNameValidation,
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: mgr.GetClient(),
	})

	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager failed: %v", err)
	}
}

func TestHeroRunGenerationChangedPredicate(t *testing.T) {
	pred := predicate.GenerationChangedPredicate{}

	oldRun := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "run-1",
			Generation: 1,
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	// Status-only update: generation unchanged, status differs.
	statusOnlyUpdate := oldRun.DeepCopy()
	statusOnlyUpdate.Status.Phase = string(state.PhaseWorking)
	if pred.Update(event.UpdateEvent{ObjectOld: oldRun, ObjectNew: statusOnlyUpdate}) {
		t.Error("expected status-only update event to be filtered, but predicate returned true")
	}

	// Generation change: spec update increments generation.
	genChangedUpdate := oldRun.DeepCopy()
	genChangedUpdate.Generation = 2
	genChangedUpdate.Status.Phase = string(state.PhaseWorking)
	if !pred.Update(event.UpdateEvent{ObjectOld: oldRun, ObjectNew: genChangedUpdate}) {
		t.Error("expected generation change update event to pass, but predicate returned false")
	}

	// Creates and deletes still pass.
	if !pred.Create(event.CreateEvent{Object: oldRun}) {
		t.Error("expected create event to pass")
	}
	if !pred.Delete(event.DeleteEvent{Object: oldRun}) {
		t.Error("expected delete event to pass")
	}
}

func TestRunForRequest_ResolvesThroughIndex(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
	})

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
	runID := "20261030T120000Z-index"
	orch.Notify(repoKey, runID)

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      sandboxNameForRun(runID),
		},
	}
	gotRepo, gotRunID, ok, err := orch.runForRequest(ctx, req)
	if err != nil || !ok {
		t.Fatalf("expected runForRequest to resolve through index, got ok=%v err=%v", ok, err)
	}
	if gotRepo != repoKey {
		t.Errorf("got repo %+v, want %+v", gotRepo, repoKey)
	}
	if gotRunID != runID {
		t.Errorf("got runID %q, want %q", gotRunID, runID)
	}
}

func TestRunForRequest_ResolvesThroughHeroRunOnIndexMiss(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
	runID := "20261030T120000Z-hrlookup"
	hrName := sandboxNameForRun(runID)

	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			Labels: map[string]string{
				LabelRun: runID,
			},
			Annotations: map[string]string{
				AnnotationRepo: repoKey.String(),
			},
		},
	}
	if err := fakeClient.Create(ctx, hr); err != nil {
		t.Fatalf("creating HeroRun: %v", err)
	}

	// Verify index does NOT have it before request
	orch.runIndexMu.RLock()
	_, foundBefore := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if foundBefore {
		t.Fatal("expected index to miss before runForRequest")
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      hrName,
		},
	}
	gotRepo, gotRunID, ok, err := orch.runForRequest(ctx, req)
	if err != nil || !ok {
		t.Fatalf("expected runForRequest to resolve through HeroRun on index miss, got ok=%v err=%v", ok, err)
	}
	if gotRepo != repoKey {
		t.Errorf("got repo %+v, want %+v", gotRepo, repoKey)
	}
	if gotRunID != runID {
		t.Errorf("got runID %q, want %q", gotRunID, runID)
	}

	// Verify index is now populated
	orch.runIndexMu.RLock()
	cached, foundAfter := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if !foundAfter {
		t.Fatal("expected index to be populated after runForRequest miss")
	}
	if cached.repo != repoKey || cached.runID != runID {
		t.Errorf("cached entry %+v, want repo %+v runID %s", cached, repoKey, runID)
	}
}

func TestOrchestrator_Reconcile_UnknownNameDroppedWithoutError(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	for _, c := range []client.Client{nil, fakeClient} {
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:      store,
			Harness:    harness,
			KubeClient: c,
		})

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: harness.Namespace(),
				Name:      "unknown-hero-run-name",
			},
		}
		res, err := orch.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("expected unknown request to be dropped without error, got: %v", err)
		}
		if res != (reconcile.Result{}) {
			t.Errorf("expected empty reconcile.Result, got: %+v", res)
		}
	}
}

func TestOrchestrator_Reconcile_ReadErrorReturnsError(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return errors.New("simulated API server read error")
		},
	}).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      "some-run",
		},
	}
	_, err := orch.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected Reconcile to return error on API read failure")
	}
	if !strings.Contains(err.Error(), "simulated API server read error") {
		t.Errorf("expected error to mention 'simulated API server read error', got: %v", err)
	}
}

func TestOrchestrator_FinishedRunHasNoIndexEntry(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
	repo := &state.Repo{URL: "https://github.com/gke-labs/test-repo", Host: repoKey.Host, Owner: repoKey.Owner, Name: repoKey.Name}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}
	runID := "20261030T120000Z-finished"
	hrName := sandboxNameForRun(runID)
	orch.Notify(repoKey, runID)

	run := &state.Run{ID: runID, Repo: repoKey, Phase: state.PhaseWorking}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}
	_ = orch.publishHeroRun(ctx, repo, run)
	orch.finishRun(ctx, repo, run, runEnd{Phase: state.PhaseNoImprovement, Kind: "finished"})

	orch.runIndexMu.RLock()
	_, found := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if found {
		t.Errorf("expected finished run %s to have no index entry", runID)
	}

	// Late event resolves through HeroRun and leaves no index entry.
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: harness.Namespace(), Name: hrName}}
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile on terminal run: %v", err)
	}
	orch.runIndexMu.RLock()
	_, foundLate := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if foundLate {
		t.Errorf("expected late event for terminal run not to leave index entry")
	}
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		Clock:      func() time.Time { return fixedTime },
		KubeClient: fakeClient,
	})

	// Reconcile Pending run -> leaves Pending to Preparing
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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

	// Run 0: non-terminal (Pending), no HeroRun exists -> Resume publishes for Pending runs too
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("orch.Resume: %v", err)
	}

	// Check runPending: Pending run has HeroRun created during Resume
	var hrPending herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: "run-20261030t120000z-runpending"}, &hrPending); err != nil {
		t.Fatalf("getting hrPending: %v", err)
	}
	if hrPending.Spec.Repository != repo.URL || hrPending.Spec.Goal != "goal-pending" || hrPending.Spec.RunID != runPending.ID {
		t.Errorf("hrPending spec mismatch: %+v", hrPending.Spec)
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: nil,
	})

	// Run leaves Pending in prepareRun with nil KubeClient
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: nil,
	})

	// Step 1: Reconcile Pending -> Preparing
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store: store, SessionStore: session.NewFileStore(t.TempDir()), Harness: harness, Clock: func() time.Time { return fixedTime }, MaxTurns: 30,
	})
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

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), run.ID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor:     nil, // explicitly nil
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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

			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		PublicURL:    "https://ao.example.com",
		Clock:        func() time.Time { return fixedTime },
		ForgeFor:     forgeFor,
	})
	return orch, store, repo, run
}

func TestOrchestrator_OpeningPR_OpensPullRequest_Succeeds(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return fakeForge, nil
		},
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})

	// 1. Reconcile run1 (Working). This starts the turn worker, which runs in a goroutine.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo1.Key(), run1ID)); err != nil {
		t.Fatalf("orch.Reconcile run1: %v", err)
	}

	// Wait for run1's worker to enter the blocking tool call.
	select {
	case <-toolEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run1 to enter blocking tool call")
	}

	// 2. While run1's tool call is blocked, reconcile run2 from Preparing to Working.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo2.Key(), run2ID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo1.Key(), run1ID)); err != nil {
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

		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
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
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err == nil {
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
	})

	// Pre-store an outcome for the run
	orch.storeTurnOutcome(runID, turnOutcome{
		Err:      nil,
		Finished: fixedTime,
	})

	// Pass 1: Reconcile consumes the outcome and moves run to NoImprovement
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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

func TestOrchestrator_CancelWorkers(t *testing.T) {
	orch := &Orchestrator{
		workerCancels: make(map[string]context.CancelFunc),
	}

	var canc1, canc2 bool
	orch.setWorkerCancel("run-1", func() { canc1 = true })
	orch.setWorkerCancel("run-2", func() { canc2 = true })
	orch.cancelWorkers()

	if !canc1 || !canc2 {
		t.Errorf("expected both workers to be cancelled, got canc1=%v canc2=%v", canc1, canc2)
	}
}

func TestNewTestOrchestrator_WaitsForWorkers(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	var workerExited bool
	t.Run("subtest with worker", func(subT *testing.T) {
		orch := newTestOrchestrator(subT, OrchestratorOptions{
			Store:   store,
			Harness: harness,
		})

		ctx, cancel := context.WithCancel(subT.Context())
		orch.setWorkerCancel("test-run", cancel)
		orch.workerWG.Add(1)
		go func() {
			defer orch.workerWG.Done()
			<-ctx.Done()
			workerExited = true
		}()
	})

	if !workerExited {
		t.Errorf("expected worker goroutine to have exited before subtest cleanup completed")
	}
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
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})

		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})

		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})

		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        getClock,
		MaxTurns:     30,
	})

	// 1. First reconcile pass starts the turn worker. RequeueAfter should be 0.
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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
	res1, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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
	res2, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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

		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err == nil {
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

		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err == nil {
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

func TestOrchestratorOptions_Defaults(t *testing.T) {
	var opts OrchestratorOptions
	opts.InitDefaults()

	if opts.Clock == nil {
		t.Fatal("expected Clock to be defaulted, got nil")
	}
	if time.Since(opts.Clock()) > time.Minute {
		t.Errorf("expected Clock to return current time, got %v", opts.Clock())
	}
	if opts.MaxTurns != DefaultMaxTurns {
		t.Errorf("MaxTurns = %d, want %d", opts.MaxTurns, DefaultMaxTurns)
	}
	if opts.MaxConcurrentRuns != DefaultMaxConcurrentRuns {
		t.Errorf("MaxConcurrentRuns = %d, want %d", opts.MaxConcurrentRuns, DefaultMaxConcurrentRuns)
	}
	if opts.ReadyTimeout != DefaultReadyTimeout {
		t.Errorf("ReadyTimeout = %v, want %v", opts.ReadyTimeout, DefaultReadyTimeout)
	}
	if opts.HeroRunStatusInterval != DefaultHeroRunStatusInterval {
		t.Errorf("HeroRunStatusInterval = %v, want %v", opts.HeroRunStatusInterval, DefaultHeroRunStatusInterval)
	}
	if opts.VerifyTimeout != DefaultVerifyTimeout {
		t.Errorf("VerifyTimeout = %v, want %v", opts.VerifyTimeout, DefaultVerifyTimeout)
	}
	if opts.DefaultMaxDuration != DefaultMaxDuration {
		t.Errorf("DefaultMaxDuration = %v, want %v", opts.DefaultMaxDuration, DefaultMaxDuration)
	}
	if opts.WorkspaceSize != DefaultWorkspaceSize {
		t.Errorf("WorkspaceSize = %q, want %q", opts.WorkspaceSize, DefaultWorkspaceSize)
	}
	if opts.WorkspaceStorageClass != "" {
		t.Errorf("WorkspaceStorageClass = %q, want empty", opts.WorkspaceStorageClass)
	}

	// Verify custom non-zero fields are not overwritten
	customTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	customOpts := OrchestratorOptions{
		Clock:                 func() time.Time { return customTime },
		MaxTurns:              15,
		MaxConcurrentRuns:     2,
		ReadyTimeout:          10 * time.Second,
		HeroRunStatusInterval: 5 * time.Second,
		VerifyTimeout:         2 * time.Minute,
		DefaultMaxDuration:    20 * time.Minute,
		WorkspaceStorageClass: "custom-sc",
		WorkspaceSize:         "25Gi",
	}
	customOpts.InitDefaults()
	if !customOpts.Clock().Equal(customTime) {
		t.Errorf("custom Clock was overwritten")
	}
	if customOpts.MaxTurns != 15 {
		t.Errorf("custom MaxTurns was overwritten: %d", customOpts.MaxTurns)
	}
	if customOpts.MaxConcurrentRuns != 2 {
		t.Errorf("custom MaxConcurrentRuns was overwritten: %d", customOpts.MaxConcurrentRuns)
	}
	if customOpts.ReadyTimeout != 10*time.Second {
		t.Errorf("custom ReadyTimeout was overwritten: %v", customOpts.ReadyTimeout)
	}
	if customOpts.HeroRunStatusInterval != 5*time.Second {
		t.Errorf("custom HeroRunStatusInterval was overwritten: %v", customOpts.HeroRunStatusInterval)
	}
	if customOpts.VerifyTimeout != 2*time.Minute {
		t.Errorf("custom VerifyTimeout was overwritten: %v", customOpts.VerifyTimeout)
	}
	if customOpts.DefaultMaxDuration != 20*time.Minute {
		t.Errorf("custom DefaultMaxDuration was overwritten: %v", customOpts.DefaultMaxDuration)
	}
	if customOpts.WorkspaceStorageClass != "custom-sc" {
		t.Errorf("custom WorkspaceStorageClass was overwritten: %q", customOpts.WorkspaceStorageClass)
	}
	if customOpts.WorkspaceSize != "25Gi" {
		t.Errorf("custom WorkspaceSize was overwritten: %q", customOpts.WorkspaceSize)
	}
}

func TestOrchestratorOptions_AddFlags(t *testing.T) {
	var opts OrchestratorOptions
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts.AddFlags(fs)

	if err := fs.Parse([]string{
		"--max-turns", "25",
		"--max-concurrent-runs", "3",
		"--public-url", "https://flag.example.com",
		"--ready-timeout", "2m",
		"--verify-timeout", "5m",
		"--max-run-duration", "30m",
		"--workspace-storage-class", "fast-ssd",
		"--workspace-size", "50Gi",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if opts.MaxTurns != 25 {
		t.Errorf("MaxTurns = %d, want 25", opts.MaxTurns)
	}
	if opts.MaxConcurrentRuns != 3 {
		t.Errorf("MaxConcurrentRuns = %d, want 3", opts.MaxConcurrentRuns)
	}
	if opts.PublicURL != "https://flag.example.com" {
		t.Errorf("PublicURL = %q, want 'https://flag.example.com'", opts.PublicURL)
	}
	if opts.ReadyTimeout != 2*time.Minute {
		t.Errorf("ReadyTimeout = %v, want 2m", opts.ReadyTimeout)
	}
	if opts.VerifyTimeout != 5*time.Minute {
		t.Errorf("VerifyTimeout = %v, want 5m", opts.VerifyTimeout)
	}
	if opts.DefaultMaxDuration != 30*time.Minute {
		t.Errorf("DefaultMaxDuration = %v, want 30m", opts.DefaultMaxDuration)
	}
	if opts.WorkspaceStorageClass != "fast-ssd" {
		t.Errorf("WorkspaceStorageClass = %q, want 'fast-ssd'", opts.WorkspaceStorageClass)
	}
	if opts.WorkspaceSize != "50Gi" {
		t.Errorf("WorkspaceSize = %q, want '50Gi'", opts.WorkspaceSize)
	}
}

func TestOrchestratorOptions_Validate(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	baseValid := func() OrchestratorOptions {
		opts := OrchestratorOptions{
			Store:   store,
			Harness: harness,
		}
		opts.InitDefaults()
		return opts
	}

	base := baseValid()
	if err := base.Validate(); err != nil {
		t.Fatalf("expected valid options to pass, got: %v", err)
	}

	tests := []struct {
		name          string
		mutate        func(o *OrchestratorOptions)
		wantErrSubstr string
	}{
		{
			name: "missing store",
			mutate: func(o *OrchestratorOptions) {
				o.Store = nil
			},
			wantErrSubstr: "store",
		},
		{
			name: "missing harness",
			mutate: func(o *OrchestratorOptions) {
				o.Harness = nil
			},
			wantErrSubstr: "harness",
		},
		{
			name: "negative MaxTurns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxTurns = -1
			},
			wantErrSubstr: "MaxTurns",
		},
		{
			name: "zero MaxTurns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxTurns = 0
			},
			wantErrSubstr: "MaxTurns",
		},
		{
			name: "negative MaxConcurrentRuns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxConcurrentRuns = -1
			},
			wantErrSubstr: "MaxConcurrentRuns",
		},
		{
			name: "zero MaxConcurrentRuns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxConcurrentRuns = 0
			},
			wantErrSubstr: "MaxConcurrentRuns",
		},
		{
			name: "negative ReadyTimeout",
			mutate: func(o *OrchestratorOptions) {
				o.ReadyTimeout = -1 * time.Second
			},
			wantErrSubstr: "ReadyTimeout",
		},
		{
			name: "negative HeroRunStatusInterval",
			mutate: func(o *OrchestratorOptions) {
				o.HeroRunStatusInterval = -1 * time.Second
			},
			wantErrSubstr: "HeroRunStatusInterval",
		},
		{
			name: "negative VerifyTimeout",
			mutate: func(o *OrchestratorOptions) {
				o.VerifyTimeout = -1 * time.Second
			},
			wantErrSubstr: "VerifyTimeout",
		},
		{
			name: "negative DefaultMaxDuration",
			mutate: func(o *OrchestratorOptions) {
				o.DefaultMaxDuration = -1 * time.Second
			},
			wantErrSubstr: "DefaultMaxDuration",
		},
		{
			name: "portal binder without policy factory",
			mutate: func(o *OrchestratorOptions) {
				o.PortalBinder = &fakePortalBinder{}
				o.PolicyFactory = nil
			},
			wantErrSubstr: "policy factory",
		},
		{
			name: "invalid WorkspaceSize",
			mutate: func(o *OrchestratorOptions) {
				o.WorkspaceSize = "not-a-valid-size"
			},
			wantErrSubstr: "WorkspaceSize",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseValid()
			tc.mutate(&opts)
			err := opts.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSubstr)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Errorf("error %q does not contain expected substring %q", err.Error(), tc.wantErrSubstr)
			}
		})
	}

	t.Run("NewOrchestrator propagates validation error", func(t *testing.T) {
		_, err := NewOrchestrator(OrchestratorOptions{
			Store:        store,
			Harness:      harness,
			ReadyTimeout: -1 * time.Minute,
		})
		if err == nil || !strings.Contains(err.Error(), "ReadyTimeout") {
			t.Fatalf("expected ReadyTimeout error from NewOrchestrator, got: %v", err)
		}
	})

	t.Run("valid with portal binder and policy factory", func(t *testing.T) {
		opts := baseValid()
		opts.PortalBinder = &fakePortalBinder{}
		opts.PolicyFactory = portal.HostListPolicyFactory(nil)
		if err := opts.Validate(); err != nil {
			t.Fatalf("expected valid options to pass, got: %v", err)
		}
	})
}

type midTurnBlockingLLM struct {
	started   chan struct{}
	release   chan struct{}
	cancelled chan struct{}
	once      sync.Once
	cancOnce  sync.Once
}

func newMidTurnBlockingLLM() *midTurnBlockingLLM {
	return &midTurnBlockingLLM{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
}

func (m *midTurnBlockingLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	m.once.Do(func() { close(m.started) })
	select {
	case <-ctx.Done():
		m.cancOnce.Do(func() { close(m.cancelled) })
		return nil, ctx.Err()
	case <-m.release:
		done := "done"
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{{Message: llm.Message{Role: "assistant", Content: &done}}},
		}, nil
	}
}

func setupWorkingTestRun(t *testing.T, runID, sbName string, deadline *time.Time, llmClient llm.ChatClient) (*state.FileStore, *session.FileStore, *state.Repo, *state.Run, *session.Harness, *fakeagents.Clientset) {
	t.Helper()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())
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
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "test-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha",
		SandboxName: sbName,
		Deadline:    deadline,
		Goal:        &goals.Goal{Name: "test-goal", Body: "body"},
	}
	if err := store.PutRun(t.Context(), run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, llmClient)
	policy := sandboxv1beta1.ShutdownPolicyDelete
	shutdownTime := time.Now().Add(time.Hour)
	if deadline != nil {
		shutdownTime = deadline.Add(15 * time.Minute)
	}
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(t.Context(), &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: "ao-sandboxes", Labels: map[string]string{LabelRun: runID}},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: shutdownTime}, ShutdownPolicy: &policy},
		},
		Status: sandboxv1beta1.SandboxStatus{Conditions: []metav1.Condition{{Type: string(sandboxv1beta1.SandboxConditionReady), Status: metav1.ConditionTrue}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}
	return store, sessionStore, repo, run, harness, agentsCS
}

func TestOrchestrator_Working_DeadlineExceededMidTurn(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261030T120000Z-deadline", "run-deadline"
	startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	deadline := startTime.Add(10 * time.Minute)

	blockingLLM := newMidTurnBlockingLLM()
	store, sessionStore, repo, run, harness, agentsCS := setupWorkingTestRun(t, runID, sbName, &deadline, blockingLLM)
	run.SetPhase(state.PhaseWorking, startTime)
	_ = store.PutRun(ctx, run)

	currentTime := startTime
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store: store, SessionStore: sessionStore, Harness: harness,
		Clock: func() time.Time { return currentTime },
	})

	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil || res.RequeueAfter != 10*time.Minute {
		t.Fatalf("Reconcile got res=%v err=%v, want RequeueAfter=10m", res, err)
	}

	select {
	case <-blockingLLM.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker to start LLM call")
	}

	currentTime = deadline.Add(time.Minute)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile after deadline: %v", err)
	}

	select {
	case <-blockingLLM.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("expected worker context to be cancelled")
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil || gotRun.Phase != state.PhaseFailed {
		t.Fatalf("expected phase Failed, got %v (err %v)", gotRun.Phase, err)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox to be deleted, got err: %v", err)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "deadline_exceeded" {
			foundEvent = true
			m, ok := e.Data.(map[string]any)
			if !ok || m["deadline"] != deadline.Format(time.RFC3339) || m["elapsed"] == "" {
				t.Errorf("expected deadline and elapsed in data, got: %+v", e.Data)
			}
			break
		}
	}
	if !foundEvent {
		t.Errorf("expected deadline_exceeded event, got events: %+v", events)
	}

	// Later outcome from finished worker is ignored
	orch.workerWG.Wait()
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile with leftover outcome: %v", err)
	}
	gotRunAfter, _ := store.GetRun(ctx, repo.Key(), runID)
	if gotRunAfter.Phase != state.PhaseFailed {
		t.Errorf("expected phase to remain Failed, got %s", gotRunAfter.Phase)
	}
}

func TestOrchestrator_Working_SandboxDeletedMidTurn(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(ctx context.Context, cs *fakeagents.Clientset, sbName string) error
		wantMsg string
	}{
		{
			name: "deleted",
			mutate: func(ctx context.Context, cs *fakeagents.Clientset, sbName string) error {
				return cs.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, sbName, metav1.DeleteOptions{})
			},
			wantMsg: "deleted",
		},
		{
			name: "failed condition",
			mutate: func(ctx context.Context, cs *fakeagents.Clientset, sbName string) error {
				sbObj, err := cs.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				sbObj.Status.Conditions = append(sbObj.Status.Conditions, metav1.Condition{
					Type:    string(sandboxv1beta1.SandboxConditionFinished),
					Status:  metav1.ConditionTrue,
					Reason:  sandboxv1beta1.SandboxReasonPodFailed,
					Message: "pod terminated with error",
				})
				_, err = cs.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sbObj, metav1.UpdateOptions{})
				return err
			},
			wantMsg: "pod terminated with error",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			runID, sbName := "20261030T120000Z-"+tc.name, "run-"+tc.name
			startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
			deadline := startTime.Add(30 * time.Minute)

			blockingLLM := newMidTurnBlockingLLM()
			store, sessionStore, repo, _, harness, agentsCS := setupWorkingTestRun(t, runID, sbName, &deadline, blockingLLM)

			orch := newTestOrchestrator(t, OrchestratorOptions{
				Store: store, SessionStore: sessionStore, Harness: harness,
				Clock: func() time.Time { return startTime },
			})

			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			select {
			case <-blockingLLM.started:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for worker to start LLM call")
			}

			if err := tc.mutate(ctx, agentsCS, sbName); err != nil {
				t.Fatalf("mutating sandbox: %v", err)
			}

			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
				t.Fatalf("Reconcile after sandbox change: %v", err)
			}

			select {
			case <-blockingLLM.cancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("expected worker context to be cancelled")
			}

			gotRun, err := store.GetRun(ctx, repo.Key(), runID)
			if err != nil || gotRun.Phase != state.PhaseFailed {
				t.Fatalf("expected phase Failed, got %v (err %v)", gotRun.Phase, err)
			}

			events, err := store.ReadEvents(ctx, repo.Key(), runID)
			if err != nil {
				t.Fatalf("ReadEvents: %v", err)
			}
			var foundEvent bool
			for _, e := range events {
				if e.Phase == state.PhaseFailed && e.Kind == "sandbox_deleted" && strings.Contains(e.Message, tc.wantMsg) {
					foundEvent = true
					break
				}
			}
			if !foundEvent {
				t.Errorf("expected sandbox_deleted event mentioning %q, got events: %+v", tc.wantMsg, events)
			}
		})
	}
}

func TestOrchestrator_Working_RequeueRemainingDeadline(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261030T120000Z-requeue", "run-requeue"
	startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	deadline := startTime.Add(25 * time.Minute)

	blockingLLM := newMidTurnBlockingLLM()
	store, sessionStore, repo, _, harness, _ := setupWorkingTestRun(t, runID, sbName, &deadline, blockingLLM)

	currentTime := startTime
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store: store, SessionStore: sessionStore, Harness: harness,
		Clock: func() time.Time { return currentTime },
	})

	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil || res.RequeueAfter != 25*time.Minute {
		t.Fatalf("Reconcile got res=%v err=%v, want RequeueAfter=25m", res, err)
	}

	select {
	case <-blockingLLM.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker to start")
	}

	// Reconcile while active should return remaining time
	currentTime = startTime.Add(5 * time.Minute)
	res2, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil || res2.RequeueAfter != 20*time.Minute {
		t.Fatalf("Reconcile active got res2=%v err=%v, want RequeueAfter=20m", res2, err)
	}
}

func TestOrchestrator_Working_PendingOutcomeConsumedBeforeDeadline(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261030T120000Z-outcome-first", "run-outcome-first"
	startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	deadline := startTime.Add(10 * time.Minute)

	store, sessionStore, repo, _, harness, _ := setupWorkingTestRun(t, runID, sbName, &deadline, &llm.ElizaClient{})

	currentTime := startTime
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store: store, SessionStore: sessionStore, Harness: harness,
		Clock: func() time.Time { return currentTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Store an outcome that finished before the deadline
	orch.storeTurnOutcome(runID, turnOutcome{
		Err:      session.ErrSessionFinished,
		Finished: startTime.Add(5 * time.Minute),
	})

	// Advance clock past the deadline before Reconcile runs
	currentTime = deadline.Add(5 * time.Minute)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s (from finished outcome), got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for _, e := range events {
		if e.Kind == "deadline_exceeded" {
			t.Errorf("did not expect deadline_exceeded event when turn outcome was waiting")
		}
	}
}

type fakePortalBinder struct {
	mu       sync.Mutex
	calls    []portalBindCall
	bindFunc func(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error
}

type portalBindCall struct {
	PodUID types.UID
	Policy *portalsv1.EgressPolicy
}

func (f *fakePortalBinder) Bind(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, portalBindCall{PodUID: podUID, Policy: policy})
	if f.bindFunc != nil {
		return f.bindFunc(ctx, podUID, policy)
	}
	return nil
}

func TestOrchestrator_PortalBinder_BindBeforeCommands(t *testing.T) {
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

	runID := "20261030T120000Z-bind1"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "success",
	}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	binder := &fakePortalBinder{}
	binder.bindFunc = func(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error {
		procSrv.mu.Lock()
		defer procSrv.mu.Unlock()
		if len(procSrv.commands) > 0 {
			t.Errorf("expected 0 commands executed before bind, got %d", len(procSrv.commands))
		}
		return nil
	}

	fixedTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	egressHosts := []string{"github.com", "golang.org"}
	var factoryCalls []portal.PolicyRequest
	policyFactory := func(ctx context.Context, req portal.PolicyRequest) (*portalsv1.EgressPolicy, error) {
		factoryCalls = append(factoryCalls, req)
		return portal.HostListPolicyFactory(egressHosts)(ctx, req)
	}
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		Clock:         func() time.Time { return fixedTime },
		PortalBinder:  binder,
		PolicyFactory: policyFactory,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile Pending -> Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Verify sandbox pod template has the interception label
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	podLabels := sb.Spec.SandboxBlueprint.PodTemplate.ObjectMeta.Labels
	if podLabels[session.PortalInterceptLabel] != "true" {
		t.Errorf("expected sandbox template label %s=true, got %+v", session.PortalInterceptLabel, podLabels)
	}

	// Ensure no bind was called yet
	binder.mu.Lock()
	callsBeforeReady := len(binder.calls)
	binder.mu.Unlock()
	if callsBeforeReady != 0 {
		t.Fatalf("expected 0 bind calls before ready, got %d", callsBeforeReady)
	}

	// Create backing pod in fake k8s
	podUID := types.UID("pod-uid-test-12345")
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       podUID,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Reconcile Preparing -> Working (binds, then clones and checkouts)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun after step 2: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}
	if gotRun.PortalBoundAt == nil {
		t.Errorf("expected PortalBoundAt to be set on run")
	}

	if len(factoryCalls) != 1 {
		t.Fatalf("expected 1 factory call, got %d", len(factoryCalls))
	}
	if factoryCalls[0].RepoURL != repo.URL {
		t.Errorf("factory req RepoURL = %q, want %q", factoryCalls[0].RepoURL, repo.URL)
	}
	if factoryCalls[0].GoalName != run.GoalName {
		t.Errorf("factory req GoalName = %q, want %q", factoryCalls[0].GoalName, run.GoalName)
	}
	if factoryCalls[0].RunID != run.ID {
		t.Errorf("factory req RunID = %q, want %q", factoryCalls[0].RunID, run.ID)
	}
	if factoryCalls[0].SandboxName != gotRun.SandboxName {
		t.Errorf("factory req SandboxName = %q, want %q", factoryCalls[0].SandboxName, gotRun.SandboxName)
	}

	binder.mu.Lock()
	numCalls := len(binder.calls)
	var call portalBindCall
	if numCalls > 0 {
		call = binder.calls[0]
	}
	binder.mu.Unlock()

	if numCalls != 1 {
		t.Fatalf("expected exactly 1 bind call, got %d", numCalls)
	}
	if call.PodUID != podUID {
		t.Errorf("expected pod UID %q, got %q", podUID, call.PodUID)
	}
	wantPolicy, err := portal.PolicyForHosts(egressHosts)
	if err != nil {
		t.Fatalf("PolicyForHosts: %v", err)
	}
	if call.Policy == nil || len(call.Policy.Rules) != len(wantPolicy.Rules) || call.Policy.Rules[0].Https == nil {
		t.Fatalf("expected policy matching %+v, got %+v", wantPolicy, call.Policy)
	}
	if !slices.Equal(call.Policy.Rules[0].Https.Hostnames, wantPolicy.Rules[0].Https.Hostnames) {
		t.Errorf("hostnames = %v, want %v", call.Policy.Rules[0].Https.Hostnames, wantPolicy.Rules[0].Https.Hostnames)
	}

	procSrv.mu.Lock()
	cmds := len(procSrv.commands)
	procSrv.mu.Unlock()
	if cmds != 3 {
		t.Errorf("expected 3 commands executed after bind, got %d", cmds)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundPortalBound bool
	for _, e := range events {
		if e.Kind == "portal_bound" {
			foundPortalBound = true
			if m, ok := e.Data.(map[string]any); ok {
				if hosts, ok := m["hosts"].([]any); ok {
					var gotHosts []string
					for _, h := range hosts {
						if s, ok := h.(string); ok {
							gotHosts = append(gotHosts, s)
						}
					}
					if !slices.Equal(gotHosts, egressHosts) {
						t.Errorf("portal_bound hosts = %v, want %v", gotHosts, egressHosts)
					}
				} else if gotHosts, ok := m["hosts"].([]string); ok {
					if !slices.Equal(gotHosts, egressHosts) {
						t.Errorf("portal_bound hosts = %v, want %v", gotHosts, egressHosts)
					}
				}
			}
		}
	}
	if !foundPortalBound {
		t.Errorf("expected portal_bound event, got events: %+v", events)
	}
}

func TestOrchestrator_PortalBinder_FailsAfterDeadline(t *testing.T) {
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

	runID := "20261030T120000Z-bindfail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	bindErr := errors.New("portal controller unreachable")
	binder := &fakePortalBinder{
		bindFunc: func(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error {
			return bindErr
		},
	}

	clockMu := sync.Mutex{}
	currentTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	getClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return currentTime
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		Clock:         getClock,
		PortalBinder:  binder,
		PolicyFactory: portal.HostListPolicyFactory(nil),
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Pending -> Preparing (sets ReadyDeadline)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline set")
	}

	// Create backing pod in fake k8s
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       types.UID("pod-uid-fail"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
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

	// Reconcile pass 1: before deadline, bind fails -> should requeue for retry
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("expected Reconcile to handle retry cleanly, got err: %v", err)
	}
	if res.RequeueAfter != portalBindRetryInterval {
		t.Errorf("expected RequeueAfter == %v for retry, got %v", portalBindRetryInterval, res.RequeueAfter)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s on retry, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Advance clock past ReadyDeadline
	clockMu.Lock()
	currentTime = gotRun.ReadyDeadline.Add(time.Minute)
	clockMu.Unlock()

	// Reconcile pass 2: after deadline, bind fails -> should fail the run with portal_bind_failed
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
		t.Logf("Reconcile returned nil error (as terminal failures are logged)")
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s after deadline, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "portal controller unreachable") {
		t.Errorf("expected error to mention 'portal controller unreachable', got %q", gotRun.Error)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundBindFailed bool
	for _, e := range events {
		if e.Kind == "portal_bind_failed" {
			foundBindFailed = true
			if !strings.Contains(e.Error, "portal controller unreachable") {
				t.Errorf("expected portal_bind_failed error %q to contain 'portal controller unreachable'", e.Error)
			}
		}
	}
	if !foundBindFailed {
		t.Errorf("expected portal_bind_failed event, got events: %+v", events)
	}

	// Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox to be deleted, got err: %v", err)
	}
}

func TestOrchestrator_PortalBinder_FactoryErrorFailsAfterDeadline(t *testing.T) {
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

	runID := "20261030T120000Z-factoryfail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	binder := &fakePortalBinder{}
	factoryErr := errors.New("egress policy generation failed")
	policyFactory := func(ctx context.Context, req portal.PolicyRequest) (*portalsv1.EgressPolicy, error) {
		return nil, factoryErr
	}

	clockMu := sync.Mutex{}
	currentTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	getClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return currentTime
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		Clock:         getClock,
		PortalBinder:  binder,
		PolicyFactory: policyFactory,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Pending -> Preparing (sets ReadyDeadline)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline set")
	}

	// Create backing pod in fake k8s
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       types.UID("pod-uid-factoryfail"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
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

	// Reconcile pass 1: before deadline, factory fails -> should requeue for retry
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("expected Reconcile to handle retry cleanly, got err: %v", err)
	}
	if res.RequeueAfter != portalBindRetryInterval {
		t.Errorf("expected RequeueAfter == %v for retry, got %v", portalBindRetryInterval, res.RequeueAfter)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s on retry, got %s", state.PhasePreparing, gotRun.Phase)
	}
	binder.mu.Lock()
	bindCalls := len(binder.calls)
	binder.mu.Unlock()
	if bindCalls != 0 {
		t.Errorf("expected 0 bind calls when factory fails, got %d", bindCalls)
	}

	// Advance clock past ReadyDeadline
	clockMu.Lock()
	currentTime = gotRun.ReadyDeadline.Add(time.Minute)
	clockMu.Unlock()

	// Reconcile pass 2: after deadline, factory fails -> should fail the run with portal_bind_failed
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
		t.Logf("Reconcile returned nil error (as terminal failures are logged)")
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s after deadline, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "egress policy generation failed") {
		t.Errorf("expected error to mention 'egress policy generation failed', got %q", gotRun.Error)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundBindFailed bool
	for _, e := range events {
		if e.Kind == "portal_bind_failed" {
			foundBindFailed = true
			if !strings.Contains(e.Error, "egress policy generation failed") {
				t.Errorf("expected portal_bind_failed error %q to contain 'egress policy generation failed'", e.Error)
			}
		}
	}
	if !foundBindFailed {
		t.Errorf("expected portal_bind_failed event, got events: %+v", events)
	}

	// Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox to be deleted, got err: %v", err)
	}
}

func TestOrchestrator_PortalBinder_DisabledWithoutBinder(t *testing.T) {
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

	runID := "20261030T120000Z-nobinder"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "success",
	}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		PortalBinder: nil, // disabled
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile Pending -> Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}

	// Verify sandbox does NOT have the interception label
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	podLabels := sb.Spec.SandboxBlueprint.PodTemplate.ObjectMeta.Labels
	if podLabels != nil && podLabels[session.PortalInterceptLabel] != "" {
		t.Errorf("expected no interception label, got %q", podLabels[session.PortalInterceptLabel])
	}

	// Create backing pod in fake k8s
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       types.UID("pod-uid-nobinder"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Reconcile Preparing -> Working
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun after step 2: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for _, e := range events {
		if e.Kind == "portal_bound" || e.Kind == "portal_bind_failed" {
			t.Errorf("unexpected portal event %q when binder is nil", e.Kind)
		}
	}
}

func TestOrchestrator_WorkspaceClaim(t *testing.T) {
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
	harness, agentsCS, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	getWorkspaceVol := func(t *testing.T, sbName string) *corev1.Volume {
		t.Helper()
		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting sandbox %s: %v", sbName, err)
		}
		for i := range sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes {
			if sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes[i].Name == "workspace" {
				return &sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes[i]
			}
		}
		t.Fatal("expected workspace volume on sandbox")
		return nil
	}

	t.Run("with StorageClass set claim exists and sandbox mounts it", func(t *testing.T) {
		runID := "20261030T120000Z-scset"
		if err := store.PutRun(ctx, &state.Run{ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: state.PhasePending}); err != nil {
			t.Fatalf("PutRun: %v", err)
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store: store, Harness: harness, KubeClient: fakeClient,
			WorkspaceStorageClass: "standard", WorkspaceSize: "20Gi",
		})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		name := sandboxNameForRun(runID)
		var claim corev1.PersistentVolumeClaim
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &claim); err != nil {
			t.Fatalf("fakeClient.Get claim %s: %v", name, err)
		}
		if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != "standard" {
			t.Errorf("expected StorageClassName 'standard', got %v", claim.Spec.StorageClassName)
		}
		if len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteOncePod {
			t.Errorf("expected AccessModes [ReadWriteOncePod], got %v", claim.Spec.AccessModes)
		}
		storageQty := claim.Spec.Resources.Requests[corev1.ResourceStorage]
		if !storageQty.Equal(resource.MustParse("20Gi")) {
			t.Errorf("expected storage size '20Gi', got %s", storageQty.String())
		}

		var hr herorunv1alpha1.HeroRun
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &hr); err != nil {
			t.Fatalf("fakeClient.Get herorun %s: %v", name, err)
		}
		if len(claim.OwnerReferences) != 1 {
			t.Fatalf("expected 1 owner reference on claim, got %d", len(claim.OwnerReferences))
		}
		owner := claim.OwnerReferences[0]
		if owner.Kind != "HeroRun" || owner.Name != hr.Name || owner.UID != hr.UID || owner.Controller == nil || !*owner.Controller {
			t.Errorf("unexpected claim owner reference: %+v", owner)
		}

		vol := getWorkspaceVol(t, name)
		if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != name || vol.EmptyDir != nil {
			t.Errorf("expected workspace volume to mount claim %s, got %+v", name, vol)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.WorkspaceClaimName != name {
			t.Errorf("expected WorkspaceClaimName %q, got %q", name, gotRun.WorkspaceClaimName)
		}
	})

	t.Run("second pass reuses existing claim", func(t *testing.T) {
		runID := "20261030T120000Z-reuse"
		if err := store.PutRun(ctx, &state.Run{ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: state.PhasePending}); err != nil {
			t.Fatalf("PutRun: %v", err)
		}
		name := sandboxNameForRun(runID)
		const existingUID = types.UID("existing-claim-uid-123")
		scName := "standard"
		existingClaim := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ao-sandboxes", UID: existingUID},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingClaim).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store: store, Harness: harness, KubeClient: fakeClient,
			WorkspaceStorageClass: "standard",
		})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		var claim corev1.PersistentVolumeClaim
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &claim); err != nil {
			t.Fatalf("fakeClient.Get: %v", err)
		}
		if claim.UID != existingUID {
			t.Errorf("expected existing claim UID %q, got %q", existingUID, claim.UID)
		}

		vol := getWorkspaceVol(t, name)
		if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != name {
			t.Errorf("expected workspace volume to mount claim %s, got %+v", name, vol)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.WorkspaceClaimName != name {
			t.Errorf("expected WorkspaceClaimName %q, got %q", name, gotRun.WorkspaceClaimName)
		}
	})

	t.Run("with no StorageClass no claim is created and sandbox uses emptyDir", func(t *testing.T) {
		runID := "20261030T120000Z-noclaim"
		if err := store.PutRun(ctx, &state.Run{ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: state.PhasePending}); err != nil {
			t.Fatalf("PutRun: %v", err)
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store: store, Harness: harness, KubeClient: fakeClient,
		})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		var claimList corev1.PersistentVolumeClaimList
		if err := fakeClient.List(ctx, &claimList, client.InNamespace("ao-sandboxes")); err != nil {
			t.Fatalf("fakeClient.List: %v", err)
		}
		if len(claimList.Items) != 0 {
			t.Errorf("expected 0 claims, got %d", len(claimList.Items))
		}

		name := sandboxNameForRun(runID)
		vol := getWorkspaceVol(t, name)
		if vol.EmptyDir == nil || vol.PersistentVolumeClaim != nil {
			t.Errorf("expected emptyDir volume for workspace, got %+v", vol)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.WorkspaceClaimName != "" {
			t.Errorf("expected empty WorkspaceClaimName, got %q", gotRun.WorkspaceClaimName)
		}
	})

	t.Run("with StorageClass set fails when HeroRun owner is unavailable", func(t *testing.T) {
		runID := "20261030T120000Z-noowner"
		if err := store.PutRun(ctx, &state.Run{ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: state.PhasePending}); err != nil {
			t.Fatalf("PutRun: %v", err)
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*herorunv1alpha1.HeroRun); ok {
					return errors.New("simulated HeroRun creation failure")
				}
				return client.Create(ctx, obj, opts...)
			},
		}).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:                 store,
			Harness:               harness,
			KubeClient:            fakeClient,
			WorkspaceStorageClass: "standard",
		})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		gotRun, err := store.GetRun(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
		}
		if !strings.Contains(gotRun.Error, "HeroRun owner") {
			t.Errorf("expected run.Error containing 'HeroRun owner', got %q", gotRun.Error)
		}

		name := sandboxNameForRun(runID)
		var claim corev1.PersistentVolumeClaim
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &claim); !k8serrors.IsNotFound(err) {
			t.Errorf("expected claim not found, got %v", err)
		}
	})
}
