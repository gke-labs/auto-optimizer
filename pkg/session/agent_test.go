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

package session

import (
	"context"
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
)

// newTestSession returns a Session backed by a scratch backup directory.
// getBackupDir() resolves backups under os.UserHomeDir()/.local/sandboxed-tools/<name>/fs,
// so redirecting HOME keeps these tests off the real user's home directory.
func newTestSession(t *testing.T) *Session {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	return &Session{Name: "test-session"}
}

// writeBackup creates a fake backup tarball with the given timestamp
// component (e.g. "20260103T000000") and returns its full path.
func writeBackup(t *testing.T, session *Session, timestamp string) string {
	t.Helper()
	dir, err := session.getBackupDir()
	if err != nil {
		t.Fatalf("getBackupDir: %v", err)
	}
	path := filepath.Join(dir, "backup-"+timestamp+".tar.gz")
	if err := os.WriteFile(path, []byte("fake-tarball"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
	return path
}

func remainingBackups(t *testing.T, session *Session) []string {
	t.Helper()
	dir, err := session.getBackupDir()
	if err != nil {
		t.Fatalf("getBackupDir: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "backup-*.tar.gz"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	return matches
}

func TestFindLatestBackup_NoBackups(t *testing.T) {
	session := newTestSession(t)

	got, err := session.FindLatestBackup()
	if err != nil {
		t.Fatalf("FindLatestBackup: %v", err)
	}
	if got != "" {
		t.Errorf("FindLatestBackup() = %q, want empty string when no backups exist", got)
	}
}

func TestFindLatestBackup_ReturnsMostRecentByTimestamp(t *testing.T) {
	session := newTestSession(t)
	// Written out of chronological order -- FindLatestBackup must sort by
	// name, not by creation order or mtime.
	writeBackup(t, session, "20260101T000000")
	want := writeBackup(t, session, "20260103T000000")
	writeBackup(t, session, "20260102T000000")

	got, err := session.FindLatestBackup()
	if err != nil {
		t.Fatalf("FindLatestBackup: %v", err)
	}
	if got != want {
		t.Errorf("FindLatestBackup() = %q, want %q (the newest timestamp)", got, want)
	}
}

func TestFindLatestBackup_IgnoresNonBackupFiles(t *testing.T) {
	session := newTestSession(t)
	want := writeBackup(t, session, "20260101T000000")

	dir, err := session.getBackupDir()
	if err != nil {
		t.Fatalf("getBackupDir: %v", err)
	}
	for _, name := range []string{"notes.txt", "backup-corrupt", "backup-20260102T000000.tar.gz.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := session.FindLatestBackup()
	if err != nil {
		t.Fatalf("FindLatestBackup: %v", err)
	}
	if got != want {
		t.Errorf("FindLatestBackup() = %q, want %q (non-matching files must be ignored)", got, want)
	}
}

func TestPruneBackups_NoBackupsIsNoop(t *testing.T) {
	session := newTestSession(t)

	if err := session.PruneBackups(t.Context(), 5); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}
}

func TestPruneBackups_KeepsEverythingWhenAtOrUnderKeepCount(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000")
	writeBackup(t, session, "20260102T000000")

	if err := session.PruneBackups(t.Context(), 5); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	if got := remainingBackups(t, session); len(got) != 2 {
		t.Errorf("remaining backups = %v, want 2 (nothing pruned)", got)
	}
}

func TestPruneBackups_ExactlyAtKeepCountIsNoop(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000")
	writeBackup(t, session, "20260102T000000")
	writeBackup(t, session, "20260103T000000")

	if err := session.PruneBackups(t.Context(), 3); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	if got := remainingBackups(t, session); len(got) != 3 {
		t.Errorf("remaining backups = %v, want 3 (boundary case: len == keepCount, nothing pruned)", got)
	}
}

func TestPruneBackups_DeletesOldestKeepingNewest(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000") // oldest, must be pruned
	writeBackup(t, session, "20260102T000000") // must be pruned
	keep1 := writeBackup(t, session, "20260103T000000")
	keep2 := writeBackup(t, session, "20260104T000000")

	if err := session.PruneBackups(t.Context(), 2); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	got := remainingBackups(t, session)
	if len(got) != 2 {
		t.Fatalf("remaining backups = %v, want exactly 2", got)
	}
	remaining := map[string]bool{got[0]: true, got[1]: true}
	if !remaining[keep1] || !remaining[keep2] {
		t.Errorf("remaining backups = %v, want the two newest (%q, %q) -- pruning kept the wrong files", got, keep1, keep2)
	}
}

func TestPruneBackups_ZeroKeepCountDeletesEverything(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000")
	writeBackup(t, session, "20260102T000000")

	if err := session.PruneBackups(t.Context(), 0); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	if got := remainingBackups(t, session); len(got) != 0 {
		t.Errorf("remaining backups = %v, want 0", got)
	}
}

func TestNewAndLoadSession_Metadata(t *testing.T) {
	ctx := t.Context()
	store := NewFileStore(t.TempDir())
	harness := NewHarness(nil, nil, nil, RunOptions{Namespace: "ns", HomeDir: "/home/x"})

	if _, err := harness.LoadSession(ctx, store, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("LoadSession(missing) err = %v, want ErrNotFound", err)
	}

	// A new session persists the metadata it was built with, stamping the
	// creation time.
	before := time.Now()
	session, err := harness.NewSession(ctx, store, "child1", Metadata{Parent: "parent1", Toolset: "geminicli"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if session.Metadata.Parent != "parent1" || session.Metadata.Toolset != "geminicli" {
		t.Errorf("new session metadata = %+v, want parent1/geminicli", session.Metadata)
	}
	if session.Metadata.CreatedAt.Before(before) {
		t.Errorf("CreatedAt = %v, want >= %v", session.Metadata.CreatedAt, before)
	}

	// Reusing an existing session name in NewSession fails.
	if _, err := harness.NewSession(ctx, store, "child1", Metadata{Toolset: "basic"}); err == nil {
		t.Error("NewSession with existing session name should fail")
	}

	// Loading the session restores the stored metadata.
	resumed, err := harness.LoadSession(ctx, store, "child1")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if !resumed.Metadata.CreatedAt.Equal(session.Metadata.CreatedAt) || resumed.Metadata.Parent != "parent1" || resumed.Metadata.Toolset != "geminicli" {
		t.Errorf("resumed metadata = %+v, want the stored %+v", resumed.Metadata, session.Metadata)
	}

	// A session recorded before metadata existed loads with zero metadata
	// rather than being backfilled.
	content := "hello"
	if err := store.AppendMessages(ctx, "legacy", llm.Message{Role: "user", Content: &content}); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}
	legacy, err := harness.LoadSession(ctx, store, "legacy")
	if err != nil {
		t.Fatalf("LoadSession (legacy): %v", err)
	}
	if legacy.Metadata != (Metadata{}) {
		t.Errorf("legacy metadata = %+v, want zero", legacy.Metadata)
	}
	if _, err := harness.NewSession(ctx, store, "legacy", Metadata{Toolset: "basic"}); err == nil {
		t.Error("NewSession on existing legacy session should fail")
	}
}

func TestWaitForReady_Success(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-ready"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset()

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	if err := sandbox.WaitForReady(ctx); err != nil {
		t.Fatalf("WaitForReady failed unexpectedly: %v", err)
	}

	if sandbox.podName != sbName {
		t.Errorf("podName = %q, want %q", sandbox.podName, sbName)
	}
}

func TestWaitForReady_FailsFast_ImagePullBackOff_WithEvent(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-pull-error"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: metav1.ConditionFalse,
				},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "sandbox",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: `Back-off pulling image "geminicli-toolbox:latest"`,
						},
					},
				},
			},
		},
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "pull-failure-event",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod",
			Name: sbName,
		},
		Type:    corev1.EventTypeWarning,
		Reason:  "Failed",
		Message: `Failed to pull image "geminicli-toolbox:latest": rpc error: code = NotFound desc = manifest unknown`,
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset(pod, event)

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	start := time.Now()
	err := sandbox.WaitForReady(ctx)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected WaitForReady to fail fast, got nil error")
	}

	if duration > 10*time.Second {
		t.Errorf("WaitForReady took %v, want < 10s", duration)
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "failed to pull image") {
		t.Errorf("expected error to contain 'failed to pull image', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "ImagePullBackOff") {
		t.Errorf("expected error to contain 'ImagePullBackOff', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "manifest unknown") {
		t.Errorf("expected error to contain event message with 'manifest unknown', got: %s", errMsg)
	}
}

func TestWaitForReady_FailsFast_ErrImagePull_NoEvent(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-err-pull"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "sandbox",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ErrImagePull",
							Message: `rpc error: code = Unknown desc = repository not found`,
						},
					},
				},
			},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset(pod)

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	start := time.Now()
	err := sandbox.WaitForReady(ctx)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected WaitForReady to fail fast, got nil error")
	}

	if duration > 10*time.Second {
		t.Errorf("WaitForReady took %v, want < 10s", duration)
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "ErrImagePull") {
		t.Errorf("expected error to contain 'ErrImagePull', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "repository not found") {
		t.Errorf("expected error to contain 'repository not found', got: %s", errMsg)
	}
}

func TestWaitForReady_FailsFast_InitContainer_ImagePullBackOff(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-init-pull-error"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "init-service-portals",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: `Back-off pulling image "init-service-portals:latest"`,
						},
					},
				},
			},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset(pod)

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	err := sandbox.WaitForReady(ctx)
	if err == nil {
		t.Fatal("expected WaitForReady to fail fast for init container, got nil error")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "init-service-portals") {
		t.Errorf("expected error to mention container 'init-service-portals', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "ImagePullBackOff") {
		t.Errorf("expected error to contain 'ImagePullBackOff', got: %s", errMsg)
	}
}

func TestCreateSandbox_AsymmetricAuth_NoSecret(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"
	sbName := "test-session-123"
	cpNamespace := "ao-system"

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
		sandboxes:    make(map[types.NamespacedName]*Sandbox),
	}

	pub, priv, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	opts := RunOptions{
		Namespace:             ns,
		ControlPlaneNamespace: cpNamespace,
		SigningKey:            priv,
	}

	harness := NewHarness(nil, client, nil, opts)

	sess := &Session{
		Name:      sbName,
		client:    client,
		sandboxID: types.NamespacedName{Namespace: ns, Name: sbName},
	}

	sb, err := harness.CreateSandbox(ctx, sess)
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	if !sb.created {
		t.Error("expected sb.created to be true")
	}
	if len(sb.signingKey) == 0 {
		t.Error("expected sb.signingKey to be set")
	}
	wantAudience := fmt.Sprintf("%s/%s", ns, sbName)
	if sb.audience != wantAudience {
		t.Errorf("sb.audience = %q, want %q", sb.audience, wantAudience)
	}

	// 1. Verify Sandbox resource in fake agents client has --auth-public-key and --auth-audience
	createdSB, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Get(ctx, sbName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get created Sandbox from fake clientset: %v", err)
	}

	var sandboxContainer *corev1.Container
	for i := range createdSB.Spec.PodTemplate.Spec.Containers {
		c := &createdSB.Spec.PodTemplate.Spec.Containers[i]
		if c.Name == "sandbox" {
			sandboxContainer = c
			break
		}
	}
	if sandboxContainer == nil {
		t.Fatal("sandbox container not found in pod template")
	}

	cmdJoined := strings.Join(sandboxContainer.Command, " ")
	wantPubKeyArg := fmt.Sprintf("--auth-public-key=%s", base64.StdEncoding.EncodeToString(pub))
	wantAudArg := fmt.Sprintf("--auth-audience=%s", wantAudience)

	if !strings.Contains(cmdJoined, wantPubKeyArg) {
		t.Errorf("expected container command to contain %q, got: %s", wantPubKeyArg, cmdJoined)
	}
	if !strings.Contains(cmdJoined, wantAudArg) {
		t.Errorf("expected container command to contain %q, got: %s", wantAudArg, cmdJoined)
	}
	if strings.Contains(cmdJoined, "--auth-token-sha256") {
		t.Errorf("expected container command to NOT contain --auth-token-sha256, got: %s", cmdJoined)
	}

	// 2. Verify NO secrets were created in the control plane namespace
	secrets, err := coreCS.CoreV1().Secrets(cpNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list secrets: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Errorf("expected 0 secrets in %s, got %d", cpNamespace, len(secrets.Items))
	}

	// 3. Verify DeleteSandbox deletes the Sandbox and succeeds without secrets
	if err := client.DeleteSandbox(ctx, sb); err != nil {
		t.Fatalf("DeleteSandbox failed: %v", err)
	}
	_, err = agentsCS.AgentsV1beta1().Sandboxes(ns).Get(ctx, sbName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected Sandbox to be deleted, but it still exists")
	}
}

func TestCreateSandbox_MissingSigningKey_Fails(t *testing.T) {
	ctx := t.Context()
	harness := NewHarness(nil, &SandboxClient{}, nil, RunOptions{
		ControlPlaneNamespace: "ao-system",
	})
	sess := &Session{
		Name:      "test-sess",
		sandboxID: types.NamespacedName{Namespace: "ns", Name: "test-sess"},
	}
	_, err := harness.CreateSandbox(ctx, sess)
	if err == nil || !strings.Contains(err.Error(), "SigningKey must be configured in RunOptions") {
		t.Fatalf("expected error mentioning SigningKey must be configured, got: %v", err)
	}
}

func TestCheckAliveAndExtendLifecycle(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "test-sb"

	now := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	inactivityTimeout := 15 * time.Minute

	initialShutdownTime := now.Add(5 * time.Minute)
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			ShutdownTime: &metav1.Time{Time: initialShutdownTime},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}

	client := &SandboxClient{
		agentsClient: agentsCS,
	}
	sandbox := &Sandbox{
		client: client,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	// 1. Success: verifies alive and extends lifecycle using the provided now
	if err := sandbox.CheckAliveAndExtendLifecycle(ctx, now, inactivityTimeout); err != nil {
		t.Fatalf("CheckAliveAndExtendLifecycle failed: %v", err)
	}

	updated, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Get(ctx, sbName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get updated sandbox: %v", err)
	}
	wantShutdownTime := now.Add(inactivityTimeout)
	if !updated.Spec.ShutdownTime.Time.Equal(wantShutdownTime) {
		t.Errorf("ShutdownTime = %v, want %v", updated.Spec.ShutdownTime.Time, wantShutdownTime)
	}

	// 2. Expired: now is after ShutdownTime
	laterNow := now.Add(20 * time.Minute)
	err = sandbox.CheckAliveAndExtendLifecycle(ctx, laterNow, inactivityTimeout)
	if err == nil {
		t.Fatal("expected error for expired sandbox, got nil")
	}
	if !errors.Is(err, ErrSandboxExpired) {
		t.Errorf("expected ErrSandboxExpired, got: %v", err)
	}

	// 3. Not found: sandbox does not exist
	missingSandbox := &Sandbox{
		client: client,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      "nonexistent",
		},
	}
	err = missingSandbox.CheckAliveAndExtendLifecycle(ctx, now, inactivityTimeout)
	if err == nil {
		t.Fatal("expected error for missing sandbox, got nil")
	}
	if !errors.Is(err, ErrSandboxExpired) {
		t.Errorf("expected ErrSandboxExpired, got: %v", err)
	}
}

func TestSandbox_CheckAlive(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "test-sb-alive"

	now := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	initialShutdownTime := now.Add(10 * time.Minute)
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			ShutdownTime: &metav1.Time{Time: initialShutdownTime},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}

	client := &SandboxClient{
		agentsClient: agentsCS,
	}
	sandbox := &Sandbox{
		client: client,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	// 1. Success: verifies alive and does NOT update ShutdownTime
	if err := sandbox.CheckAlive(ctx, now); err != nil {
		t.Fatalf("CheckAlive failed: %v", err)
	}
	got, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Get(ctx, sbName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	if !got.Spec.ShutdownTime.Time.Equal(initialShutdownTime) {
		t.Errorf("expected ShutdownTime to remain %v, got %v", initialShutdownTime, got.Spec.ShutdownTime.Time)
	}

	// 2. Expired: now is after ShutdownTime
	laterNow := now.Add(20 * time.Minute)
	err = sandbox.CheckAlive(ctx, laterNow)
	if err == nil {
		t.Fatal("expected error for expired sandbox, got nil")
	}
	if !errors.Is(err, ErrSandboxExpired) {
		t.Errorf("expected ErrSandboxExpired, got: %v", err)
	}

	// 3. Not found: sandbox does not exist
	missingSandbox := &Sandbox{
		client: client,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      "nonexistent",
		},
	}
	err = missingSandbox.CheckAlive(ctx, now)
	if err == nil {
		t.Fatal("expected error for missing sandbox, got nil")
	}
	if !errors.Is(err, ErrSandboxExpired) {
		t.Errorf("expected ErrSandboxExpired, got: %v", err)
	}
}

func TestCreateSandboxWithOptions(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"
	sbName := "test-sb-options"
	cpNamespace := "ao-system"

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	client := NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	_, privKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	harness := NewHarness(nil, client, nil, RunOptions{
		Namespace:             ns,
		ControlPlaneNamespace: cpNamespace,
		SigningKey:            privKey,
	})

	isController := true
	ownerRefs := []metav1.OwnerReference{
		{
			APIVersion: "auto-optimizer.labs.gke.io/v1alpha1",
			Kind:       "HeroRun",
			Name:       "hero-run-1",
			UID:        "uid-123",
			Controller: &isController,
		},
	}
	labels := map[string]string{
		"auto-optimizer.labs.gke.io/run": "run-123",
	}
	annotations := map[string]string{
		"auto-optimizer.labs.gke.io/repo": "github.com/gke-labs/test",
	}

	customShutdown := time.Date(2026, 11, 2, 12, 45, 0, 0, time.UTC)
	sb, err := harness.CreateSandboxWithOptions(ctx, CreateSandboxOptions{
		Name:            sbName,
		Labels:          labels,
		Annotations:     annotations,
		OwnerReferences: ownerRefs,
		ShutdownTime:    customShutdown,
	})
	if err != nil {
		t.Fatalf("CreateSandboxWithOptions failed: %v", err)
	}
	defer func() { _ = client.DeleteSandbox(ctx, sb) }()

	created, err := client.GetSandboxResource(ctx, ns, sbName)
	if err != nil {
		t.Fatalf("GetSandboxResource failed: %v", err)
	}
	if created.Labels["auto-optimizer.labs.gke.io/run"] != "run-123" {
		t.Errorf("expected label run-123, got %q", created.Labels["auto-optimizer.labs.gke.io/run"])
	}
	if created.Annotations["auto-optimizer.labs.gke.io/repo"] != "github.com/gke-labs/test" {
		t.Errorf("expected annotation github.com/gke-labs/test, got %q", created.Annotations["auto-optimizer.labs.gke.io/repo"])
	}
	if len(created.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(created.OwnerReferences))
	}
	ref := created.OwnerReferences[0]
	if ref.APIVersion != "auto-optimizer.labs.gke.io/v1alpha1" {
		t.Errorf("expected APIVersion auto-optimizer.labs.gke.io/v1alpha1, got %q", ref.APIVersion)
	}
	if ref.Kind != "HeroRun" {
		t.Errorf("expected Kind HeroRun, got %q", ref.Kind)
	}
	if ref.Name != "hero-run-1" {
		t.Errorf("expected Name hero-run-1, got %q", ref.Name)
	}
	if ref.UID != "uid-123" {
		t.Errorf("expected UID uid-123, got %q", ref.UID)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("expected Controller true")
	}
	if !created.Spec.ShutdownTime.Time.Equal(customShutdown) {
		t.Errorf("expected ShutdownTime %v, got %v", customShutdown, created.Spec.ShutdownTime.Time)
	}

	// Zero ShutdownTime defaults to now + SandboxInactivityTimeout
	zeroSbName := "test-sb-zero-shutdown"
	zeroSb, err := harness.CreateSandboxWithOptions(ctx, CreateSandboxOptions{
		Name: zeroSbName,
	})
	if err != nil {
		t.Fatalf("CreateSandboxWithOptions with zero ShutdownTime failed: %v", err)
	}
	defer func() { _ = client.DeleteSandbox(ctx, zeroSb) }()

	zeroCreated, err := client.GetSandboxResource(ctx, ns, zeroSbName)
	if err != nil {
		t.Fatalf("GetSandboxResource failed: %v", err)
	}
	if zeroCreated.Spec.ShutdownTime == nil {
		t.Fatal("expected non-nil ShutdownTime for default zero option")
	}
	expectedMin := time.Now().Add(SandboxInactivityTimeout - 10*time.Second)
	expectedMax := time.Now().Add(SandboxInactivityTimeout + 10*time.Second)
	if zeroCreated.Spec.ShutdownTime.Time.Before(expectedMin) || zeroCreated.Spec.ShutdownTime.Time.After(expectedMax) {
		t.Errorf("expected default ShutdownTime near now + %v, got %v", SandboxInactivityTimeout, zeroCreated.Spec.ShutdownTime.Time)
	}
}

func TestEnsureSandbox_OnDemandWithCustomOptionsAndPreparer(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"
	sessionName := "testsessionondemand"
	cpNamespace := "ao-system"

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	client := NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	agentsCS.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		sb := createAction.GetObject().(*sandboxv1beta1.Sandbox).DeepCopy()
		sb.Status.Conditions = []metav1.Condition{
			{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			},
		}
		_ = agentsCS.Tracker().Create(action.GetResource(), sb, action.GetNamespace())
		return true, sb, nil
	})

	_, privKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	harness := NewHarness(nil, client, nil, RunOptions{
		Namespace:             ns,
		ControlPlaneNamespace: cpNamespace,
		SigningKey:            privKey,
	})

	sessionStore := NewFileStore(t.TempDir())
	sess, err := harness.NewSession(ctx, sessionStore, sessionName, Metadata{CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	claimName := "claim-ondemand"
	sess.SetCreateSandboxOptions(CreateSandboxOptions{
		Name:               sessionName,
		Labels:             map[string]string{"run-id": "123"},
		WorkspaceClaimName: claimName,
		ShutdownTime:       time.Now().Add(2 * time.Hour),
		ExtendLifecycle:    false,
	})

	prepareCount := 0
	sess.SetSandboxPreparer(func(ctx context.Context, sb *Sandbox) error {
		prepareCount++
		return nil
	})

	// First ensureSandbox call creates the sandbox on demand and runs preparer
	sb1, err := harness.ensureSandbox(ctx, sess)
	if err != nil {
		t.Fatalf("ensureSandbox 1: %v", err)
	}
	if sb1 == nil {
		t.Fatal("expected non-nil sandbox")
	}
	if prepareCount != 1 {
		t.Errorf("expected prepareCount 1, got %d", prepareCount)
	}

	// Verify created sandbox resource
	res1, err := client.GetSandboxResource(ctx, ns, sessionName)
	if err != nil {
		t.Fatalf("getting sandbox resource: %v", err)
	}
	if res1.Labels["run-id"] != "123" {
		t.Errorf("expected label 123, got %q", res1.Labels["run-id"])
	}

	// Delete sandbox externally
	if err := agentsCS.AgentsV1beta1().Sandboxes(ns).Delete(ctx, sessionName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting sandbox: %v", err)
	}

	// Drop dead sandbox through helper and verify ensureSandbox recreates on demand, calling preparer again
	harness.dropDeadSandbox(ctx, sess, sess.activeSandbox)
	sb2, err := harness.ensureSandbox(ctx, sess)
	if err != nil {
		t.Fatalf("ensureSandbox 2: %v", err)
	}
	if sb2 == nil {
		t.Fatal("expected non-nil sandbox on recreation")
	}
	if prepareCount != 2 {
		t.Errorf("expected prepareCount 2, got %d", prepareCount)
	}
}

type mockControlPlaneTool struct {
	calledWithSb tools.Sandbox
	called       bool
}

func (m *mockControlPlaneTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name:        "mock_cp_tool",
			Description: "a mock control plane tool",
		},
	}
}

func (m *mockControlPlaneTool) Run(ctx context.Context, sb tools.Sandbox) (llm.Message, error) {
	m.called = true
	m.calledWithSb = sb
	if sb != nil {
		return llm.Message{}, fmt.Errorf("expected sb == nil, got %v", sb)
	}
	content := "success"
	return llm.Message{Role: "tool", Content: &content}, nil
}

func TestControlPlaneTool_RunsBeforeSandboxExists(t *testing.T) {
	ctx := t.Context()
	cpTool := &mockControlPlaneTool{}

	done := "done"
	mockLLM := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			{
				ID: "cp-call",
				Choices: []llm.Choice{
					{
						Message: llm.Message{
							Role: "assistant",
							ToolCalls: []llm.ToolCall{
								{
									ID:   "call-1",
									Type: "function",
									Function: llm.FunctionCall{
										Name:      "mock_cp_tool",
										Arguments: "{}",
									},
								},
							},
						},
						FinishReason: "tool_calls",
					},
				},
			},
			{
				ID: "cp-done",
				Choices: []llm.Choice{
					{
						Message: llm.Message{
							Role:    "assistant",
							Content: &done,
						},
						FinishReason: "stop",
					},
				},
			},
		},
	}

	harness := NewHarness(mockLLM, nil, nil, RunOptions{}).WithExtraTools(cpTool)
	sessionStore := NewFileStore(t.TempDir())
	sess, err := harness.NewSession(ctx, sessionStore, "cpsession", Metadata{CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Verify no sandbox exists initially
	if sess.activeSandbox != nil {
		t.Fatal("expected activeSandbox to be nil")
	}

	events := &recordingEvents{}
	if err := harness.RunTurn(ctx, sess, "run cp tool", events); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}

	if !cpTool.called {
		t.Fatal("expected mock_cp_tool to be called")
	}
	if cpTool.calledWithSb != nil {
		t.Errorf("expected mock_cp_tool to receive nil Sandbox, got %v", cpTool.calledWithSb)
	}
}

func TestSandbox_SetPodInfo(t *testing.T) {
	sb := &Sandbox{}
	sb.SetPodInfo("pod-abc", "10.0.0.1")
	if sb.podName != "pod-abc" {
		t.Errorf("expected podName 'pod-abc', got %q", sb.podName)
	}
	if sb.podIP != "10.0.0.1" {
		t.Errorf("expected podIP '10.0.0.1', got %q", sb.podIP)
	}
}

func setupContinueTurnTest(t *testing.T, llmClient llm.ChatClient) (*Harness, *Session, *recordingSandbox, *recordingEvents) {
	t.Helper()
	ctx := t.Context()

	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get(basic): %v", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts RunOptions
	opts.InitDefaults()
	opts.ModelName = "test-model"

	fakeSB := &recordingSandbox{}

	harness := NewHarness(llmClient, nil, toolsRegistry, opts)
	harness.SetSandboxOverride(fakeSB)

	storeDir := t.TempDir()
	sessionStore := NewFileStore(storeDir)

	sess, err := harness.NewSession(ctx, sessionStore, "continueturnsess", Metadata{Toolset: "basic"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	events := &recordingEvents{}
	return harness, sess, fakeSB, events
}

func countUserMessages(msgs []llm.Message) int {
	n := 0
	for _, m := range msgs {
		if m.Role == "user" {
			n++
		}
	}
	return n
}

func ptr[T any](v T) *T {
	return &v
}

func scriptedResponse(msg llm.Message) *llm.ChatCompletionResponse {
	return &llm.ChatCompletionResponse{
		ID: "resp-test",
		Choices: []llm.Choice{
			{
				Message:      msg,
				FinishReason: "stop",
			},
		},
	}
}

func TestContinueTurn_UnansweredToolCall(t *testing.T) {
	ctx := t.Context()
	reply := "Linux verified"
	llmClient := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			scriptedResponse(llm.Message{Role: "assistant", Content: &reply}),
		},
	}

	harness, sess, fakeSB, events := setupContinueTurnTest(t, llmClient)

	userMsg := llm.Message{Role: "user", Content: ptr("check uname")}
	assistantMsg := llm.Message{
		Role: "assistant",
		ToolCalls: []llm.ToolCall{
			{
				ID:   "call-1",
				Type: "function",
				Function: llm.FunctionCall{
					Name:      "run_command",
					Arguments: `{"command":"uname -a"}`,
				},
			},
		},
	}
	if err := sess.AddMessages(ctx, userMsg, assistantMsg); err != nil {
		t.Fatalf("AddMessages: %v", err)
	}

	if err := harness.ContinueTurn(ctx, sess, events); err != nil {
		t.Fatalf("ContinueTurn: %v", err)
	}

	if got := len(fakeSB.calls); got != 1 {
		t.Fatalf("sandbox calls = %d, want 1", got)
	}
	expectedCmd := []string{"sh", "-c", "uname -a"}
	if !slices.Equal(fakeSB.calls[0].Command, expectedCmd) {
		t.Errorf("command = %v, want %v", fakeSB.calls[0].Command, expectedCmd)
	}
	if llmClient.idx != 1 {
		t.Errorf("llmClient.idx = %d, want 1 (model called once to continue)", llmClient.idx)
	}

	msgs := sess.Messages()
	if got := countUserMessages(msgs); got != 1 {
		t.Errorf("user message count = %d, want 1 (no extra user message appended)", got)
	}
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages (user, assistant, tool, assistant), got %d", len(msgs))
	}
	if msgs[2].Role != "tool" || msgs[2].ToolCallID != "call-1" {
		t.Errorf("msg[2] = %+v, want tool message for call-1", msgs[2])
	}
	if msgs[3].Role != "assistant" || valueOf(msgs[3].Content) != reply {
		t.Errorf("msg[3] = %+v, want assistant message with %q", msgs[3], reply)
	}
}

func TestContinueTurn_PartialToolResults(t *testing.T) {
	ctx := t.Context()
	reply := "both done"
	llmClient := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			scriptedResponse(llm.Message{Role: "assistant", Content: &reply}),
		},
	}

	harness, sess, fakeSB, events := setupContinueTurnTest(t, llmClient)

	userMsg := llm.Message{Role: "user", Content: ptr("run two commands")}
	assistantMsg := llm.Message{
		Role: "assistant",
		ToolCalls: []llm.ToolCall{
			{
				ID:   "call-1",
				Type: "function",
				Function: llm.FunctionCall{
					Name:      "run_command",
					Arguments: `{"command":"uname -a"}`,
				},
			},
			{
				ID:   "call-2",
				Type: "function",
				Function: llm.FunctionCall{
					Name:      "run_command",
					Arguments: `{"command":"date"}`,
				},
			},
		},
	}
	toolResult1 := llm.Message{
		Role:       "tool",
		ToolCallID: "call-1",
		Content:    ptr("Linux fake-node"),
	}
	if err := sess.AddMessages(ctx, userMsg, assistantMsg, toolResult1); err != nil {
		t.Fatalf("AddMessages: %v", err)
	}

	if err := harness.ContinueTurn(ctx, sess, events); err != nil {
		t.Fatalf("ContinueTurn: %v", err)
	}

	if got := len(fakeSB.calls); got != 1 {
		t.Fatalf("sandbox calls = %d, want 1 (only the unanswered tool call should run)", got)
	}
	expectedCmd := []string{"sh", "-c", "date"}
	if !slices.Equal(fakeSB.calls[0].Command, expectedCmd) {
		t.Errorf("command = %v, want %v", fakeSB.calls[0].Command, expectedCmd)
	}
	if llmClient.idx != 1 {
		t.Errorf("llmClient.idx = %d, want 1 (model called once to continue)", llmClient.idx)
	}

	msgs := sess.Messages()
	if got := countUserMessages(msgs); got != 1 {
		t.Errorf("user message count = %d, want 1 (no extra user message appended)", got)
	}
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages (user, assistant, tool1, tool2, assistant), got %d", len(msgs))
	}
	if msgs[2].ToolCallID != "call-1" || msgs[3].ToolCallID != "call-2" {
		t.Errorf("tool messages: [2]=%s, [3]=%s; want call-1, call-2", msgs[2].ToolCallID, msgs[3].ToolCallID)
	}
	if msgs[4].Role != "assistant" || valueOf(msgs[4].Content) != reply {
		t.Errorf("msg[4] = %+v, want assistant message with %q", msgs[4], reply)
	}
}

func TestContinueTurn_CompleteToolResults(t *testing.T) {
	ctx := t.Context()
	reply := "command finished"
	llmClient := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			scriptedResponse(llm.Message{Role: "assistant", Content: &reply}),
		},
	}

	harness, sess, fakeSB, events := setupContinueTurnTest(t, llmClient)

	userMsg := llm.Message{Role: "user", Content: ptr("run command")}
	assistantMsg := llm.Message{
		Role: "assistant",
		ToolCalls: []llm.ToolCall{
			{
				ID:   "call-1",
				Type: "function",
				Function: llm.FunctionCall{
					Name:      "run_command",
					Arguments: `{"command":"uname -a"}`,
				},
			},
		},
	}
	toolResult1 := llm.Message{
		Role:       "tool",
		ToolCallID: "call-1",
		Content:    ptr("Linux fake-node"),
	}
	if err := sess.AddMessages(ctx, userMsg, assistantMsg, toolResult1); err != nil {
		t.Fatalf("AddMessages: %v", err)
	}

	if err := harness.ContinueTurn(ctx, sess, events); err != nil {
		t.Fatalf("ContinueTurn: %v", err)
	}

	if got := len(fakeSB.calls); got != 0 {
		t.Fatalf("sandbox calls = %d, want 0 (tool call already had result, should not run again)", got)
	}
	if llmClient.idx != 1 {
		t.Errorf("llmClient.idx = %d, want 1 (model called to continue loop)", llmClient.idx)
	}

	msgs := sess.Messages()
	if got := countUserMessages(msgs); got != 1 {
		t.Errorf("user message count = %d, want 1 (no extra user message appended)", got)
	}
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages (user, assistant, tool, assistant), got %d", len(msgs))
	}
	if msgs[3].Role != "assistant" || valueOf(msgs[3].Content) != reply {
		t.Errorf("msg[3] = %+v, want assistant message with %q", msgs[3], reply)
	}
}

func TestContinueTurn_FinalAssistantMessage(t *testing.T) {
	ctx := t.Context()
	// No responses configured; if LLM is called, CreateChatCompletion returns an error.
	llmClient := &scriptedLLM{}

	harness, sess, fakeSB, events := setupContinueTurnTest(t, llmClient)

	userMsg := llm.Message{Role: "user", Content: ptr("hello")}
	assistantMsg := llm.Message{Role: "assistant", Content: ptr("How can I help you?")}
	if err := sess.AddMessages(ctx, userMsg, assistantMsg); err != nil {
		t.Fatalf("AddMessages: %v", err)
	}

	if err := harness.ContinueTurn(ctx, sess, events); err != nil {
		t.Fatalf("ContinueTurn: %v", err)
	}

	if got := len(fakeSB.calls); got != 0 {
		t.Errorf("sandbox calls = %d, want 0", got)
	}
	if llmClient.idx != 0 {
		t.Errorf("llmClient.idx = %d, want 0 (model should not be called)", llmClient.idx)
	}

	msgs := sess.Messages()
	if got := countUserMessages(msgs); got != 1 {
		t.Errorf("user message count = %d, want 1 (no extra user message appended)", got)
	}
	if len(msgs) != 2 {
		t.Errorf("session messages count = %d, want 2 (transcript unchanged)", len(msgs))
	}
}

func TestContinueTurn_UserMessageTail(t *testing.T) {
	ctx := t.Context()
	reply := "Hello user"
	llmClient := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			scriptedResponse(llm.Message{Role: "assistant", Content: &reply}),
		},
	}

	harness, sess, fakeSB, events := setupContinueTurnTest(t, llmClient)

	userMsg := llm.Message{Role: "user", Content: ptr("hello from user")}
	if err := sess.AddMessages(ctx, userMsg); err != nil {
		t.Fatalf("AddMessages: %v", err)
	}

	if err := harness.ContinueTurn(ctx, sess, events); err != nil {
		t.Fatalf("ContinueTurn: %v", err)
	}

	if got := len(fakeSB.calls); got != 0 {
		t.Errorf("sandbox calls = %d, want 0", got)
	}
	if llmClient.idx != 1 {
		t.Errorf("llmClient.idx = %d, want 1", llmClient.idx)
	}

	msgs := sess.Messages()
	if got := countUserMessages(msgs); got != 1 {
		t.Errorf("user message count = %d, want 1 (no extra user message appended)", got)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (user, assistant), got %d", len(msgs))
	}
	if msgs[1].Role != "assistant" || valueOf(msgs[1].Content) != reply {
		t.Errorf("msg[1] = %+v, want assistant message with %q", msgs[1], reply)
	}
}

func TestContinueTurn_CancelledToolCallRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	reply := "Linux verified"
	llmClient := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			scriptedResponse(llm.Message{
				Role: "assistant",
				ToolCalls: []llm.ToolCall{{
					ID: "call-1", Type: "function",
					Function: llm.FunctionCall{Name: "run_command", Arguments: `{"command":"uname -a"}`},
				}},
			}),
			scriptedResponse(llm.Message{Role: "assistant", Content: &reply}),
		},
	}

	harness, sess, fakeSB, events := setupContinueTurnTest(t, llmClient)

	var cancelled bool
	fakeSB.onExec = func(execCtx context.Context, _ tools.ExecCommandOptions) error {
		if !cancelled {
			cancelled = true
			cancel()
			return execCtx.Err()
		}
		return nil
	}

	err := harness.RunTurn(ctx, sess, "check uname", events)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunTurn err = %v, want context.Canceled", err)
	}

	msgs := sess.Messages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (user, assistant), got %d", len(msgs))
	}
	if len(msgs[1].ToolCalls) != 1 || msgs[1].ToolCalls[0].ID != "call-1" {
		t.Fatalf("expected assistant message with tool call call-1, got %+v", msgs[1])
	}

	freshCtx := t.Context()
	if err := harness.ContinueTurn(freshCtx, sess, events); err != nil {
		t.Fatalf("ContinueTurn: %v", err)
	}

	if got := len(fakeSB.calls); got != 2 {
		t.Fatalf("sandbox calls = %d, want 2 (1 cancelled, 1 retry)", got)
	}
	if llmClient.idx != 2 {
		t.Errorf("llmClient.idx = %d, want 2", llmClient.idx)
	}

	msgs = sess.Messages()
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages (user, assistant, tool, assistant), got %d", len(msgs))
	}
	if msgs[2].Role != "tool" || msgs[2].ToolCallID != "call-1" {
		t.Errorf("msg[2] = %+v, want tool message for call-1", msgs[2])
	}
	if msgs[3].Role != "assistant" || valueOf(msgs[3].Content) != reply {
		t.Errorf("msg[3] = %+v, want assistant message with %q", msgs[3], reply)
	}
}

func TestCreateSandboxWithOptions_WorkspaceClaim(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"
	cpNamespace := "ao-system"

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	client := NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	_, privKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	harness := NewHarness(nil, client, nil, RunOptions{
		Namespace:             ns,
		ControlPlaneNamespace: cpNamespace,
		SigningKey:            privKey,
	})

	tests := []struct {
		name        string
		sandboxName string
		claimName   string
	}{
		{
			name:        "mounts PersistentVolumeClaim when claim name is set",
			sandboxName: "test-sb-pvc",
			claimName:   "test-claim",
		},
		{
			name:        "mounts emptyDir when claim name is empty",
			sandboxName: "test-sb-empty-claim",
			claimName:   "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sb, err := harness.CreateSandboxWithOptions(ctx, CreateSandboxOptions{
				Name:               tc.sandboxName,
				WorkspaceClaimName: tc.claimName,
			})
			if err != nil {
				t.Fatalf("CreateSandboxWithOptions failed: %v", err)
			}
			defer func() { _ = client.DeleteSandbox(ctx, sb) }()

			created, err := client.GetSandboxResource(ctx, ns, tc.sandboxName)
			if err != nil {
				t.Fatalf("GetSandboxResource failed: %v", err)
			}

			var vol *corev1.Volume
			for i := range created.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes {
				if created.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes[i].Name == "workspace" {
					vol = &created.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes[i]
					break
				}
			}
			if vol == nil {
				t.Fatal("expected workspace volume to exist")
			}

			if tc.claimName != "" {
				if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != tc.claimName {
					t.Errorf("expected PersistentVolumeClaim with ClaimName %q, got %+v", tc.claimName, vol.PersistentVolumeClaim)
				}
				if vol.EmptyDir != nil {
					t.Errorf("expected EmptyDir to be nil, got %+v", vol.EmptyDir)
				}
			} else {
				if vol.EmptyDir == nil {
					t.Fatal("expected EmptyDir on workspace volume, got nil")
				}
				if vol.PersistentVolumeClaim != nil {
					t.Errorf("expected PersistentVolumeClaim to be nil, got %+v", vol.PersistentVolumeClaim)
				}
			}

			containers := created.Spec.SandboxBlueprint.PodTemplate.Spec.Containers
			if len(containers) == 0 {
				t.Fatal("expected at least one container")
			}
			var mount *corev1.VolumeMount
			for i := range containers[0].VolumeMounts {
				if containers[0].VolumeMounts[i].Name == "workspace" {
					mount = &containers[0].VolumeMounts[i]
					break
				}
			}
			if mount == nil {
				t.Fatal("expected workspace volume mount to exist")
			}
			if mount.MountPath != "/workspace" {
				t.Errorf("expected MountPath /workspace, got %q", mount.MountPath)
			}
		})
	}
}

func TestCreateSandboxWithOptions_PortalIntercept(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"
	cpNamespace := "ao-system"

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	client := NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	_, privKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	harness := NewHarness(nil, client, nil, RunOptions{
		Namespace:             ns,
		ControlPlaneNamespace: cpNamespace,
		SigningKey:            privKey,
	})

	tests := []struct {
		name            string
		sandboxName     string
		portalIntercept bool
		wantLabel       bool
	}{
		{
			name:            "portal intercept enabled labels pod template",
			sandboxName:     "test-sb-intercept-enabled",
			portalIntercept: true,
			wantLabel:       true,
		},
		{
			name:            "portal intercept disabled leaves pod template unchanged",
			sandboxName:     "test-sb-intercept-disabled",
			portalIntercept: false,
			wantLabel:       false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sb, err := harness.CreateSandboxWithOptions(ctx, CreateSandboxOptions{
				Name:            tc.sandboxName,
				PortalIntercept: tc.portalIntercept,
			})
			if err != nil {
				t.Fatalf("CreateSandboxWithOptions failed: %v", err)
			}
			defer func() { _ = client.DeleteSandbox(ctx, sb) }()

			created, err := client.GetSandboxResource(ctx, ns, tc.sandboxName)
			if err != nil {
				t.Fatalf("GetSandboxResource failed: %v", err)
			}

			podLabels := created.Spec.SandboxBlueprint.PodTemplate.ObjectMeta.Labels
			if tc.wantLabel {
				if podLabels == nil || podLabels[PortalInterceptLabel] != "true" {
					t.Errorf("expected pod template label %s=%q, got %+v", PortalInterceptLabel, "true", podLabels)
				}
			} else {
				if podLabels != nil && podLabels[PortalInterceptLabel] != "" {
					t.Errorf("expected pod template label %s to not be set, got %q", PortalInterceptLabel, podLabels[PortalInterceptLabel])
				}
			}

			// Verify no extra containers, volumes, capabilities, or environment variables are added
			podSpec := created.Spec.SandboxBlueprint.PodTemplate.Spec
			if len(podSpec.InitContainers) != 1 || podSpec.InitContainers[0].Name != "install-sandboxd" {
				t.Errorf("unexpected init containers: %+v", podSpec.InitContainers)
			}
			if len(podSpec.Containers) != 1 || podSpec.Containers[0].Name != "sandbox" {
				t.Errorf("unexpected containers: %+v", podSpec.Containers)
			}
			if len(podSpec.Containers[0].Env) != 1 || podSpec.Containers[0].Env[0].Name != "HOME" {
				t.Errorf("unexpected env: %+v", podSpec.Containers[0].Env)
			}
		})
	}
}

func TestSandbox_PodUID(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"
	sbName := "test-sb-poduid"
	cpNamespace := "ao-system"

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	client := NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	_, privKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	harness := NewHarness(nil, client, nil, RunOptions{
		Namespace:             ns,
		ControlPlaneNamespace: cpNamespace,
		SigningKey:            privKey,
	})

	sb, err := harness.CreateSandboxWithOptions(ctx, CreateSandboxOptions{
		Name: sbName,
	})
	if err != nil {
		t.Fatalf("CreateSandboxWithOptions failed: %v", err)
	}
	defer func() { _ = client.DeleteSandbox(ctx, sb) }()

	// 1. Pod does not exist yet: PodUID fails clearly
	uid, err := sb.PodUID(ctx)
	if err == nil {
		t.Fatalf("expected error when pod does not exist, got uid %q", uid)
	}
	if !errors.Is(err, ErrPodNotFound) {
		t.Errorf("expected ErrPodNotFound, got: %v", err)
	}

	// 2. Pod exists: PodUID returns the pod's UID
	expectedUID := types.UID("uid-sandbox-pod-1234")
	_, err = coreCS.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbName,
			Namespace: ns,
			UID:       expectedUID,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create fake pod: %v", err)
	}

	gotUID, err := sb.PodUID(ctx)
	if err != nil {
		t.Fatalf("PodUID failed: %v", err)
	}
	if gotUID != expectedUID {
		t.Errorf("PodUID = %q, want %q", gotUID, expectedUID)
	}

	// 3. With explicit pod name via SetPodInfo
	otherPodName := "custom-pod-name"
	otherUID := types.UID("uid-custom-5678")
	_, err = coreCS.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      otherPodName,
			Namespace: ns,
			UID:       otherUID,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create fake pod: %v", err)
	}

	sb.SetPodInfo(otherPodName, "10.0.0.42")
	gotOtherUID, err := sb.PodUID(ctx)
	if err != nil {
		t.Fatalf("PodUID with custom pod name failed: %v", err)
	}
	if gotOtherUID != otherUID {
		t.Errorf("PodUID with custom pod name = %q, want %q", gotOtherUID, otherUID)
	}
}
