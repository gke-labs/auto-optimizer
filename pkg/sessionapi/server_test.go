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

package sessionapi

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type recordingSandbox struct {
	mu    sync.Mutex
	calls []tools.ExecCommandOptions
}

func (s *recordingSandbox) ExecCommand(_ context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, opts)

	return &tools.ExecCommandResult{
		Stdout:   "Linux fake-test-node 6.6.0 #1 SMP\n",
		Stderr:   "",
		ExitCode: 0,
	}, nil
}

type blockingSandbox struct {
	startedCh chan struct{}
	blockCh   chan struct{}
}

func (s *blockingSandbox) ExecCommand(_ context.Context, _ tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	select {
	case s.startedCh <- struct{}{}:
	default:
	}
	<-s.blockCh
	return &tools.ExecCommandResult{
		Stdout:   "Linux unblocked\n",
		ExitCode: 0,
	}, nil
}

func setupTestServer(t *testing.T, sb tools.Sandbox) (sessionv1.SessionServiceClient, *session.FileStore, *session.Harness) {
	t.Helper()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}

	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()

	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)
	if sb != nil {
		harness.SetSandboxOverride(sb)
	}

	store := session.NewFileStore(t.TempDir())

	srv, err := NewServer(ServerOptions{
		Store:          store,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
	})

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
	})

	return sessionv1.NewSessionServiceClient(conn), store, harness
}

func TestCreateAndGetSession(t *testing.T) {
	ctx := t.Context()
	client, _, _ := setupTestServer(t, nil)

	// Create the root session first.
	_, err := client.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    "root",
		Toolset: "basic",
	})
	if err != nil {
		t.Fatalf("CreateSession(root) failed: %v", err)
	}

	// Create a new session as a child of "root".
	createResp, err := client.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    "alpha",
		Toolset: "basic",
		Parent:  "root",
	})
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}
	if createResp.Name != "alpha" {
		t.Errorf("Name = %q, want alpha", createResp.Name)
	}
	if createResp.Toolset != "basic" {
		t.Errorf("Toolset = %q, want basic", createResp.Toolset)
	}
	if createResp.Parent != "root" {
		t.Errorf("Parent = %q, want root", createResp.Parent)
	}
	if createResp.MessageCount != 0 {
		t.Errorf("MessageCount = %d, want 0", createResp.MessageCount)
	}

	// Fetch the created session.
	getResp, err := client.GetSession(ctx, &sessionv1.GetSessionRequest{Name: "alpha"})
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if getResp.Name != "alpha" || getResp.Toolset != "basic" || getResp.Parent != "root" {
		t.Errorf("GetSession = %+v, want alpha/basic/root", getResp)
	}

	// Creating again with same name must return AlreadyExists.
	_, err = client.CreateSession(ctx, &sessionv1.CreateSessionRequest{Name: "alpha"})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateSession duplicate got status code %v, want AlreadyExists", status.Code(err))
	}

	// Fetch non-existent session must return NotFound.
	_, err = client.GetSession(ctx, &sessionv1.GetSessionRequest{Name: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetSession missing got status code %v, want NotFound", status.Code(err))
	}
}

func TestListSessions(t *testing.T) {
	ctx := t.Context()
	client, _, _ := setupTestServer(t, nil)

	listResp, err := client.ListSessions(ctx, &sessionv1.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions on empty server: %v", err)
	}
	if len(listResp.Sessions) != 0 {
		t.Errorf("got %d sessions, want 0", len(listResp.Sessions))
	}

	if _, err := client.CreateSession(ctx, &sessionv1.CreateSessionRequest{Name: "s1"}); err != nil {
		t.Fatalf("CreateSession s1: %v", err)
	}
	if _, err := client.CreateSession(ctx, &sessionv1.CreateSessionRequest{Name: "s2"}); err != nil {
		t.Fatalf("CreateSession s2: %v", err)
	}

	listResp, err = client.ListSessions(ctx, &sessionv1.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != 2 {
		t.Fatalf("got %d sessions, want 2", len(listResp.Sessions))
	}
	if listResp.Sessions[0].Name != "s1" || listResp.Sessions[1].Name != "s2" {
		t.Errorf("unexpected sessions: %+v", listResp.Sessions)
	}
}

func TestSendMessage_PlainQuestion(t *testing.T) {
	ctx := t.Context()
	client, _, _ := setupTestServer(t, nil)

	// Sending to a session that does not exist creates it.
	stream, err := client.SendMessage(ctx, &sessionv1.SendMessageRequest{
		Name: "demo",
		Text: "what is the capital of France?",
	})
	if err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}

	var events []*sessionv1.SessionEvent
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv error: %v", err)
		}
		events = append(events, event)
	}

	// Verify we got MessageAppended events and TurnFinished event.
	var finalReply string
	for _, e := range events {
		if tf := e.GetTurnFinished(); tf != nil {
			finalReply = tf.Text
		}
	}

	wantEcho := "What do you think it means when you say 'what is the capital of France?'?"
	if !strings.Contains(finalReply, wantEcho) {
		t.Errorf("finalReply = %q, want containing %q", finalReply, wantEcho)
	}

	// Verify messages via ListMessages.
	listMsgResp, err := client.ListMessages(ctx, &sessionv1.ListMessagesRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(listMsgResp.Messages) < 2 {
		t.Fatalf("ListMessages returned %d messages, want at least 2", len(listMsgResp.Messages))
	}
}

func TestSendMessage_ToolCall(t *testing.T) {
	ctx := t.Context()
	fakeSB := &recordingSandbox{}
	client, _, _ := setupTestServer(t, fakeSB)

	stream, err := client.SendMessage(ctx, &sessionv1.SendMessageRequest{
		Name: "tooldemo",
		Text: "run: uname -a",
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	var toolStarted bool
	var toolFinished bool
	var finalReply string

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv error: %v", err)
		}

		if tc := eToolCallStarted(event); tc != nil {
			toolStarted = true
			if tc.Function.Name != "run_command" {
				t.Errorf("ToolCallStarted name = %q, want run_command", tc.Function.Name)
			}
		}
		if tf := eToolCallFinished(event); tf != nil {
			toolFinished = true
		}
		if finish := event.GetTurnFinished(); finish != nil {
			finalReply = finish.Text
		}
	}

	if !toolStarted {
		t.Errorf("expected ToolCallStarted event to appear")
	}
	if !toolFinished {
		t.Errorf("expected ToolCallFinished event to appear")
	}
	if !strings.Contains(finalReply, "Linux") {
		t.Errorf("finalReply = %q, want containing 'Linux'", finalReply)
	}

	// Verify sandbox recorded invocation.
	fakeSB.mu.Lock()
	calls := len(fakeSB.calls)
	fakeSB.mu.Unlock()
	if calls != 1 {
		t.Errorf("sandbox calls = %d, want 1", calls)
	}
}

func TestSendMessage_BusySession(t *testing.T) {
	ctx := t.Context()
	blockingSB := &blockingSandbox{
		startedCh: make(chan struct{}, 1),
		blockCh:   make(chan struct{}),
	}
	client, _, _ := setupTestServer(t, blockingSB)

	// Start first turn in background.
	turn1ErrCh := make(chan error, 1)
	go func() {
		stream, err := client.SendMessage(ctx, &sessionv1.SendMessageRequest{
			Name: "busy",
			Text: "run: uname -a",
		})
		if err != nil {
			turn1ErrCh <- err
			return
		}
		for {
			_, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					turn1ErrCh <- nil
				} else {
					turn1ErrCh <- err
				}
				return
			}
		}
	}()

	// Wait until the first turn is actively running inside the sandbox.
	select {
	case <-blockingSB.startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first turn to start")
	}

	// Second concurrent SendMessage on the same session must return FAILED_PRECONDITION.
	stream2, err := client.SendMessage(ctx, &sessionv1.SendMessageRequest{
		Name: "busy",
		Text: "second prompt",
	})
	if err == nil {
		_, err = stream2.Recv()
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("concurrent SendMessage got status code %v (err: %v), want FailedPrecondition", status.Code(err), err)
	}

	// Unblock the first turn and verify it finishes.
	close(blockingSB.blockCh)
	select {
	case err := <-turn1ErrCh:
		if err != nil {
			t.Errorf("turn 1 failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for turn 1 to complete")
	}
}

func TestConcurrentReadDuringTurn(t *testing.T) {
	ctx := t.Context()
	blockingSB := &blockingSandbox{
		startedCh: make(chan struct{}, 1),
		blockCh:   make(chan struct{}),
	}
	client, _, _ := setupTestServer(t, blockingSB)

	turnErrCh := make(chan error, 1)
	go func() {
		stream, err := client.SendMessage(ctx, &sessionv1.SendMessageRequest{
			Name: "concurrent",
			Text: "run: uname -a",
		})
		if err != nil {
			turnErrCh <- err
			return
		}
		for {
			_, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					turnErrCh <- nil
				} else {
					turnErrCh <- err
				}
				return
			}
		}
	}()

	// Wait until the turn is in-flight and blocked in the sandbox.
	select {
	case <-blockingSB.startedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for turn to reach sandbox")
	}

	// Run concurrent readers while the turn is in-flight and while it finishes.
	stopReaders := make(chan struct{})
	var wg sync.WaitGroup

	// Reader 1: ListMessages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReaders:
				return
			default:
				resp, err := client.ListMessages(ctx, &sessionv1.ListMessagesRequest{Name: "concurrent"})
				if err != nil {
					t.Errorf("concurrent ListMessages failed: %v", err)
					return
				}
				if len(resp.Messages) == 0 {
					t.Errorf("concurrent ListMessages returned 0 messages")
					return
				}
			}
		}
	}()

	// Reader 2: ListSessions
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReaders:
				return
			default:
				resp, err := client.ListSessions(ctx, &sessionv1.ListSessionsRequest{})
				if err != nil {
					t.Errorf("concurrent ListSessions failed: %v", err)
					return
				}
				if len(resp.Sessions) == 0 {
					t.Errorf("concurrent ListSessions returned 0 sessions")
					return
				}
			}
		}
	}()

	// Reader 3: GetSession
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReaders:
				return
			default:
				resp, err := client.GetSession(ctx, &sessionv1.GetSessionRequest{Name: "concurrent"})
				if err != nil {
					t.Errorf("concurrent GetSession failed: %v", err)
					return
				}
				if resp.MessageCount == 0 {
					t.Errorf("concurrent GetSession returned 0 message count")
					return
				}
			}
		}
	}()

	// Let the readers loop while blocked in the sandbox.
	time.Sleep(20 * time.Millisecond)

	// Unblock the sandbox so the turn proceeds and appends more messages.
	close(blockingSB.blockCh)

	select {
	case err := <-turnErrCh:
		if err != nil {
			t.Fatalf("turn failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for turn to finish")
	}

	close(stopReaders)
	wg.Wait()
}

func eToolCallStarted(e *sessionv1.SessionEvent) *sessionv1.ToolCall {
	if s := e.GetToolCallStarted(); s != nil {
		return s.ToolCall
	}
	return nil
}

func eToolCallFinished(e *sessionv1.SessionEvent) *sessionv1.ToolCallFinished {
	return e.GetToolCallFinished()
}

func newTestServer(t *testing.T, sb tools.Sandbox) (*Server, *session.FileStore) {
	t.Helper()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}

	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()

	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)
	if sb != nil {
		harness.SetSandboxOverride(sb)
	}

	store := session.NewFileStore(t.TempDir())

	srv, err := NewServer(ServerOptions{
		Store:          store,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv, store
}

func TestWebMethods_CreateListAndGet(t *testing.T) {
	ctx := t.Context()
	srv, _ := newTestServer(t, nil)

	if err := srv.CreateNewSession(ctx, "alpha", "basic"); err != nil {
		t.Fatalf("CreateNewSession(alpha): %v", err)
	}
	if err := srv.CreateNewSession(ctx, "beta", "basic"); err != nil {
		t.Fatalf("CreateNewSession(beta): %v", err)
	}

	summaries, err := srv.ListSessionSummaries(ctx)
	if err != nil {
		t.Fatalf("ListSessionSummaries: %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("got %d summaries, want 2", len(summaries))
	}
	if summaries[0].Name != "beta" || summaries[1].Name != "alpha" {
		t.Errorf("unexpected order: %s, %s", summaries[0].Name, summaries[1].Name)
	}

	detail, err := srv.GetSessionDetail(ctx, "alpha")
	if err != nil {
		t.Fatalf("GetSessionDetail(alpha): %v", err)
	}
	if detail.Summary.Name != "alpha" {
		t.Errorf("detail name = %q, want alpha", detail.Summary.Name)
	}
	if detail.Summary.InFlight {
		t.Errorf("detail InFlight = true, want false")
	}
}

func TestWebMethods_StartTurnAndBusy(t *testing.T) {
	ctx := t.Context()
	sb := &blockingSandbox{
		startedCh: make(chan struct{}, 1),
		blockCh:   make(chan struct{}),
	}
	srv, _ := newTestServer(t, sb)

	if err := srv.CreateNewSession(ctx, "turnsess", "basic"); err != nil {
		t.Fatalf("CreateNewSession: %v", err)
	}

	if err := srv.StartTurn(ctx, "turnsess", "run: uname -a"); err != nil {
		t.Fatalf("StartTurn: %v", err)
	}

	// Wait until sandbox starts execution
	select {
	case <-sb.startedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn to start")
	}

	// Verify session is marked busy immediately
	detail, err := srv.GetSessionDetail(ctx, "turnsess")
	if err != nil {
		t.Fatalf("GetSessionDetail: %v", err)
	}
	if !detail.Summary.InFlight {
		t.Errorf("InFlight = false, want true")
	}

	// A second StartTurn on a busy session must return ErrBusy
	err = srv.StartTurn(ctx, "turnsess", "second message")
	if !errors.Is(err, ErrBusy) {
		t.Errorf("second StartTurn err = %v, want ErrBusy", err)
	}

	// Unblock turn execution
	close(sb.blockCh)

	// Poll until turn finishes
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d, err := srv.GetSessionDetail(ctx, "turnsess")
		if err == nil && !d.Summary.InFlight {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	detail, err = srv.GetSessionDetail(ctx, "turnsess")
	if err != nil {
		t.Fatalf("GetSessionDetail: %v", err)
	}
	if detail.Summary.InFlight {
		t.Fatal("turn still in flight after unblocking")
	}
	if len(detail.Messages) == 0 {
		t.Error("expected messages in detail after turn finished")
	}
}

func TestListRepositoriesAndGoals(t *testing.T) {
	ctx := t.Context()
	stateStore := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	srv, err := NewServer(ServerOptions{
		Store:          sessionStore,
		StateStore:     stateStore,
		DefaultToolset: "basic",
		HarnessFactory: func(_ string) (*session.Harness, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
	})

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	client := sessionv1.NewSessionServiceClient(conn)

	// List on empty store
	reposResp, err := client.ListRepositories(ctx, &sessionv1.ListRepositoriesRequest{})
	if err != nil {
		t.Fatalf("ListRepositories empty: %v", err)
	}
	if len(reposResp.Repositories) != 0 {
		t.Fatalf("expected 0 repos, got %d", len(reposResp.Repositories))
	}

	// Add a repo and a goal
	key := state.RepoKey{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "test-repo",
	}
	repo := &state.Repo{
		URL:            "https://github.com/gke-labs/test-repo",
		Host:           key.Host,
		Owner:          key.Owner,
		Name:           key.Name,
		DefaultBranch:  "main",
		ForgeKind:      "gitremote",
		RegisteredTime: time.Now(),
		HeadCommit:     "c0ffee",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	bestVal := 42.5
	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "benchmark",
			Body:     "Speed up sorting",
			Schedule: "daily",
			Verify:   "go test ./...",
		},
		Hash:       "hash123",
		BaseCommit: "c0ffee",
		BestValue:  &bestVal,
		LastRunID:  "20261029T120000Z-abcd",
	}
	if err := stateStore.PutGoal(ctx, key, goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	// ListRepositories should now return 1 repo
	reposResp, err = client.ListRepositories(ctx, &sessionv1.ListRepositoriesRequest{})
	if err != nil {
		t.Fatalf("ListRepositories: %v", err)
	}
	if len(reposResp.Repositories) != 1 {
		t.Fatalf("expected 1 repo, got %d", len(reposResp.Repositories))
	}
	if reposResp.Repositories[0].Url != repo.URL || reposResp.Repositories[0].HeadCommit != "c0ffee" {
		t.Errorf("unexpected repo: %+v", reposResp.Repositories[0])
	}

	// ListGoals should return the goal
	goalsResp, err := client.ListGoals(ctx, &sessionv1.ListGoalsRequest{RepoUrl: repo.URL})
	if err != nil {
		t.Fatalf("ListGoals: %v", err)
	}
	if len(goalsResp.Goals) != 1 {
		t.Fatalf("expected 1 goal, got %d", len(goalsResp.Goals))
	}
	g := goalsResp.Goals[0]
	if g.Name != "benchmark" || g.Hash != "hash123" || g.BaseCommit != "c0ffee" || g.BestValue != "42.5" {
		t.Errorf("unexpected goal: %+v", g)
	}

	// ListRuns with empty repo_url should fail
	if _, err := client.ListRuns(ctx, &sessionv1.ListRunsRequest{RepoUrl: ""}); err == nil {
		t.Error("expected error for empty repo_url")
	}

	// ListRuns should be empty initially
	runsResp, err := client.ListRuns(ctx, &sessionv1.ListRunsRequest{RepoUrl: repo.URL})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runsResp.Runs) != 0 {
		t.Fatalf("expected 0 runs, got %d", len(runsResp.Runs))
	}

	// Create a run in stateStore
	runID := "20261029T120000Z-abcd"
	run := &state.Run{
		ID:         runID,
		Repo:       key,
		GoalName:   "benchmark",
		Phase:      state.PhasePending,
		BaseCommit: "c0ffee",
	}
	if err := stateStore.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	runsResp, err = client.ListRuns(ctx, &sessionv1.ListRunsRequest{RepoUrl: repo.URL})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runsResp.Runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runsResp.Runs))
	}
	r := runsResp.Runs[0]
	if r.Id != runID || r.GoalName != "benchmark" || r.Phase != string(state.PhasePending) || r.BaseCommit != "c0ffee" {
		t.Errorf("unexpected run: %+v", r)
	}

	// Test GetRun with empty ID
	if _, err := client.GetRun(ctx, &sessionv1.GetRunRequest{Id: ""}); err == nil {
		t.Error("expected error for empty id in GetRun")
	}

	// Test GetRun with non-existent ID
	if _, err := client.GetRun(ctx, &sessionv1.GetRunRequest{Id: "nonexistent"}); err == nil {
		t.Error("expected error for nonexistent run in GetRun")
	}

	// Add event to run
	now := time.Now().Truncate(time.Second)
	event := state.Event{
		Time:    now,
		Phase:   state.PhasePending,
		Kind:    "created",
		Message: "Run created",
	}
	if err := stateStore.AppendEvent(ctx, key, runID, event); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	// Test GetRun for existing run
	getRunResp, err := client.GetRun(ctx, &sessionv1.GetRunRequest{Id: runID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if getRunResp.Run == nil {
		t.Fatal("expected run record in GetRun response")
	}
	if getRunResp.Run.Id != runID || getRunResp.Run.GoalName != "benchmark" || getRunResp.Run.RepoUrl != repo.URL {
		t.Errorf("unexpected run in GetRun: %+v", getRunResp.Run)
	}
	if len(getRunResp.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(getRunResp.Events))
	}
	ev := getRunResp.Events[0]
	if ev.Kind != "created" || ev.Message != "Run created" || ev.Phase != string(state.PhasePending) {
		t.Errorf("unexpected event in GetRun: %+v", ev)
	}
}

func TestRunSessionsTreeAndSideBySideRuns(t *testing.T) {
	ctx := t.Context()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}
	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}
	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()
	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)

	sessionStore := session.NewFileStore(t.TempDir())
	stateStore := state.NewFileStore(t.TempDir())

	srv, err := NewServer(ServerOptions{
		Store:          sessionStore,
		StateStore:     stateStore,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	repoKey := state.RepoKey{Host: "github.com", Owner: "octocat", Name: "myrepo"}
	if err := stateStore.PutRepo(ctx, &state.Repo{
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		URL:           "https://github.com/octocat/myrepo",
		DefaultBranch: "main",
	}); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	// 1. Setup Run 1 with top-level session created the way the orchestrator does (with run and repo in metadata).
	runID1 := "20261121T100000Z-0001"
	parent1 := "run1parent"
	if err := stateStore.PutRun(ctx, &state.Run{
		ID:        runID1,
		Repo:      repoKey,
		GoalName:  "optimize",
		Phase:     state.PhaseWorking,
		SessionID: parent1,
	}); err != nil {
		t.Fatalf("PutRun(1): %v", err)
	}
	if err := sessionStore.NewSession(ctx, parent1, session.Metadata{
		Repo:      repoKey.String(),
		RunID:     runID1,
		Toolset:   "basic",
		CreatedAt: time.Now().Add(-10 * time.Minute),
	}); err != nil {
		t.Fatalf("NewSession(%s): %v", parent1, err)
	}

	// Create two children and one grandchild for Run 1.
	child1a := "run1childa"
	child1b := "run1childb"
	grandchild1 := "run1grandchild"
	for _, req := range []*sessionv1.CreateSessionRequest{
		{Name: child1a, Parent: parent1, Toolset: "basic"},
		{Name: child1b, Parent: parent1, Toolset: "basic"},
		{Name: grandchild1, Parent: child1a, Toolset: "basic"},
	} {
		if _, err := srv.CreateSession(ctx, req); err != nil {
			t.Fatalf("CreateSession(%s): %v", req.Name, err)
		}
	}

	// 2. Setup Run 2 side-by-side with Run 1, with its own top-level session and child.
	runID2 := "20261121T110000Z-0002"
	parent2 := "run2parent"
	if err := stateStore.PutRun(ctx, &state.Run{
		ID:        runID2,
		Repo:      repoKey,
		GoalName:  "lint",
		Phase:     state.PhaseWorking,
		SessionID: parent2,
	}); err != nil {
		t.Fatalf("PutRun(2): %v", err)
	}
	if err := sessionStore.NewSession(ctx, parent2, session.Metadata{
		Repo:      repoKey.String(),
		RunID:     runID2,
		Toolset:   "basic",
		CreatedAt: time.Now().Add(-5 * time.Minute),
	}); err != nil {
		t.Fatalf("NewSession(%s): %v", parent2, err)
	}

	child2a := "run2childa"
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    child2a,
		Parent:  parent2,
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(%s): %v", child2a, err)
	}

	// 3. Setup a session with no run, and a child of that session (parent with no run gives child with no run).
	noRunParent := "sessionnorun"
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    noRunParent,
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(%s): %v", noRunParent, err)
	}

	noRunChild := "norunchild"
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    noRunChild,
		Parent:  noRunParent,
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(%s): %v", noRunChild, err)
	}

	// Verify metadata persisted for noRunChild: parent recorded, but no RunID or Repo.
	_, noRunChildMeta, err := sessionStore.LoadSession(ctx, noRunChild)
	if err != nil {
		t.Fatalf("LoadSession(%s): %v", noRunChild, err)
	}
	if noRunChildMeta.Parent != noRunParent {
		t.Errorf("noRunChild Parent = %q, want %q", noRunChildMeta.Parent, noRunParent)
	}
	if noRunChildMeta.RunID != "" {
		t.Errorf("noRunChild RunID = %q, want empty", noRunChildMeta.RunID)
	}
	if noRunChildMeta.Repo != "" {
		t.Errorf("noRunChild Repo = %q, want empty", noRunChildMeta.Repo)
	}

	// 4. Verify ListRunSessions for Run 1: exactly 4 sessions, only from Run 1, ordered by depth.
	sessions1, err := srv.ListRunSessions(ctx, runID1)
	if err != nil {
		t.Fatalf("ListRunSessions(run1): %v", err)
	}
	if len(sessions1) != 4 {
		t.Fatalf("ListRunSessions(run1) returned %d sessions, want 4", len(sessions1))
	}
	if sessions1[0].Name != parent1 || sessions1[0].Depth != 0 || sessions1[0].Parent != "" || sessions1[0].RunID != runID1 {
		t.Errorf("unexpected summary for parent1: %+v", sessions1[0])
	}
	if sessions1[1].Depth != 1 || sessions1[2].Depth != 1 {
		t.Errorf("expected depth 1 for items 1 and 2, got %d and %d", sessions1[1].Depth, sessions1[2].Depth)
	}
	if sessions1[3].Name != grandchild1 || sessions1[3].Depth != 2 || sessions1[3].Parent != child1a || sessions1[3].RunID != runID1 {
		t.Errorf("unexpected summary for grandchild1: %+v", sessions1[3])
	}

	// 5. Verify ListRunSessions for Run 2: exactly 2 sessions, only from Run 2.
	sessions2, err := srv.ListRunSessions(ctx, runID2)
	if err != nil {
		t.Fatalf("ListRunSessions(run2): %v", err)
	}
	if len(sessions2) != 2 {
		t.Fatalf("ListRunSessions(run2) returned %d sessions, want 2", len(sessions2))
	}
	if sessions2[0].Name != parent2 || sessions2[0].Depth != 0 || sessions2[0].RunID != runID2 {
		t.Errorf("unexpected summary for parent2: %+v", sessions2[0])
	}
	if sessions2[1].Name != child2a || sessions2[1].Depth != 1 || sessions2[1].Parent != parent2 || sessions2[1].RunID != runID2 {
		t.Errorf("unexpected summary for child2a: %+v", sessions2[1])
	}

	// 6. Verify SessionDepth for individual sessions.
	for _, tc := range []struct {
		name      string
		wantDepth int
	}{
		{parent1, 0},
		{child1a, 1},
		{child1b, 1},
		{grandchild1, 2},
		{parent2, 0},
		{child2a, 1},
		{noRunParent, 0},
		{noRunChild, 1},
	} {
		depth, err := srv.SessionDepth(ctx, tc.name)
		if err != nil {
			t.Errorf("SessionDepth(%s) error: %v", tc.name, err)
		}
		if depth != tc.wantDepth {
			t.Errorf("SessionDepth(%s) = %d, want %d", tc.name, depth, tc.wantDepth)
		}
	}

	// 7. Verify ListRunSessions with empty or nonexistent run ID.
	if _, err := srv.ListRunSessions(ctx, ""); err == nil {
		t.Error("ListRunSessions with empty runID should fail")
	}
	unknown, err := srv.ListRunSessions(ctx, "nonexistent-run")
	if err != nil {
		t.Fatalf("ListRunSessions(nonexistent): %v", err)
	}
	if len(unknown) != 0 {
		t.Errorf("expected 0 sessions for nonexistent run, got %d", len(unknown))
	}
}

func TestCreateSessionAndSessionDepth_Errors(t *testing.T) {
	ctx := t.Context()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}
	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}
	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()
	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)

	storeDir := t.TempDir()
	sessionStore := session.NewFileStore(storeDir)

	srv, err := NewServer(ServerOptions{
		Store:          sessionStore,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// 1. Missing parent on CreateSession returns NotFound.
	_, err = srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    "orphan",
		Parent:  "doesnotexist",
		Toolset: "basic",
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("CreateSession with missing parent got code %v, want NotFound (%v)", status.Code(err), err)
	}

	// 2. Unreadable/corrupted parent metadata returns Internal.
	corruptDir := filepath.Join(storeDir, "corrupt")
	if err := os.MkdirAll(corruptDir, 0700); err != nil {
		t.Fatalf("MkdirAll(corrupt): %v", err)
	}
	if err := os.WriteFile(filepath.Join(corruptDir, "metadata.json"), []byte("{not-valid-json"), 0600); err != nil {
		t.Fatalf("WriteFile(corrupt metadata): %v", err)
	}

	_, err = srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    "childofcorrupt",
		Parent:  "corrupt",
		Toolset: "basic",
	})
	if status.Code(err) != codes.Internal {
		t.Errorf("CreateSession with unreadable parent got code %v, want Internal (%v)", status.Code(err), err)
	}

	// 3. SessionDepth on nonexistent session returns NotFound.
	if _, err := srv.SessionDepth(ctx, "doesnotexist"); status.Code(err) != codes.NotFound {
		t.Errorf("SessionDepth on nonexistent session got code %v, want NotFound (%v)", status.Code(err), err)
	}

	// 4. SessionDepth on invalid session name returns InvalidArgument.
	if _, err := srv.SessionDepth(ctx, "invalid name!"); status.Code(err) != codes.InvalidArgument {
		t.Errorf("SessionDepth on invalid session name got code %v, want InvalidArgument (%v)", status.Code(err), err)
	}
}

func TestSessionAncestryAndMissingAncestor(t *testing.T) {
	ctx := t.Context()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}
	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}
	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()
	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)

	storeDir := t.TempDir()
	sessionStore := session.NewFileStore(storeDir)

	srv, err := NewServer(ServerOptions{
		Store:          sessionStore,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// 1. Top-level session: depth 0, empty ancestors.
	topLevel := "topsession"
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    topLevel,
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(top): %v", err)
	}

	_, topMeta, err := sessionStore.LoadSession(ctx, topLevel)
	if err != nil {
		t.Fatalf("LoadSession(top): %v", err)
	}
	if len(topMeta.Ancestors) != 0 {
		t.Errorf("top-level Ancestors = %v, want empty", topMeta.Ancestors)
	}
	if topMeta.Parent != "" {
		t.Errorf("top-level Parent = %q, want empty", topMeta.Parent)
	}

	topDepth, err := srv.SessionDepth(ctx, topLevel)
	if err != nil {
		t.Fatalf("SessionDepth(top): %v", err)
	}
	if topDepth != 0 {
		t.Errorf("SessionDepth(top) = %d, want 0", topDepth)
	}

	topDetail, err := srv.GetSessionDetail(ctx, topLevel)
	if err != nil {
		t.Fatalf("GetSessionDetail(top): %v", err)
	}
	if topDetail.Summary.Depth != 0 {
		t.Errorf("GetSessionDetail(top) depth = %d, want 0", topDetail.Summary.Depth)
	}
	if len(topDetail.Summary.Ancestors) != 0 {
		t.Errorf("GetSessionDetail(top) ancestors = %v, want empty", topDetail.Summary.Ancestors)
	}

	// 2. Child session: depth 1, ancestors [topLevel].
	child := "childsession"
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    child,
		Parent:  topLevel,
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(child): %v", err)
	}

	_, childMeta, err := sessionStore.LoadSession(ctx, child)
	if err != nil {
		t.Fatalf("LoadSession(child): %v", err)
	}
	wantChildAncestors := []string{topLevel}
	if !slices.Equal(childMeta.Ancestors, wantChildAncestors) {
		t.Errorf("child Ancestors = %v, want %v", childMeta.Ancestors, wantChildAncestors)
	}
	if childMeta.Parent != topLevel {
		t.Errorf("child Parent = %q, want %q", childMeta.Parent, topLevel)
	}

	childDepth, err := srv.SessionDepth(ctx, child)
	if err != nil {
		t.Fatalf("SessionDepth(child): %v", err)
	}
	if childDepth != 1 {
		t.Errorf("SessionDepth(child) = %d, want 1", childDepth)
	}

	childDetail, err := srv.GetSessionDetail(ctx, child)
	if err != nil {
		t.Fatalf("GetSessionDetail(child): %v", err)
	}
	if childDetail.Summary.Depth != 1 {
		t.Errorf("GetSessionDetail(child) depth = %d, want 1", childDetail.Summary.Depth)
	}
	if !slices.Equal(childDetail.Summary.Ancestors, wantChildAncestors) {
		t.Errorf("GetSessionDetail(child) ancestors = %v, want %v", childDetail.Summary.Ancestors, wantChildAncestors)
	}

	// 3. Grandchild session: depth 2, ancestors [topLevel, child].
	grandchild := "grandchildsession"
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    grandchild,
		Parent:  child,
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(grandchild): %v", err)
	}

	_, grandchildMeta, err := sessionStore.LoadSession(ctx, grandchild)
	if err != nil {
		t.Fatalf("LoadSession(grandchild): %v", err)
	}
	wantGrandchildAncestors := []string{topLevel, child}
	if !slices.Equal(grandchildMeta.Ancestors, wantGrandchildAncestors) {
		t.Errorf("grandchild Ancestors = %v, want %v", grandchildMeta.Ancestors, wantGrandchildAncestors)
	}
	if grandchildMeta.Parent != child {
		t.Errorf("grandchild Parent = %q, want %q", grandchildMeta.Parent, child)
	}

	grandchildDepth, err := srv.SessionDepth(ctx, grandchild)
	if err != nil {
		t.Fatalf("SessionDepth(grandchild): %v", err)
	}
	if grandchildDepth != 2 {
		t.Errorf("SessionDepth(grandchild) = %d, want 2", grandchildDepth)
	}

	grandchildDetail, err := srv.GetSessionDetail(ctx, grandchild)
	if err != nil {
		t.Fatalf("GetSessionDetail(grandchild): %v", err)
	}
	if grandchildDetail.Summary.Depth != 2 {
		t.Errorf("GetSessionDetail(grandchild) depth = %d, want 2", grandchildDetail.Summary.Depth)
	}
	if !slices.Equal(grandchildDetail.Summary.Ancestors, wantGrandchildAncestors) {
		t.Errorf("GetSessionDetail(grandchild) ancestors = %v, want %v", grandchildDetail.Summary.Ancestors, wantGrandchildAncestors)
	}

	// 4. Session whose ancestors no longer exist.
	// Delete ancestor sessions topLevel and child from the underlying store.
	if err := os.RemoveAll(filepath.Join(storeDir, topLevel)); err != nil {
		t.Fatalf("RemoveAll(top): %v", err)
	}
	if err := os.RemoveAll(filepath.Join(storeDir, child)); err != nil {
		t.Fatalf("RemoveAll(child): %v", err)
	}

	// Create a new server without activeSessions in memory to ensure loading from store.
	srvFromDisk, err := NewServer(ServerOptions{
		Store:          sessionStore,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("NewServer(fromDisk): %v", err)
	}

	// SessionDepth and GetSessionDetail must succeed even though ancestors were deleted.
	missingAncestorDepth, err := srvFromDisk.SessionDepth(ctx, grandchild)
	if err != nil {
		t.Fatalf("SessionDepth with missing ancestors should succeed, got error: %v", err)
	}
	if missingAncestorDepth != 2 {
		t.Errorf("SessionDepth with missing ancestors = %d, want 2", missingAncestorDepth)
	}

	missingAncestorDetail, err := srvFromDisk.GetSessionDetail(ctx, grandchild)
	if err != nil {
		t.Fatalf("GetSessionDetail with missing ancestors should succeed, got error: %v", err)
	}
	if missingAncestorDetail.Summary.Depth != 2 {
		t.Errorf("GetSessionDetail with missing ancestors depth = %d, want 2", missingAncestorDetail.Summary.Depth)
	}
	if !slices.Equal(missingAncestorDetail.Summary.Ancestors, wantGrandchildAncestors) {
		t.Errorf("GetSessionDetail with missing ancestors ancestors = %v, want %v", missingAncestorDetail.Summary.Ancestors, wantGrandchildAncestors)
	}

	// 5. Legacy session written before this change (only Parent recorded, no Ancestors).
	legacyChildDir := filepath.Join(storeDir, "legacychild")
	if err := os.MkdirAll(legacyChildDir, 0700); err != nil {
		t.Fatalf("MkdirAll(legacychild): %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyChildDir, "metadata.json"), []byte(`{"parent":"missingancestor","toolset":"basic"}`), 0600); err != nil {
		t.Fatalf("WriteFile(legacychild): %v", err)
	}

	// Legacy child still loads.
	legacyDetail, err := srvFromDisk.GetSessionDetail(ctx, "legacychild")
	if err != nil {
		t.Fatalf("GetSessionDetail(legacychild) should succeed, got error: %v", err)
	}
	if legacyDetail.Summary.Depth != 1 {
		t.Errorf("legacychild depth = %d, want 1", legacyDetail.Summary.Depth)
	}
	if legacyDetail.Summary.Parent != "missingancestor" {
		t.Errorf("legacychild parent = %q, want missingancestor", legacyDetail.Summary.Parent)
	}

	legacyDepth, err := srvFromDisk.SessionDepth(ctx, "legacychild")
	if err != nil {
		t.Fatalf("SessionDepth(legacychild) should succeed, got error: %v", err)
	}
	if legacyDepth != 1 {
		t.Errorf("SessionDepth(legacychild) = %d, want 1", legacyDepth)
	}

	// Legacy child still lists.
	summaries, err := srvFromDisk.ListSessionSummaries(ctx)
	if err != nil {
		t.Fatalf("ListSessionSummaries: %v", err)
	}
	var foundLegacy bool
	for _, sum := range summaries {
		if sum.Name == "legacychild" {
			foundLegacy = true
			if sum.Depth != 1 {
				t.Errorf("legacychild summary depth = %d, want 1", sum.Depth)
			}
			if sum.Parent != "missingancestor" {
				t.Errorf("legacychild summary parent = %q, want missingancestor", sum.Parent)
			}
		}
	}
	if !foundLegacy {
		t.Error("legacychild not found in ListSessionSummaries")
	}

	// 6. Legacy top-level session (Parent is empty, Ancestors empty).
	legacyTopDir := filepath.Join(storeDir, "legacytop")
	if err := os.MkdirAll(legacyTopDir, 0700); err != nil {
		t.Fatalf("MkdirAll(legacytop): %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyTopDir, "metadata.json"), []byte(`{"toolset":"basic"}`), 0600); err != nil {
		t.Fatalf("WriteFile(legacytop): %v", err)
	}
	legacyTopDetail, err := srvFromDisk.GetSessionDetail(ctx, "legacytop")
	if err != nil {
		t.Fatalf("GetSessionDetail(legacytop) should succeed, got error: %v", err)
	}
	if legacyTopDetail.Summary.Depth != 0 {
		t.Errorf("legacytop depth = %d, want 0", legacyTopDetail.Summary.Depth)
	}
	legacyTopDepth, err := srvFromDisk.SessionDepth(ctx, "legacytop")
	if err != nil {
		t.Fatalf("SessionDepth(legacytop) should succeed, got error: %v", err)
	}
	if legacyTopDepth != 0 {
		t.Errorf("SessionDepth(legacytop) = %d, want 0", legacyTopDepth)
	}
}
