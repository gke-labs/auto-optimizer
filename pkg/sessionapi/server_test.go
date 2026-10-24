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
	"strings"
	"sync"
	"testing"
	"time"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
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

	// Create a new session.
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
