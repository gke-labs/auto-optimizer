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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

// recordingSandbox is an in-memory double of tools.Sandbox that records
// commands executed against it without requiring a Kubernetes cluster or network.
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

type recordingEvents struct {
	assistantMessages []string
	approvedCalls     []llm.ToolCall
}

func (e *recordingEvents) AssistantMessage(_ context.Context, text string) {
	e.assistantMessages = append(e.assistantMessages, text)
}

func (e *recordingEvents) ApproveToolCall(_ context.Context, call llm.ToolCall) (bool, error) {
	e.approvedCalls = append(e.approvedCalls, call)
	return true, nil
}

func (e *recordingEvents) ToolCallStarted(_ context.Context, _ llm.ToolCall) {}

func (e *recordingEvents) ToolCallFinished(_ context.Context, _ llm.ToolCall, _ error) {}

func (e *recordingEvents) MessageAppended(_ context.Context, _ llm.Message) {}

func TestOneShot_FakeEliza(t *testing.T) {
	ctx := t.Context()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("NewFromEnv(fake-eliza): %v", err)
	}

	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get(basic): %v", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()

	fakeSB := &recordingSandbox{}

	harness := NewHarness(llmClient, nil, toolsRegistry, opts)
	harness.SetSandboxOverride(fakeSB)

	storeDir := t.TempDir()
	sessionStore := NewFileStore(storeDir)

	sess, err := harness.NewSession(ctx, sessionStore, "oneshottest", Metadata{Toolset: "basic"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
		t.Fatalf("EnsureSystemPrompt: %v", err)
	}

	events := &recordingEvents{}
	prompt := "run: uname -a"
	if err := harness.RunTurn(ctx, sess, prompt, events); err != nil {
		t.Fatalf("RunTurn failed: %v", err)
	}

	// Verify fake sandbox recorded the command
	fakeSB.mu.Lock()
	callCount := len(fakeSB.calls)
	var lastCmd []string
	if callCount > 0 {
		lastCmd = fakeSB.calls[0].Command
	}
	fakeSB.mu.Unlock()

	if callCount != 1 {
		t.Fatalf("expected 1 command executed in sandbox, got %d", callCount)
	}
	expectedCmd := []string{"sh", "-c", "uname -a"}
	if !slices.Equal(lastCmd, expectedCmd) {
		t.Errorf("command = %v, want %v", lastCmd, expectedCmd)
	}

	// Verify tool call approval
	if len(events.approvedCalls) != 1 {
		t.Fatalf("expected 1 approved tool call, got %d", len(events.approvedCalls))
	}
	if events.approvedCalls[0].Function.Name != "run_command" {
		t.Errorf("approved tool name = %q, want run_command", events.approvedCalls[0].Function.Name)
	}

	// Verify assistant messages contain "Linux"
	foundLinux := false
	for _, msg := range events.assistantMessages {
		if strings.Contains(msg, "Linux") {
			foundLinux = true
			break
		}
	}
	if !foundLinux {
		t.Errorf("assistant messages %v did not contain 'Linux'", events.assistantMessages)
	}

	// Verify session history
	msgs := sess.Messages()
	if len(msgs) < 3 {
		t.Fatalf("expected at least 3 messages in history (user, tool call, tool result, assistant), got %d", len(msgs))
	}
}
