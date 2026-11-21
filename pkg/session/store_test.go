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
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/google/go-cmp/cmp"
)

func TestFileStore(t *testing.T) {
	ctx := t.Context()
	tempDir, err := os.MkdirTemp("", "sessions-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFileStore(tempDir)

	sessionID := "testsession123"

	// 1. Load non-existent session
	msgs, meta, err := store.LoadSession(ctx, sessionID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadSession for non-existent session: got err %v, want ErrNotFound", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected empty messages, got %d", len(msgs))
	}
	if meta != nil {
		t.Errorf("expected nil metadata for non-existent session, got %+v", meta)
	}

	// 2. Append a message
	sysPrompt := "You are a helpful assistant."
	msg1 := llm.Message{
		Role:    "system",
		Content: &sysPrompt,
	}
	if err := store.AppendMessages(ctx, sessionID, msg1); err != nil {
		t.Fatalf("AppendMessage failed: %v", err)
	}

	// 3. Load and assert msg1
	msgs, meta, err = store.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession failed: %v", err)
	}
	if diff := cmp.Diff([]llm.Message{msg1}, msgs); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%s", diff)
	}
	if meta == nil || !meta.IsZero() {
		t.Errorf("expected empty metadata when only messages exist, got %+v", meta)
	}

	// 4. Append more messages (user and assistant with tool calls)
	userPrompt := "list files"
	msg2 := llm.Message{
		Role:    "user",
		Content: &userPrompt,
	}
	if err := store.AppendMessages(ctx, sessionID, msg2); err != nil {
		t.Fatalf("AppendMessage failed: %v", err)
	}

	toolCall := llm.ToolCall{
		ID:   "call_1",
		Type: "function",
		Function: llm.FunctionCall{
			Name:      "list_files",
			Arguments: `{"path": "."}`,
		},
	}
	msg3 := llm.Message{
		Role:      "assistant",
		ToolCalls: []llm.ToolCall{toolCall},
	}

	toolResult := "file1.txt\nfile2.txt"
	msg4 := llm.Message{
		Role:       "tool",
		ToolCallID: "call_1",
		Content:    &toolResult,
	}
	if err := store.AppendMessages(ctx, sessionID, msg3, msg4); err != nil {
		t.Fatalf("AppendMessage failed: %v", err)
	}

	// 5. Load and assert everything
	msgs, meta, err = store.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession failed: %v", err)
	}
	wantMsgs := []llm.Message{msg1, msg2, msg3, msg4}
	if diff := cmp.Diff(wantMsgs, msgs); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%s", diff)
	}
	if meta == nil || !meta.IsZero() {
		t.Errorf("expected empty metadata when only messages exist, got %+v", meta)
	}
}

func TestFileStoreMetadata(t *testing.T) {
	ctx := t.Context()
	store := NewFileStore(t.TempDir())

	// A legacy session (messages recorded before metadata existed) returns an
	// empty Metadata from LoadSession.
	content := "hi"
	if err := store.AppendMessages(ctx, "legacy", llm.Message{Role: "user", Content: &content}); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}
	_, legacyMeta, err := store.LoadSession(ctx, "legacy")
	if err != nil {
		t.Fatalf("LoadSession(legacy): %v", err)
	}
	if legacyMeta == nil || !legacyMeta.IsZero() {
		t.Errorf("LoadSession(legacy) metadata = %+v, want empty Metadata", legacyMeta)
	}

	want := Metadata{
		Parent:    "parent1",
		Toolset:   "geminicli",
		Repo:      "github.com/gke-labs/auto-optimizer",
		RunID:     "20261021T180000Z-0001",
		CreatedAt: time.Date(2026, 10, 21, 18, 0, 0, 0, time.UTC),
	}

	// Reusing a session ID that already has messages must fail.
	if err := store.NewSession(ctx, "legacy", want); err == nil {
		t.Error("NewSession on existing legacy session should fail")
	}

	if err := store.NewSession(ctx, "child1", want); err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Loading a newly created session returns empty messages and the stored
	// metadata.
	msgs, gotMeta, err := store.LoadSession(ctx, "child1")
	if err != nil {
		t.Fatalf("LoadSession after NewSession: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("LoadSession after NewSession messages = %v, want empty", msgs)
	}
	if diff := cmp.Diff(want, *gotMeta); diff != "" {
		t.Errorf("LoadSession metadata mismatch (-want +got):\n%s", diff)
	}

	// Reusing a session ID that already has metadata must fail.
	if err := store.NewSession(ctx, "child1", want); err == nil {
		t.Error("NewSession with a reused session ID should fail")
	}

	if err := store.NewSession(ctx, "Not Valid", want); err == nil {
		t.Error("NewSession with an invalid session name should fail")
	}
}

func TestFileStoreListSessions(t *testing.T) {
	ctx := t.Context()
	store := NewFileStore(t.TempDir())

	// Listing an empty store returns empty slice.
	list, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions on empty store: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("ListSessions on empty store got %d items, want 0", len(list))
	}

	meta1 := Metadata{
		Parent:    "",
		Toolset:   "basic",
		CreatedAt: time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC),
	}
	if err := store.NewSession(ctx, "sess1", meta1); err != nil {
		t.Fatalf("NewSession sess1: %v", err)
	}
	content := "hello"
	if err := store.AppendMessages(ctx, "sess1", llm.Message{Role: "user", Content: &content}); err != nil {
		t.Fatalf("AppendMessages sess1: %v", err)
	}

	meta2 := Metadata{
		Parent:    "sess1",
		Toolset:   "geminicli",
		CreatedAt: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC),
	}
	if err := store.NewSession(ctx, "sess2", meta2); err != nil {
		t.Fatalf("NewSession sess2: %v", err)
	}

	list, err = store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListSessions got %d items, want 2", len(list))
	}

	if list[0].Name != "sess1" || list[0].MessageCount != 1 || list[0].Metadata.Toolset != "basic" {
		t.Errorf("list[0] = %+v, want sess1 with 1 message", list[0])
	}
	if list[1].Name != "sess2" || list[1].MessageCount != 0 || list[1].Metadata.Parent != "sess1" {
		t.Errorf("list[1] = %+v, want sess2 with 0 messages", list[1])
	}
}

func TestSession_CutOffRecovery(t *testing.T) {
	ctx := t.Context()
	store := NewFileStore(t.TempDir())
	sessionID := "sesscutoff"

	content1 := "message 1"
	content2 := "message 2"
	content3 := "message 3"
	content4 := "message 4"
	m1 := llm.Message{Role: "user", Content: &content1}
	m2 := llm.Message{Role: "assistant", Content: &content2}
	m3 := llm.Message{Role: "user", Content: &content3}
	m4 := llm.Message{Role: "assistant", Content: &content4}

	if err := store.AppendMessages(ctx, sessionID, m1, m2, m3); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}
	path := store.messagesPath(sessionID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	n0 := bytes.IndexByte(data, '\n')
	n1 := bytes.IndexByte(data[n0+1:], '\n') + n0 + 1
	n2 := bytes.IndexByte(data[n1+1:], '\n') + n1 + 1
	len2 := n2 + 1

	// Cut the file at every byte offset inside the third record, check that the
	// reader returns the first two records, then append a record and read back
	// three complete records.
	for cut := len2; cut < len(data); cut++ {
		if err := os.WriteFile(path, data[:cut], 0600); err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		got, _, err := store.LoadSession(ctx, sessionID)
		if err != nil {
			t.Fatalf("cut at %d: LoadSession failed: %v", cut, err)
		}
		if len(got) != 2 || *got[0].Content != content1 || *got[1].Content != content2 {
			t.Fatalf("cut at %d: expected first 2 messages, got %+v", cut, got)
		}
		if err := store.AppendMessages(ctx, sessionID, m4); err != nil {
			t.Fatalf("cut at %d: AppendMessages(m4) failed: %v", cut, err)
		}
		got3, _, err := store.LoadSession(ctx, sessionID)
		if err != nil {
			t.Fatalf("cut at %d: LoadSession after append failed: %v", cut, err)
		}
		// If cut was before the closing bracket of m3, m3 was incomplete and is skipped,
		// leaving [m1, m2, m4]. If cut was after the closing bracket (missing only the newline),
		// the appended newline completes m3, giving [m1, m2, m3, m4].
		if cut < len(data)-1 {
			if len(got3) != 3 || *got3[0].Content != content1 || *got3[1].Content != content2 || *got3[2].Content != content4 {
				t.Fatalf("cut at %d: expected [m1, m2, m4], got %+v", cut, got3)
			}
		} else {
			if len(got3) != 4 || *got3[0].Content != content1 || *got3[1].Content != content2 || *got3[2].Content != content3 || *got3[3].Content != content4 {
				t.Fatalf("cut at %d: expected [m1, m2, m3, m4], got %+v", cut, got3)
			}
		}
	}

	// A damaged record anywhere in the file is skipped and logged.
	damagedID := "sessdamaged"
	if err := store.ensureDir(damagedID); err != nil {
		t.Fatalf("ensureDir: %v", err)
	}
	damagedPath := store.messagesPath(damagedID)
	damagedContent := `{"role":"user","content":"first"}` + "\n" + `{"bad json` + "\n" + `{"role":"assistant","content":"third"}` + "\n"
	if err := os.WriteFile(damagedPath, []byte(damagedContent), 0600); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	gotDamaged, _, err := store.LoadSession(ctx, damagedID)
	if err != nil {
		t.Fatalf("LoadSession with damaged record: %v", err)
	}
	if len(gotDamaged) != 2 || *gotDamaged[0].Content != "first" || *gotDamaged[1].Content != "third" {
		t.Fatalf("expected [first, third], got %+v", gotDamaged)
	}

	// Two appends in a row with no crash read back with no extra records.
	normalID := "sessnormal"
	if err := store.AppendMessages(ctx, normalID, m1, m2); err != nil {
		t.Fatalf("AppendMessages(m1, m2): %v", err)
	}
	if err := store.AppendMessages(ctx, normalID, m3, m4); err != nil {
		t.Fatalf("AppendMessages(m3, m4): %v", err)
	}
	gotNormal, _, err := store.LoadSession(ctx, normalID)
	if err != nil {
		t.Fatalf("LoadSession(normal): %v", err)
	}
	if len(gotNormal) != 4 || *gotNormal[0].Content != content1 || *gotNormal[1].Content != content2 || *gotNormal[2].Content != content3 || *gotNormal[3].Content != content4 {
		t.Fatalf("expected [m1, m2, m3, m4], got %+v", gotNormal)
	}
}

func TestMetadataDepth(t *testing.T) {
	ctx := t.Context()
	store := NewFileStore(t.TempDir())

	tests := []struct {
		name      string
		meta      Metadata
		wantDepth int
	}{
		{
			name:      "top-level session with empty parent and empty ancestors",
			meta:      Metadata{},
			wantDepth: 0,
		},
		{
			name:      "child session with ancestors",
			meta:      Metadata{Parent: "root", Ancestors: []string{"root"}},
			wantDepth: 1,
		},
		{
			name:      "grandchild session with ancestors",
			meta:      Metadata{Parent: "child", Ancestors: []string{"root", "child"}},
			wantDepth: 2,
		},
		{
			name:      "great-grandchild session with ancestors",
			meta:      Metadata{Parent: "grandchild", Ancestors: []string{"root", "child", "grandchild"}},
			wantDepth: 3,
		},
		{
			name:      "legacy session with parent only",
			meta:      Metadata{Parent: "root"},
			wantDepth: 1,
		},
		{
			name:      "legacy top-level session with empty parent",
			meta:      Metadata{Parent: ""},
			wantDepth: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.meta.Depth(); got != tc.wantDepth {
				t.Errorf("Metadata.Depth() = %d, want %d", got, tc.wantDepth)
			}
		})
	}

	// Verify persistence and round-tripping of Ancestors in FileStore.
	wantMeta := Metadata{
		Parent:    "child1",
		Ancestors: []string{"root", "child1"},
		Toolset:   "basic",
		CreatedAt: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC),
	}
	if err := store.NewSession(ctx, "roundtrip", wantMeta); err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	_, loadedMeta, err := store.LoadSession(ctx, "roundtrip")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if diff := cmp.Diff(wantMeta, *loadedMeta); diff != "" {
		t.Errorf("Metadata roundtrip mismatch (-want +got):\n%s", diff)
	}
	if loadedMeta.Depth() != 2 {
		t.Errorf("loadedMeta.Depth() = %d, want 2", loadedMeta.Depth())
	}
}
