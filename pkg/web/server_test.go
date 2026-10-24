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

package web

import (
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/google/go-cmp/cmp"
)

var update = flag.Bool("update", false, "update golden files")

func assertGolden(t *testing.T, filename string, actual string) {
	t.Helper()
	goldenPath := filepath.Join("testdata", filename)
	if *update {
		if err := os.MkdirAll("testdata", 0755); err != nil {
			t.Fatalf("failed to create testdata dir: %v", err)
		}
		if err := os.WriteFile(goldenPath, []byte(actual), 0644); err != nil {
			t.Fatalf("failed to update golden file %s: %v", goldenPath, err)
		}
		return
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file %s: %v (run with -update to generate)", goldenPath, err)
	}

	if diff := cmp.Diff(string(expected), actual); diff != "" {
		t.Errorf("golden file %s mismatch (-want +got):\n%s", filename, diff)
	}
}

type fakeSessionSource struct {
	sessions map[string]*SessionDetail
	list     []SessionSummary
}

func (f *fakeSessionSource) ListSessions(_ context.Context) ([]SessionSummary, error) {
	return f.list, nil
}

func (f *fakeSessionSource) GetSession(_ context.Context, name string) (*SessionDetail, error) {
	detail, ok := f.sessions[name]
	if !ok {
		return nil, ErrNotFound
	}
	return detail, nil
}

func stringPtr(s string) *string {
	return &s
}

func newTestServer(t *testing.T) (*Server, *fakeSessionSource) {
	t.Helper()

	fixedTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	fakeSource := &fakeSessionSource{
		sessions: map[string]*SessionDetail{
			"empty-session": {
				Name:      "empty-session",
				Toolset:   "basic",
				CreatedAt: fixedTime,
				Busy:      false,
				Messages:  nil,
			},
			"tool-session": {
				Name:      "tool-session",
				Toolset:   "geminicli",
				CreatedAt: fixedTime,
				Busy:      false,
				Messages: []llm.Message{
					{
						Role:    "user",
						Content: stringPtr("What is the kernel version?"),
					},
					{
						Role:    "assistant",
						Content: stringPtr("Let me check the kernel version."),
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call_1",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: `{"command":"uname -a"}`,
								},
							},
						},
					},
					{
						Role:       "tool",
						Content:    stringPtr("Linux sandbox 6.6.0 #1 SMP PREEMPT_DYNAMIC x86_64"),
						ToolCallID: "call_1",
					},
					{
						Role:    "assistant",
						Content: stringPtr("The kernel version is Linux 6.6.0."),
					},
				},
			},
			"busy-session": {
				Name:      "busy-session",
				Toolset:   "basic",
				CreatedAt: fixedTime,
				Busy:      true,
				Messages: []llm.Message{
					{
						Role:    "user",
						Content: stringPtr("Please run the benchmark"),
					},
				},
			},
			"idle-session": {
				Name:      "idle-session",
				Toolset:   "basic",
				CreatedAt: fixedTime,
				Busy:      false,
				Messages: []llm.Message{
					{
						Role:    "user",
						Content: stringPtr("Run complete?"),
					},
					{
						Role:    "assistant",
						Content: stringPtr("Yes, all benchmarks completed successfully."),
					},
				},
			},
			"long-session": {
				Name:      "long-session",
				Toolset:   "geminicli",
				CreatedAt: fixedTime,
				Busy:      false,
				Messages: []llm.Message{
					{
						Role:    "user",
						Content: stringPtr("Show long log"),
					},
					{
						Role:    "tool",
						Content: stringPtr(strings.Repeat("Line of log output with lots of details\n", 50)),
					},
				},
			},
		},
	}

	server, err := NewServer(ServerOptions{
		Sessions:         fakeSource,
		MaxContentLength: 200,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	return server, fakeSource
}

func TestGolden_EmptySession(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/sessions/empty-session", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if strings.Contains(body, `http-equiv="refresh"`) {
		t.Errorf("idle empty session should not have refresh tag, got:\n%s", body)
	}

	assertGolden(t, "empty_session.golden.html", body)
}

func TestGolden_ToolCallSession(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/sessions/tool-session", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	for _, expected := range []string{
		"tool-session",
		"What is the kernel version?",
		"Let me check the kernel version.",
		`run_command({&#34;command&#34;:&#34;uname -a&#34;})`,
		"<details>",
		"<summary>Tool result</summary>",
		"Linux sandbox 6.6.0 #1 SMP PREEMPT_DYNAMIC x86_64",
		"</details>",
		"The kernel version is Linux 6.6.0.",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("expected body to contain %q, but was missing", expected)
		}
	}

	assertGolden(t, "tool_call_session.golden.html", body)
}

func TestGolden_BusySession(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/sessions/busy-session", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	expectedRefresh := `<meta http-equiv="refresh" content="2">`
	if !strings.Contains(body, expectedRefresh) {
		t.Errorf("expected busy session to contain refresh tag %q, got:\n%s", expectedRefresh, body)
	}
	if !strings.Contains(body, "chip-busy") {
		t.Errorf("expected busy chip on busy session, got:\n%s", body)
	}

	assertGolden(t, "busy_session.golden.html", body)
}

func TestGolden_IdleSession(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/sessions/idle-session", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if strings.Contains(body, `http-equiv="refresh"`) {
		t.Errorf("expected idle session not to contain refresh tag, got:\n%s", body)
	}
	if !strings.Contains(body, "chip-idle") {
		t.Errorf("expected idle chip on idle session, got:\n%s", body)
	}

	assertGolden(t, "idle_session.golden.html", body)
}

func TestGolden_UnknownSession(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/sessions/nonexistent", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Session Not Found") {
		t.Errorf("expected body to contain 'Session Not Found', got:\n%s", body)
	}
	if !strings.Contains(body, "nonexistent") {
		t.Errorf("expected body to contain session name 'nonexistent', got:\n%s", body)
	}

	assertGolden(t, "unknown_session.golden.html", body)
}

func TestGolden_TruncatedSession(t *testing.T) {
	server, _ := newTestServer(t)

	// Without ?full=1, content should be truncated and carry show all link.
	req := httptest.NewRequest(http.MethodGet, "/sessions/long-session", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	expectedLink := `<a href="/sessions/long-session?full=1">show all</a>`
	if !strings.Contains(body, expectedLink) {
		t.Errorf("expected body to contain show all link %q, got:\n%s", expectedLink, body)
	}

	assertGolden(t, "truncated_session.golden.html", body)
}

func TestGolden_FullSession(t *testing.T) {
	server, _ := newTestServer(t)

	// With ?full=1, content should not be truncated.
	req := httptest.NewRequest(http.MethodGet, "/sessions/long-session?full=1", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if strings.Contains(body, "show all") {
		t.Errorf("expected body with ?full=1 not to contain 'show all', got:\n%s", body)
	}

	assertGolden(t, "full_session.golden.html", body)
}

func TestHealthz(t *testing.T) {
	server, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if rec.Body.String() != "ok\n" {
		t.Fatalf("expected 'ok\n', got %q", rec.Body.String())
	}
}

func TestStoreSessionSource(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	store := session.NewFileStore(tmpDir)

	sessName := "persisted1"
	meta := session.Metadata{
		Toolset:   "basic",
		Parent:    "parent1",
		CreatedAt: time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC),
	}
	if err := store.NewSession(ctx, sessName, meta); err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	userMsg := "hello store"
	if err := store.AppendMessages(ctx, sessName, llm.Message{
		Role:    "user",
		Content: &userMsg,
	}); err != nil {
		t.Fatalf("failed to append messages: %v", err)
	}

	busySet := map[string]bool{sessName: true}
	adapter := NewStoreSessionSource(store, func(name string) bool {
		return busySet[name]
	})

	detail, err := adapter.GetSession(ctx, sessName)
	if err != nil {
		t.Fatalf("failed to get session: %v", err)
	}
	if detail.Name != sessName {
		t.Errorf("detail.Name = %q, want %q", detail.Name, sessName)
	}
	if detail.Toolset != "basic" {
		t.Errorf("detail.Toolset = %q, want basic", detail.Toolset)
	}
	if detail.Parent != "parent1" {
		t.Errorf("detail.Parent = %q, want parent1", detail.Parent)
	}
	if !detail.Busy {
		t.Errorf("detail.Busy = false, want true")
	}
	if len(detail.Messages) != 1 || *detail.Messages[0].Content != userMsg {
		t.Errorf("detail.Messages = %+v, want 1 user message", detail.Messages)
	}

	// Nonexistent session returns ErrNotFound
	_, err = adapter.GetSession(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent session, got nil")
	}
}
