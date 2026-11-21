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
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/run"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
	"github.com/gke-labs/auto-optimizer/pkg/web/auth"
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

func setupTestWebServer(t *testing.T, sb tools.Sandbox) (*httptest.Server, *sessionapi.Server) {
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

	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          store,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("sessionapi.NewServer: %v", err)
	}

	webServer, err := NewServer(srv, nil, nil, ServerOptions{})
	if err != nil {
		t.Fatalf("web.NewServer: %v", err)
	}

	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	return ts, srv
}

func testCSRFToken(t *testing.T, client *http.Client, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	if client.Jar != nil {
		for _, c := range client.Jar.Cookies(u) {
			if c.Name == csrfCookieName {
				return c.Value
			}
		}
	}
	t.Fatalf("csrf cookie %q not found in client jar for %s", csrfCookieName, rawURL)
	return ""
}

func TestAcceptance_SendMessageAndPoll(t *testing.T) {
	sb := &recordingSandbox{}
	ts, _ := setupTestWebServer(t, sb)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{Jar: jar}

	// 1. Visit list page to establish SameSite CSRF cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}

	// 2. Post a message to POST /sessions/test/messages
	prompt := "What is the capital of France?"
	postResp, err := client.PostForm(ts.URL+"/sessions/test/messages", url.Values{
		"text": {prompt},
		"csrf": {testCSRFToken(t, client, ts.URL)},
	})
	if err != nil {
		t.Fatalf("POST /sessions/test/messages: %v", err)
	}
	defer postResp.Body.Close()

	// http.Client followed the 303 redirect to /sessions/test
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("post redirect status = %d, want 200", postResp.StatusCode)
	}

	// 3. Poll the transcript page until the refresh tag disappears
	const refreshTag = `<meta http-equiv="refresh"`
	deadline := time.Now().Add(5 * time.Second)
	var finalBody string

	for time.Now().Before(deadline) {
		getResp, err := client.Get(ts.URL + "/sessions/test")
		if err != nil {
			t.Fatalf("GET /sessions/test: %v", err)
		}
		bodyBytes, err := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()
		if err != nil {
			t.Fatalf("reading body: %v", err)
		}
		finalBody = string(bodyBytes)

		if !strings.Contains(finalBody, refreshTag) {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if strings.Contains(finalBody, refreshTag) {
		t.Fatal("turn timed out: refresh tag still present on transcript page")
	}

	// 4. Assert the Eliza reply is rendered in the page
	wantReply := "What do you think it means when you say 'What is the capital of France?'?"
	if !strings.Contains(html.UnescapeString(finalBody), wantReply) {
		t.Errorf("expected reply %q in transcript page, got:\n%s", wantReply, finalBody)
	}
}

func TestSendMessage_ToolCallExecution(t *testing.T) {
	sb := &recordingSandbox{}
	ts, _ := setupTestWebServer(t, sb)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{Jar: jar}

	// Initialize cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()

	// Post message triggering run_command tool call
	postResp, err := client.PostForm(ts.URL+"/sessions/toolcall/messages", url.Values{
		"text": {"run: uname -a"},
		"csrf": {testCSRFToken(t, client, ts.URL)},
	})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = postResp.Body.Close()

	// Poll until turn finishes
	const refreshTag = `<meta http-equiv="refresh"`
	deadline := time.Now().Add(5 * time.Second)
	var finalBody string

	for time.Now().Before(deadline) {
		getResp, err := client.Get(ts.URL + "/sessions/toolcall")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		bodyBytes, err := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		finalBody = string(bodyBytes)
		if !strings.Contains(finalBody, refreshTag) {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if strings.Contains(finalBody, refreshTag) {
		t.Fatal("turn timed out: refresh tag still present")
	}

	if !strings.Contains(finalBody, "run_command") {
		t.Errorf("expected tool call run_command in transcript, got:\n%s", finalBody)
	}
	if !strings.Contains(finalBody, "Linux fake-test-node") {
		t.Errorf("expected tool result output in transcript, got:\n%s", finalBody)
	}
}

func TestSendMessage_BusySessionReturns409(t *testing.T) {
	sb := &blockingSandbox{
		startedCh: make(chan struct{}, 1),
		blockCh:   make(chan struct{}),
	}
	ts, _ := setupTestWebServer(t, sb)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // don't follow redirects automatically
		},
	}

	// Initialize cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()
	tok := testCSRFToken(t, client, ts.URL)

	// First message starts a blocking turn
	postResp1, err := client.PostForm(ts.URL+"/sessions/busysess/messages", url.Values{
		"text": {"run: sleep 10"},
		"csrf": {tok},
	})
	if err != nil {
		t.Fatalf("first POST: %v", err)
	}
	_ = postResp1.Body.Close()
	if postResp1.StatusCode != http.StatusSeeOther {
		t.Fatalf("first POST status = %d, want 303 See Other", postResp1.StatusCode)
	}

	// Wait until sandbox starts execution
	select {
	case <-sb.startedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn to start")
	}

	// Second POST to the busy session must return 409 Conflict
	postResp2, err := client.PostForm(ts.URL+"/sessions/busysess/messages", url.Values{
		"text": {"second message"},
		"csrf": {tok},
	})
	if err != nil {
		t.Fatalf("second POST: %v", err)
	}
	bodyBytes, _ := io.ReadAll(postResp2.Body)
	_ = postResp2.Body.Close()

	if postResp2.StatusCode != http.StatusConflict {
		t.Fatalf("second POST status = %d, want 409 Conflict. Body: %s", postResp2.StatusCode, string(bodyBytes))
	}
	if !strings.Contains(string(bodyBytes), "409 Conflict") {
		t.Errorf("expected 409 Conflict page, got: %s", string(bodyBytes))
	}

	// Unblock and wait for in-flight turn to finish before returning
	close(sb.blockCh)

	const refreshTag = `<meta http-equiv="refresh"`
	deadline := time.Now().Add(5 * time.Second)
	var finalBody string
	for time.Now().Before(deadline) {
		getResp, err := client.Get(ts.URL + "/sessions/busysess")
		if err != nil {
			t.Fatalf("GET /sessions/busysess: %v", err)
		}
		bodyBytes, err := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		finalBody = string(bodyBytes)
		if !strings.Contains(finalBody, refreshTag) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if strings.Contains(finalBody, refreshTag) {
		t.Fatal("turn timed out: refresh tag still present on transcript page")
	}
}

func TestCSRFProtection_RejectsCrossSitePost(t *testing.T) {
	ts, _ := setupTestWebServer(t, nil)

	// Client without cookiejar (no cookies sent)
	client := &http.Client{}

	resp, err := client.PostForm(ts.URL+"/sessions/nocookie/messages", url.Values{
		"text": {"hello"},
	})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST without cookie status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// Create session without cookie must also fail
	resp, err = client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"newsess"},
		"toolset": {"basic"},
	})
	if err != nil {
		t.Fatalf("POST /sessions: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /sessions without cookie status = %d, want 403 Forbidden", resp.StatusCode)
	}
}

func TestCSRFToken_RenderedInPage(t *testing.T) {
	key := []byte("secret-session-signing-key-32b!!")
	fakeSvc := &fakeSessionService{}
	webServer, err := NewServer(fakeSvc, nil, nil, ServerOptions{
		RequireSignIn: true,
		SessionKey:    key,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	sess := auth.Session{
		Login:   "alice",
		Expires: time.Now().Add(time.Hour),
	}
	sessVal, err := sess.Encode(key)
	if err != nil {
		t.Fatalf("Encode session: %v", err)
	}
	expectedCSRF := auth.CSRFToken(sessVal, key)
	if expectedCSRF == "" {
		t.Fatal("expected non-empty derived CSRF token")
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	u, _ := url.Parse(ts.URL)
	jar.SetCookies(u, []*http.Cookie{
		{Name: auth.CookieName, Value: sessVal, Path: "/"},
	})

	client := &http.Client{Jar: jar}
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	wantHidden := fmt.Sprintf(`<input type="hidden" name="csrf" value="%s">`, expectedCSRF)
	if !strings.Contains(string(body), wantHidden) {
		t.Errorf("expected rendered page to contain %q, got body:\n%s", wantHidden, string(body))
	}
}

func TestCreateSessionForm(t *testing.T) {
	ts, _ := setupTestWebServer(t, nil)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Get cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()

	// Post new session form
	postResp, err := client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"createdsess"},
		"toolset": {"basic"},
		"csrf":    {testCSRFToken(t, client, ts.URL)},
	})
	if err != nil {
		t.Fatalf("POST /sessions: %v", err)
	}
	_ = postResp.Body.Close()

	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303 See Other", postResp.StatusCode)
	}
	if loc := postResp.Header.Get("Location"); loc != "/sessions/createdsess" {
		t.Fatalf("Location = %q, want /sessions/createdsess", loc)
	}

	// Verify transcript page is accessible
	getResp, err := client.Get(ts.URL + "/sessions/createdsess")
	if err != nil {
		t.Fatalf("GET /sessions/createdsess: %v", err)
	}
	body, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.StatusCode)
	}
	if !strings.Contains(string(body), "Session: createdsess") {
		t.Errorf("expected session name in body, got: %s", string(body))
	}
}

func TestHealthzAndStatic(t *testing.T) {
	ts, _ := setupTestWebServer(t, nil)
	client := &http.Client{}

	// GET /healthz
	resp, err := client.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok\n" {
		t.Errorf("healthz body = %q, want ok\\n", string(body))
	}

	// GET /static/style.css
	resp, err = client.Get(ts.URL + "/static/style.css")
	if err != nil {
		t.Fatalf("GET /static/style.css: %v", err)
	}
	cssBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("static status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(cssBody), "--indigo") {
		t.Errorf("expected CSS content, got: %s", string(cssBody))
	}
}

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		maxRunes      int
		wantPrefix    string
		wantTruncated bool
	}{
		{
			name:          "empty",
			input:         "",
			maxRunes:      10,
			wantPrefix:    "",
			wantTruncated: false,
		},
		{
			name:          "short ascii",
			input:         "hello",
			maxRunes:      10,
			wantPrefix:    "hello",
			wantTruncated: false,
		},
		{
			name:          "exact ascii",
			input:         "hello",
			maxRunes:      5,
			wantPrefix:    "hello",
			wantTruncated: false,
		},
		{
			name:          "truncated ascii",
			input:         "hello world",
			maxRunes:      5,
			wantPrefix:    "hello",
			wantTruncated: true,
		},
		{
			name:          "multibyte utf8 within limit",
			input:         "你好世界",
			maxRunes:      4,
			wantPrefix:    "你好世界",
			wantTruncated: false,
		},
		{
			name:          "multibyte utf8 truncated",
			input:         "你好世界",
			maxRunes:      2,
			wantPrefix:    "你好",
			wantTruncated: true,
		},
		{
			name:          "emoji boundary not split",
			input:         "abc🚀def",
			maxRunes:      4,
			wantPrefix:    "abc🚀",
			wantTruncated: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPrefix, gotTruncated := truncateRunes(tc.input, tc.maxRunes)
			if gotTruncated != tc.wantTruncated {
				t.Errorf("truncateRunes(%q, %d) truncated = %v, want %v", tc.input, tc.maxRunes, gotTruncated, tc.wantTruncated)
			}
			if gotPrefix != tc.wantPrefix {
				t.Errorf("truncateRunes(%q, %d) prefix = %q, want %q", tc.input, tc.maxRunes, gotPrefix, tc.wantPrefix)
			}
			if !utf8.ValidString(gotPrefix) {
				t.Errorf("truncateRunes(%q, %d) returned invalid UTF-8: %q", tc.input, tc.maxRunes, gotPrefix)
			}
		})
	}
}

func TestTranscriptTruncation_UTF8(t *testing.T) {
	// 600 multibyte characters (each 3 bytes in UTF-8: 1800 bytes total).
	// Byte slicing at 500 would split the 167th rune; rune-aware truncation must keep 500 complete runes.
	const totalRunes = 600
	var sb strings.Builder
	for i := 0; i < totalRunes; i++ {
		sb.WriteRune('世')
	}
	longContent := sb.String()

	fakeSvc := &fakeSessionService{
		details: map[string]*sessionapi.SessionDetail{
			"utf8-session": {
				Summary: sessionapi.SessionSummary{
					Name:         "utf8-session",
					Toolset:      "basic",
					MessageCount: 1,
					CreatedAt:    time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC),
				},
				Messages: []sessionapi.Message{
					{
						Role:    "assistant",
						Content: longContent,
					},
				},
			},
		},
	}

	webServer, err := NewServer(fakeSvc, nil, nil, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	client := &http.Client{}

	// 1. Default request should be truncated at maxTruncateLen (500 runes)
	resp, err := client.Get(ts.URL + "/sessions/utf8-session")
	if err != nil {
		t.Fatalf("GET /sessions/utf8-session: %v", err)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	body := string(bodyBytes)

	if !utf8.Valid(bodyBytes) {
		t.Fatal("response body is not valid UTF-8")
	}

	wantPrefix := strings.Repeat("世", maxTruncateLen) + "..."
	if !strings.Contains(body, wantPrefix) {
		t.Errorf("expected truncated prefix %q in body", wantPrefix)
	}
	if !strings.Contains(body, `href="/sessions/utf8-session?full=1"`) {
		t.Errorf("expected 'show all' link with full=1 in body")
	}

	// 2. Request with ?full=1 should show all 600 runes
	fullResp, err := client.Get(ts.URL + "/sessions/utf8-session?full=1")
	if err != nil {
		t.Fatalf("GET /sessions/utf8-session?full=1: %v", err)
	}
	fullBodyBytes, err := io.ReadAll(fullResp.Body)
	_ = fullResp.Body.Close()
	if err != nil {
		t.Fatalf("reading full body: %v", err)
	}
	fullBody := string(fullBodyBytes)

	if !utf8.Valid(fullBodyBytes) {
		t.Fatal("full response body is not valid UTF-8")
	}
	if !strings.Contains(fullBody, longContent) {
		t.Errorf("expected full content (%d runes) in body", totalRunes)
	}
	if !strings.Contains(fullBody, `href="/sessions/utf8-session"`) {
		t.Errorf("expected 'show less' link in full body")
	}
}

func TestSessionDetail_MissingAncestorRenders(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := session.NewFileStore(storeDir)

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
	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          store,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("sessionapi.NewServer: %v", err)
	}

	// 1. Create a chain: root -> child -> grandchild
	if err := srv.CreateNewSession(ctx, "root", "basic"); err != nil {
		t.Fatalf("CreateNewSession(root): %v", err)
	}
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    "child",
		Parent:  "root",
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(child): %v", err)
	}
	if _, err := srv.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    "grandchild",
		Parent:  "child",
		Toolset: "basic",
	}); err != nil {
		t.Fatalf("CreateSession(grandchild): %v", err)
	}

	// 2. Delete root and child ancestor sessions from the store.
	if err := os.RemoveAll(filepath.Join(storeDir, "root")); err != nil {
		t.Fatalf("RemoveAll(root): %v", err)
	}
	if err := os.RemoveAll(filepath.Join(storeDir, "child")); err != nil {
		t.Fatalf("RemoveAll(child): %v", err)
	}

	// 3. Start web server backed by a new sessionapi server (so activeSessions does not cache them).
	freshSrv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          store,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("sessionapi.NewServer(fresh): %v", err)
	}

	webServer, err := NewServer(freshSrv, nil, nil, ServerOptions{})
	if err != nil {
		t.Fatalf("web.NewServer: %v", err)
	}
	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	client := ts.Client()

	// 4. GET /sessions/grandchild must render 200 OK (not 500 error).
	resp, err := client.Get(ts.URL + "/sessions/grandchild")
	if err != nil {
		t.Fatalf("GET /sessions/grandchild: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /sessions/grandchild status = %d, want 200 OK; body:\n%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), "grandchild") {
		t.Errorf("expected 'grandchild' in response body")
	}

	// 5. Also test a legacy session where parent was recorded but ancestor does not exist.
	legacyDir := filepath.Join(storeDir, "legacymissing")
	if err := os.MkdirAll(legacyDir, 0700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "metadata.json"), []byte(`{"parent":"goneparent","toolset":"basic"}`), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	respLegacy, err := client.Get(ts.URL + "/sessions/legacymissing")
	if err != nil {
		t.Fatalf("GET /sessions/legacymissing: %v", err)
	}
	bodyLegacy, err := io.ReadAll(respLegacy.Body)
	_ = respLegacy.Body.Close()
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if respLegacy.StatusCode != http.StatusOK {
		t.Fatalf("GET /sessions/legacymissing status = %d, want 200 OK; body:\n%s", respLegacy.StatusCode, string(bodyLegacy))
	}
	if !strings.Contains(string(bodyLegacy), "legacymissing") {
		t.Errorf("expected 'legacymissing' in response body")
	}
}

func TestServer_ReposAndRuns(t *testing.T) {
	ctx := t.Context()
	stateDir := t.TempDir()

	sessStore := session.NewFileStore(stateDir)
	stateStore := state.NewFileStore(stateDir)

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}
	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()
	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)

	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessStore,
		StateStore:     stateStore,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("sessionapi.NewServer: %v", err)
	}

	webServer, err := NewServer(srv, stateStore, nil, ServerOptions{})
	if err != nil {
		t.Fatalf("web.NewServer: %v", err)
	}

	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	client := ts.Client()

	// 1. Populate state with a repository, a goal, a run, events, and a session
	repoKey := state.RepoKey{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "auto-optimizer",
	}
	syncTime := time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)
	repo := &state.Repo{
		URL:            "https://github.com/gke-labs/auto-optimizer",
		Host:           repoKey.Host,
		Owner:          repoKey.Owner,
		Name:           repoKey.Name,
		DefaultBranch:  "main",
		HeadCommit:     "commit-head-123456",
		ForgeKind:      "github",
		RegisteredTime: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
		LastChangeTime: syncTime,
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261029T140000Z-0001"
	sessionID := "sessrun1"

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "latency-reduction",
			Schedule: "on-change",
			Body:     "# Latency Reduction\nReduce p99 latency by 20%.\n",
		},
		Hash:       "goal-hash-9999",
		BaseCommit: "commit-head-123456",
		LastRunID:  runID,
	}
	if err := stateStore.PutGoal(ctx, repoKey, goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	run := &state.Run{
		ID:         runID,
		Repo:       repoKey,
		GoalName:   "latency-reduction",
		Phase:      state.PhaseWorking,
		BaseCommit: "commit-head-123456",
		Branch:     "ao/latency-reduction/run-1",
		PRURL:      "https://github.com/gke-labs/auto-optimizer/pull/99",
		SessionID:  sessionID,
		Goal:       goalRec.Goal,
	}
	if err := stateStore.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	if err := stateStore.AppendEvent(ctx, repoKey, runID,
		state.Event{
			Time:    syncTime,
			Phase:   state.PhasePreparing,
			Kind:    "clone",
			Message: "Cloning repository",
		},
		state.Event{
			Time:    syncTime.Add(30 * time.Second),
			Phase:   state.PhaseWorking,
			Kind:    "session_start",
			Message: "Session initiated",
		},
	); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	// Create session linked to the run
	if err := srv.CreateNewSession(ctx, sessionID, "basic"); err != nil {
		t.Fatalf("CreateNewSession: %v", err)
	}

	// 2. Test GET /repos
	resp, err := client.Get(ts.URL + "/repos")
	if err != nil {
		t.Fatalf("GET /repos: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repos status = %d, want 200", resp.StatusCode)
	}
	bodyStr := string(body)
	for _, want := range []string{
		"github.com/gke-labs/auto-optimizer",
		"main",
		"github",
		"2026-10-29 14:00:00 UTC",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("GET /repos missing expected substring %q", want)
		}
	}

	// 3. Test GET /repos/github.com/gke-labs/auto-optimizer
	resp, err = client.Get(ts.URL + "/repos/github.com/gke-labs/auto-optimizer")
	if err != nil {
		t.Fatalf("GET /repos/...: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repos/... status = %d, want 200", resp.StatusCode)
	}
	bodyStr = string(body)
	for _, want := range []string{
		"latency-reduction",
		"Working",
		runID,
		"commit-head-123456",
		"ao/latency-reduction/run-1",
		"https://github.com/gke-labs/auto-optimizer/pull/99",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("GET /repos/... missing expected substring %q", want)
		}
	}

	// 4. Test GET /repos/missing/repo/here returns 404
	resp, err = client.Get(ts.URL + "/repos/missing/repo/here")
	if err != nil {
		t.Fatalf("GET /repos/missing/repo/here: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing repo, got %d", resp.StatusCode)
	}

	// 5. Test GET /runs/<runID>
	resp, err = client.Get(ts.URL + "/runs/" + runID)
	if err != nil {
		t.Fatalf("GET /runs/<runID>: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /runs/... status = %d, want 200", resp.StatusCode)
	}
	bodyStr = string(body)
	for _, want := range []string{
		"Run: " + runID,
		"Working",
		"Cloning repository",
		"Session initiated",
		"ao/latency-reduction/run-1",
		"https://github.com/gke-labs/auto-optimizer/pull/99",
		"/sessions/" + sessionID,
		"Reduce p99 latency by 20%.",
		"<meta http-equiv=\"refresh\" content=\"2\">",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("GET /runs/... missing expected substring %q", want)
		}
	}

	// 6. Test GET /runs/missing-id returns 404
	resp, err = client.Get(ts.URL + "/runs/20261029T000000Z-9999")
	if err != nil {
		t.Fatalf("GET /runs/missing: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing run, got %d", resp.StatusCode)
	}

	// 7. Test GET / (session list page shows run next to session)
	resp, err = client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}
	bodyStr = string(body)
	if !strings.Contains(bodyStr, sessionID) {
		t.Errorf("expected session list to contain session ID %q", sessionID)
	}
	if !strings.Contains(bodyStr, `/runs/`+runID) {
		t.Errorf("expected session list to link to run /runs/%s, got:\n%s", runID, bodyStr)
	}
	if !strings.Contains(bodyStr, `href="/repos"`) {
		t.Errorf("expected session list to link to /repos, got:\n%s", bodyStr)
	}
}

func TestRunAgain(t *testing.T) {
	ctx := t.Context()
	stateStore := state.NewFileStore(t.TempDir())

	scheduler, err := run.NewScheduler(run.SchedulerOptions{
		Store: stateStore,
	})
	if err != nil {
		t.Fatalf("run.NewScheduler: %v", err)
	}

	webServer, err := NewServer(&fakeSessionService{}, stateStore, scheduler, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/auto-optimizer",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "auto-optimizer",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name: "test-goal",
			Body: "Test goal description",
		},
		BaseCommit: "commit-base-123",
	}
	if err := stateStore.PutGoal(ctx, repoKey, goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	failedRunID, err := state.NewRunIDAt(now)
	if err != nil {
		t.Fatalf("NewRunIDAt: %v", err)
	}

	failedRun := &state.Run{
		ID:         failedRunID,
		Repo:       repoKey,
		GoalName:   "test-goal",
		Phase:      state.PhaseFailed,
		BaseCommit: "commit-base-123",
		Error:      "verification failed",
		Goal:       goalRec.Goal,
	}
	if err := stateStore.PutRun(ctx, failedRun); err != nil {
		t.Fatalf("PutRun failed: %v", err)
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	post := func(c *http.Client, path string, token string) (*http.Response, string) {
		t.Helper()
		var body io.Reader
		if token != "" {
			body = strings.NewReader(url.Values{"csrf": {token}}.Encode())
		}
		resp, err := c.Post(ts.URL+path, "application/x-www-form-urlencoded", body)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp, string(b)
	}

	// 1. Refuses without a valid CSRF token
	if resp, _ := post(&http.Client{}, "/runs/"+failedRunID+"/rerun", ""); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// 2. Establish CSRF cookie via GET /runs/<runID> and check "Run again" button
	getResp, err := client.Get(ts.URL + "/runs/" + failedRunID)
	if err != nil {
		t.Fatalf("GET /runs: %v", err)
	}
	getBody, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /runs status = %d, want 200", getResp.StatusCode)
	}
	if !strings.Contains(string(getBody), `action="/runs/`+failedRunID+`/rerun"`) {
		t.Errorf("expected finished run page to point form to /rerun, got:\n%s", string(getBody))
	}
	if !strings.Contains(string(getBody), "Run again") {
		t.Errorf("expected finished run page to contain 'Run again' button, got:\n%s", string(getBody))
	}
	tok := testCSRFToken(t, client, ts.URL)

	// 3. POST to the run's own URL /runs/<id> is not accepted (returns 404)
	if resp, _ := post(client, "/runs/"+failedRunID, tok); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST /runs/<id> status = %d, want 404 Not Found", resp.StatusCode)
	}

	// 4. Successfully starts a run for the failed goal
	postResp, _ := post(client, "/runs/"+failedRunID+"/rerun", tok)
	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /runs/<id>/rerun status = %d, want 303 See Other", postResp.StatusCode)
	}
	newRunLocation := postResp.Header.Get("Location")
	if !strings.HasPrefix(newRunLocation, "/runs/") {
		t.Fatalf("Location = %q, want prefix /runs/", newRunLocation)
	}
	newRunID := strings.TrimPrefix(newRunLocation, "/runs/")

	// Verify new run in store
	newRun, err := stateStore.GetRun(ctx, repoKey, newRunID)
	if err != nil {
		t.Fatalf("GetRun newRunID: %v", err)
	}
	if newRun.Phase != state.PhasePending {
		t.Errorf("new run phase = %s, want Pending", newRun.Phase)
	}
	events, err := stateStore.ReadEvents(ctx, repoKey, newRunID)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected 1 event, got %d (err: %v)", len(events), err)
	}
	if events[0].Data.(map[string]any)["reason"] != "manual" {
		t.Errorf("event reason = %v, want manual", events[0].Data)
	}
	if events[0].Data.(map[string]any)["by"] != "anonymous" {
		t.Errorf("event by = %v, want anonymous", events[0].Data)
	}

	// 5. Refuses while run is in progress
	activeResp, activeBody := post(client, "/runs/"+newRunID+"/rerun", tok)
	if activeResp.StatusCode != http.StatusConflict {
		t.Fatalf("POST active run status = %d, want 409 Conflict", activeResp.StatusCode)
	}
	if !strings.Contains(activeBody, "Cannot Start Run") || !strings.Contains(activeBody, "already has an active run") {
		t.Errorf("expected refusal error on run page, got:\n%s", activeBody)
	}

	// Active run page does not show "Run again" button
	activeGetResp, err := client.Get(ts.URL + "/runs/" + newRunID)
	if err != nil {
		t.Fatalf("GET active run: %v", err)
	}
	activeGetBody, _ := io.ReadAll(activeGetResp.Body)
	_ = activeGetResp.Body.Close()
	if strings.Contains(string(activeGetBody), "Run again") {
		t.Errorf("active run page should NOT contain 'Run again' button")
	}

	// 6. Refuses with 409 Conflict when goal is no longer in repository
	missingGoalRunID, err := state.NewRunIDAt(now.Add(2 * time.Minute))
	if err != nil {
		t.Fatalf("NewRunIDAt: %v", err)
	}
	missingRun := &state.Run{
		ID:       missingGoalRunID,
		Repo:     repoKey,
		GoalName: "removed-goal",
		Phase:    state.PhaseFailed,
	}
	if err := stateStore.PutRun(ctx, missingRun); err != nil {
		t.Fatalf("PutRun: %v", err)
	}
	missingResp, missingBody := post(client, "/runs/"+missingGoalRunID+"/rerun", tok)
	if missingResp.StatusCode != http.StatusConflict {
		t.Fatalf("POST missing goal status = %d, want 409 Conflict", missingResp.StatusCode)
	}
	if !strings.Contains(missingBody, "is no longer in the repository") {
		t.Errorf("expected missing goal error on run page, got:\n%s", missingBody)
	}

	// 7. Store / server error produces 500 Internal Server Error
	errServer, err := NewServer(&fakeSessionService{}, stateStore, &failRunStarter{err: errors.New("simulated internal store failure")}, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	tsErr := httptest.NewServer(errServer)
	t.Cleanup(tsErr.Close)
	errClient := tsErr.Client()
	errClient.Jar = jar
	errResp, err := errClient.Post(tsErr.URL+"/runs/"+failedRunID+"/rerun", "application/x-www-form-urlencoded", strings.NewReader(url.Values{"csrf": {tok}}.Encode()))
	if err != nil {
		t.Fatalf("POST with error starter: %v", err)
	}
	_ = errResp.Body.Close()
	if errResp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("POST error starter status = %d, want 500 Internal Server Error", errResp.StatusCode)
	}
}

type failRunStarter struct {
	err error
}

func (f *failRunStarter) StartRun(_ context.Context, _ *state.Repo, _ string, _ run.StartOptions) (*state.Run, error) {
	return nil, f.err
}

type recordRunStarter struct {
	lastOpts run.StartOptions
	run      *state.Run
	err      error
}

func (r *recordRunStarter) StartRun(_ context.Context, _ *state.Repo, _ string, opts run.StartOptions) (*state.Run, error) {
	r.lastOpts = opts
	return r.run, r.err
}

func TestRunAgain_Actor(t *testing.T) {
	ctx := t.Context()
	stateStore := state.NewFileStore(t.TempDir())

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/auto-optimizer",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "auto-optimizer",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID, err := state.NewRunIDAt(time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewRunIDAt: %v", err)
	}
	runRec := &state.Run{
		ID:       runID,
		Repo:     repoKey,
		GoalName: "test-goal",
		Phase:    state.PhaseFailed,
	}
	if err := stateStore.PutRun(ctx, runRec); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	starter := &recordRunStarter{
		run: &state.Run{ID: "20261116T110000Z-newr"},
	}
	srv, err := NewServer(&fakeSessionService{}, stateStore, starter, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// 1. Signed-in user passes authenticated login
	req := httptest.NewRequest(http.MethodPost, "/runs/"+runID+"/rerun", strings.NewReader(url.Values{"csrf": {"valid-csrf"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "valid-csrf"})
	req = req.WithContext(auth.WithUser(req.Context(), "octocat"))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /runs/<id>/rerun status = %d, want 303", rec.Code)
	}
	if starter.lastOpts.Reason != "manual" {
		t.Errorf("starter reason = %q, want manual", starter.lastOpts.Reason)
	}
	if starter.lastOpts.StartedByUser != "octocat" {
		t.Errorf("starter startedByUser = %q, want octocat", starter.lastOpts.StartedByUser)
	}

	// 2. Unauthenticated user passes anonymous
	reqAnon := httptest.NewRequest(http.MethodPost, "/runs/"+runID+"/rerun", strings.NewReader(url.Values{"csrf": {"valid-csrf"}}.Encode()))
	reqAnon.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqAnon.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "valid-csrf"})
	recAnon := httptest.NewRecorder()

	srv.ServeHTTP(recAnon, reqAnon)

	if recAnon.Code != http.StatusSeeOther {
		t.Fatalf("POST /runs/<id>/rerun status = %d, want 303", recAnon.Code)
	}
	if starter.lastOpts.StartedByUser != "anonymous" {
		t.Errorf("starter startedByUser = %q, want anonymous", starter.lastOpts.StartedByUser)
	}
}

func TestNewServer_Validation(t *testing.T) {
	fakeSvc := &fakeSessionService{}
	validKey := []byte("12345678901234567890123456789012") // 32 bytes

	if _, err := NewServer(fakeSvc, nil, nil, ServerOptions{RequireSignIn: true}); err == nil {
		t.Error("expected error when RequireSignIn is true and SessionKey is missing")
	}
	if _, err := NewServer(fakeSvc, nil, nil, ServerOptions{RequireSignIn: true, SessionKey: []byte("too-short")}); err == nil {
		t.Error("expected error when RequireSignIn is true and SessionKey is shorter than MinKeyLength")
	}
	if _, err := NewServer(fakeSvc, nil, nil, ServerOptions{RequireSignIn: true, SessionKey: validKey}); err != nil {
		t.Errorf("unexpected error with valid key: %v", err)
	}
	if _, err := NewServer(fakeSvc, nil, nil, ServerOptions{RequireSignIn: false}); err != nil {
		t.Errorf("unexpected error when RequireSignIn is false: %v", err)
	}
	if _, err := NewServer(fakeSvc, nil, nil, ServerOptions{RequireSignIn: false, SessionKey: []byte("short")}); err != nil {
		t.Errorf("unexpected error when RequireSignIn is false with short key: %v", err)
	}
}

func TestCSRFValidation(t *testing.T) {
	t.Run("insecure_no_auth", func(t *testing.T) {
		ts, _ := setupTestWebServer(t, nil)
		jar, _ := cookiejar.New(nil)
		client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

		getResp, err := client.Get(ts.URL + "/")
		if err != nil {
			t.Fatalf("GET /: %v", err)
		}
		body, _ := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()

		tok := testCSRFToken(t, client, ts.URL)
		if want := fmt.Sprintf(`<input type="hidden" name="csrf" value="%s">`, tok); !strings.Contains(string(body), want) {
			t.Errorf("expected page to render %q, got:\n%s", want, string(body))
		}

		post := func(csrf string) int {
			vals := url.Values{"name": {"sess1"}, "toolset": {"basic"}}
			if csrf != "" {
				vals.Set("csrf", csrf)
			}
			resp, err := client.PostForm(ts.URL+"/sessions", vals)
			if err != nil {
				t.Fatalf("POST /sessions: %v", err)
			}
			_ = resp.Body.Close()
			return resp.StatusCode
		}

		if got := post(""); got != http.StatusForbidden {
			t.Errorf("POST with no csrf status = %d, want 403", got)
		}
		if got := post("wrong-token"); got != http.StatusForbidden {
			t.Errorf("POST with wrong csrf status = %d, want 403", got)
		}
		if got := post(tok); got != http.StatusSeeOther {
			t.Errorf("POST with correct csrf status = %d, want 303", got)
		}
	})

	t.Run("sign_in_on", func(t *testing.T) {
		key := []byte("secret-session-signing-key-32b!!")
		webServer, err := NewServer(&fakeSessionService{}, nil, nil, ServerOptions{RequireSignIn: true, SessionKey: key})
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		ts := httptest.NewServer(webServer)
		t.Cleanup(ts.Close)

		sess := auth.Session{Login: "alice", Expires: time.Now().Add(time.Hour)}
		sessVal, err := sess.Encode(key)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		correctCSRF := auth.CSRFToken(sessVal, key)

		jar, _ := cookiejar.New(nil)
		u, _ := url.Parse(ts.URL)
		jar.SetCookies(u, []*http.Cookie{{Name: auth.CookieName, Value: sessVal, Path: "/"}})
		client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

		post := func(csrf string) int {
			vals := url.Values{"name": {"sess2"}, "toolset": {"basic"}}
			if csrf != "" {
				vals.Set("csrf", csrf)
			}
			resp, err := client.PostForm(ts.URL+"/sessions", vals)
			if err != nil {
				t.Fatalf("POST /sessions: %v", err)
			}
			_ = resp.Body.Close()
			return resp.StatusCode
		}

		if got := post(""); got != http.StatusForbidden {
			t.Errorf("POST with no csrf status = %d, want 403", got)
		}
		if got := post("wrong-token"); got != http.StatusForbidden {
			t.Errorf("POST with wrong csrf status = %d, want 403", got)
		}
		if got := post(correctCSRF); got != http.StatusSeeOther {
			t.Errorf("POST with correct csrf status = %d, want 303", got)
		}
	})

	t.Run("sign_in_off_with_session_key", func(t *testing.T) {
		key := []byte("secret-session-signing-key-32b!!")
		webServer, err := NewServer(&fakeSessionService{}, nil, nil, ServerOptions{RequireSignIn: false, SessionKey: key})
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		ts := httptest.NewServer(webServer)
		t.Cleanup(ts.Close)

		jar, _ := cookiejar.New(nil)
		client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

		resp, err := client.Get(ts.URL + "/")
		if err != nil {
			t.Fatalf("GET /: %v", err)
		}
		_ = resp.Body.Close()

		browserTok := testCSRFToken(t, client, ts.URL)

		sess := auth.Session{Login: "alice", Expires: time.Now().Add(time.Hour)}
		sessVal, _ := sess.Encode(key)
		signInCSRF := auth.CSRFToken(sessVal, key)

		u, _ := url.Parse(ts.URL)
		jar.SetCookies(u, []*http.Cookie{{Name: auth.CookieName, Value: sessVal, Path: "/"}})

		post := func(csrf string) int {
			resp, err := client.PostForm(ts.URL+"/sessions", url.Values{"name": {"sess3"}, "toolset": {"basic"}, "csrf": {csrf}})
			if err != nil {
				t.Fatalf("POST /sessions: %v", err)
			}
			_ = resp.Body.Close()
			return resp.StatusCode
		}

		if got := post(signInCSRF); got != http.StatusForbidden {
			t.Errorf("POST with sign-in CSRF token when sign-in is off status = %d, want 403", got)
		}
		if got := post(browserTok); got != http.StatusSeeOther {
			t.Errorf("POST with browser cookie token status = %d, want 303", got)
		}
	})
}

func TestPostMessage_AppendsEvent(t *testing.T) {
	ctx := t.Context()
	stateDir := t.TempDir()
	stateStore := state.NewFileStore(stateDir)
	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	if err := stateStore.PutRepo(ctx, &state.Repo{Host: repoKey.Host, Owner: repoKey.Owner, Name: repoKey.Name}); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID, sessionID := "20261121T100000Z-0001", "sess-run-42"
	if err := stateStore.PutRun(ctx, &state.Run{ID: runID, Repo: repoKey, GoalName: "test-goal", SessionID: sessionID}); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	fakeSvc := &fakeSessionService{
		details: map[string]*sessionapi.SessionDetail{
			sessionID: {
				Summary: sessionapi.SessionSummary{
					Name:  sessionID,
					Repo:  repoKey.String(),
					RunID: runID,
				},
			},
			"standalone": {
				Summary: sessionapi.SessionSummary{
					Name: "standalone",
				},
			},
		},
	}
	webServer, err := NewServer(fakeSvc, stateStore, nil, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	secretBody := "super secret instruction that must not leak"
	post := func(user, sess, text string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/sessions/"+sess+"/messages", strings.NewReader(url.Values{"text": {text}, "csrf": {"csrf-val"}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "csrf-val"})
		if user != "" {
			req = req.WithContext(auth.WithUser(req.Context(), user))
		}
		rec := httptest.NewRecorder()
		webServer.ServeHTTP(rec, req)
		return rec
	}

	// 1. Authenticated user posts a message
	if rec := post("octocat", sessionID, secretBody); rec.Code != http.StatusSeeOther {
		t.Fatalf("POST status = %d, want 303", rec.Code)
	}
	events, err := stateStore.ReadEvents(ctx, repoKey, runID)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected 1 event, got %d (err: %v)", len(events), err)
	}
	if events[0].Kind != "message_posted" || events[0].Data.(map[string]any)["by"] != "octocat" {
		t.Errorf("unexpected event: %+v", events[0])
	}
	raw, _ := os.ReadFile(filepath.Join(stateDir, "repos", repoKey.Dir(), "runs", runID, "events.jsonl"))
	if strings.Contains(string(raw), secretBody) {
		t.Errorf("events.jsonl contains secret body: %s", string(raw))
	}

	// 2. Anonymous user posts a message
	if rec := post("", sessionID, "anon body"); rec.Code != http.StatusSeeOther {
		t.Fatalf("POST anon status = %d, want 303", rec.Code)
	}
	events, _ = stateStore.ReadEvents(ctx, repoKey, runID)
	if len(events) != 2 || events[1].Data.(map[string]any)["by"] != "anonymous" {
		t.Errorf("unexpected second event: %+v", events)
	}

	// 3. Standalone session without run
	if rec := post("", "standalone", "hi"); rec.Code != http.StatusSeeOther {
		t.Fatalf("POST standalone status = %d, want 303", rec.Code)
	}
}
