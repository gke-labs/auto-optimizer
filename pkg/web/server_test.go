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
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
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

	webServer, err := NewServer(srv)
	if err != nil {
		t.Fatalf("web.NewServer: %v", err)
	}

	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	return ts, srv
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

	// First message starts a blocking turn
	postResp1, err := client.PostForm(ts.URL+"/sessions/busysess/messages", url.Values{
		"text": {"run: sleep 10"},
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

	// Unblock and clean up
	close(sb.blockCh)
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
