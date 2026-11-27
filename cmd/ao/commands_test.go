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

package main

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/forge/github"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	runpkg "github.com/gke-labs/auto-optimizer/pkg/run"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/web/auth"
)

func TestCommandHierarchy(t *testing.T) {
	root := newRootCommand()

	expectedSubcommands := []string{"serve", "session", "repo", "goal", "run"}
	for _, name := range expectedSubcommands {
		cmd, _, err := root.Find([]string{name})
		if err != nil || cmd == nil || cmd.Name() != name {
			t.Errorf("expected subcommand %q on root command", name)
		}
	}
}

func TestRepoCommand(t *testing.T) {
	cmd := newRepoCommand()
	listCmd, _, err := cmd.Find([]string{"list"})
	if err != nil || listCmd == nil || listCmd.Name() != "list" {
		t.Fatalf("expected list subcommand on repo command")
	}

	if listCmd.Flag("server") == nil {
		t.Error("expected --server flag on ao repo list")
	}
	if listCmd.Flag("json") == nil {
		t.Error("expected --json flag on ao repo list")
	}
}

func TestGoalCommand(t *testing.T) {
	cmd := newGoalCommand()
	listCmd, _, err := cmd.Find([]string{"list"})
	if err != nil || listCmd == nil || listCmd.Name() != "list" {
		t.Fatalf("expected list subcommand on goal command")
	}

	if listCmd.Flag("repo") == nil {
		t.Error("expected --repo flag on ao goal list")
	}
	if listCmd.Flag("server") == nil {
		t.Error("expected --server flag on ao goal list")
	}
}

func TestRunCommand(t *testing.T) {
	cmd := newRunCommand()
	listCmd, _, err := cmd.Find([]string{"list"})
	if err != nil || listCmd == nil || listCmd.Name() != "list" {
		t.Fatalf("expected list subcommand on run command")
	}

	if listCmd.Flag("repo") == nil {
		t.Error("expected --repo flag on ao run list")
	}
	if listCmd.Flag("server") == nil {
		t.Error("expected --server flag on ao run list")
	}
	if listCmd.Flag("json") == nil {
		t.Error("expected --json flag on ao run list")
	}

	getCmd, _, err := cmd.Find([]string{"get"})
	if err != nil || getCmd == nil || getCmd.Name() != "get" {
		t.Fatalf("expected get subcommand on run command")
	}

	if getCmd.Flag("server") == nil {
		t.Error("expected --server flag on ao run get")
	}
	if getCmd.Flag("json") == nil {
		t.Error("expected --json flag on ao run get")
	}
}

func TestServeFlags(t *testing.T) {
	cmd := newServeCommand()
	if cmd.Flag("repo") == nil {
		t.Error("expected --repo flag on ao serve")
	}
	maxTurnsFlag := cmd.Flag("max-turns")
	if maxTurnsFlag == nil {
		t.Error("expected --max-turns flag on ao serve")
	} else if maxTurnsFlag.DefValue != "100" {
		t.Errorf("expected default max-turns to be 100, got %s", maxTurnsFlag.DefValue)
	}
	maxSessionDepthFlag := cmd.Flag("max-session-depth")
	if maxSessionDepthFlag == nil {
		t.Error("expected --max-session-depth flag on ao serve")
	} else if maxSessionDepthFlag.DefValue != "3" {
		t.Errorf("expected default max-session-depth to be 3, got %s", maxSessionDepthFlag.DefValue)
	}
	flag := cmd.Flag("max-concurrent-runs")
	if flag == nil {
		t.Error("expected --max-concurrent-runs flag on ao serve")
	} else if flag.DefValue != "4" {
		t.Errorf("expected default max-concurrent-runs to be 4, got %s", flag.DefValue)
	}
	gitAuthor := cmd.Flag("git-author")
	if gitAuthor == nil {
		t.Error("expected --git-author flag on ao serve")
	} else if gitAuthor.DefValue != "auto-optimizer <auto-optimizer@users.noreply.github.com>" {
		t.Errorf("expected default git-author to be %q, got %s", "auto-optimizer <auto-optimizer@users.noreply.github.com>", gitAuthor.DefValue)
	}
	readyTimeoutFlag := cmd.Flag("ready-timeout")
	if readyTimeoutFlag == nil {
		t.Error("expected --ready-timeout flag on ao serve")
	} else if readyTimeoutFlag.DefValue != "3m0s" {
		t.Errorf("expected default ready-timeout to be 3m0s, got %s", readyTimeoutFlag.DefValue)
	}
	verifyTimeoutFlag := cmd.Flag("verify-timeout")
	if verifyTimeoutFlag == nil {
		t.Error("expected --verify-timeout flag on ao serve")
	} else if verifyTimeoutFlag.DefValue != "10m0s" {
		t.Errorf("expected default verify-timeout to be 10m0s, got %s", verifyTimeoutFlag.DefValue)
	}
	maxRunDurationFlag := cmd.Flag("max-run-duration")
	if maxRunDurationFlag == nil {
		t.Error("expected --max-run-duration flag on ao serve")
	} else if maxRunDurationFlag.DefValue != "1h0m0s" {
		t.Errorf("expected default max-run-duration to be 1h0m0s, got %s", maxRunDurationFlag.DefValue)
	}
	publicURLFlag := cmd.Flag("public-url")
	if publicURLFlag == nil {
		t.Error("expected --public-url flag on ao serve")
	} else if publicURLFlag.DefValue != "" {
		t.Errorf("expected default public-url to be empty, got %s", publicURLFlag.DefValue)
	}
	if cmd.Flag("insecure-no-auth") == nil {
		t.Error("expected --insecure-no-auth flag on ao serve")
	}
	if cmd.Flag("github-client-id") == nil {
		t.Error("expected --github-client-id flag on ao serve")
	}
	if cmd.Flag("github-allowed-user") == nil {
		t.Error("expected --github-allowed-user flag on ao serve")
	}
}

func TestServeAuthValidation(t *testing.T) {
	t.Run("refuses to start with neither client ID nor insecure-no-auth", func(t *testing.T) {
		var opts serveOptions
		opts.InitDefaults()
		if err := opts.Validate(); err == nil {
			t.Error("expected error with neither client ID nor --insecure-no-auth, got nil")
		}
	})

	t.Run("succeeds with insecure-no-auth", func(t *testing.T) {
		var opts serveOptions
		opts.InitDefaults()
		opts.insecureNoAuth = true
		if err := opts.Validate(); err != nil {
			t.Errorf("expected nil error with --insecure-no-auth, got %v", err)
		}
	})

	t.Run("refuses to start with client ID but missing AO_SESSION_KEY", func(t *testing.T) {
		t.Setenv("AO_SESSION_KEY", "")
		var opts serveOptions
		opts.InitDefaults()
		opts.github = auth.GitHubOptions{
			ClientID:     "id",
			ClientSecret: "secret",
			PublicURL:    "https://example.com",
		}
		if err := opts.Validate(); err == nil {
			t.Error("expected error for missing AO_SESSION_KEY, got nil")
		}
	})

	t.Run("refuses to start with client ID and short AO_SESSION_KEY", func(t *testing.T) {
		t.Setenv("AO_SESSION_KEY", "too-short")
		var opts serveOptions
		opts.InitDefaults()
		opts.github = auth.GitHubOptions{
			ClientID:     "id",
			ClientSecret: "secret",
			PublicURL:    "https://example.com",
		}
		if err := opts.Validate(); err == nil {
			t.Error("expected error for short AO_SESSION_KEY, got nil")
		}
	})

	t.Run("succeeds with client ID, valid key, and valid github options", func(t *testing.T) {
		t.Setenv("AO_SESSION_KEY", "session-signing-key-32-bytes-long!")
		var opts serveOptions
		opts.InitDefaults()
		opts.github = auth.GitHubOptions{
			ClientID:     "id",
			ClientSecret: "secret",
			PublicURL:    "https://example.com",
			AllowedUsers: []string{"octocat"},
		}
		opts.github.InitDefaults()
		if err := opts.Validate(); err != nil {
			t.Errorf("expected valid, got %v", err)
		}
	})
}

func TestServeGitAuthorValidation(t *testing.T) {
	t.Run("valid custom author", func(t *testing.T) {
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{"--git-author", "Custom Bot <bot@example.com>"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
	})

	t.Run("unparsable value rejected", func(t *testing.T) {
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{"--git-author", "not-a-valid-address"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err == nil {
			t.Error("expected error for unparsable --git-author, got nil")
		}
	})

	t.Run("AO_GIT_AUTHOR overrides default when flag unset", func(t *testing.T) {
		t.Setenv("AO_GIT_AUTHOR", "Env Bot <env@example.com>")
		var opts serveOptions
		cmd := newServeCommandWithOptions(&opts)
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if opts.git.Author != "Env Bot <env@example.com>" {
			t.Errorf("git.Author = %q, want %q", opts.git.Author, "Env Bot <env@example.com>")
		}
	})

	t.Run("flag overrides AO_GIT_AUTHOR", func(t *testing.T) {
		t.Setenv("AO_GIT_AUTHOR", "Env Bot <env@example.com>")
		var opts serveOptions
		cmd := newServeCommandWithOptions(&opts)
		if err := cmd.ParseFlags([]string{"--git-author", "Flag Bot <flag@example.com>"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if opts.git.Author != "Flag Bot <flag@example.com>" {
			t.Errorf("git.Author = %q, want %q", opts.git.Author, "Flag Bot <flag@example.com>")
		}
	})

	t.Run("invalid AO_GIT_AUTHOR rejected", func(t *testing.T) {
		t.Setenv("AO_GIT_AUTHOR", "not-a-valid-address")
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err == nil {
			t.Error("expected error for invalid AO_GIT_AUTHOR, got nil")
		}
	})
}

func TestServePublicURLValidation(t *testing.T) {
	t.Run("default empty", func(t *testing.T) {
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if got := cmd.Flag("public-url").Value.String(); got != "" {
			t.Errorf("expected empty public-url, got %q", got)
		}
	})

	t.Run("valid URL accepted with slash stripped", func(t *testing.T) {
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{"--public-url", "https://ao.example.com/"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if got := cmd.Flag("public-url").Value.String(); got != "https://ao.example.com" {
			t.Errorf("got public-url %q, want %q", got, "https://ao.example.com")
		}
	})

	t.Run("invalid value rejected", func(t *testing.T) {
		for _, invalid := range []string{
			"not-a-valid-url",
			"ftp://ao.example.com",
			"http://",
			"https://",
			"://bad-url",
		} {
			t.Run(invalid, func(t *testing.T) {
				cmd := newServeCommand()
				if err := cmd.ParseFlags([]string{"--public-url", invalid}); err != nil {
					t.Fatalf("ParseFlags: %v", err)
				}
				if err := cmd.PreRunE(cmd, nil); err == nil {
					t.Errorf("expected error for invalid --public-url %q, got nil", invalid)
				}
			})
		}
	})

	t.Run("environment variable accepted when flag unset", func(t *testing.T) {
		t.Setenv("AO_PUBLIC_URL", "https://ao-env.example.com/")
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if got := cmd.Flag("public-url").Value.String(); got != "https://ao-env.example.com" {
			t.Errorf("got public-url %q, want %q", got, "https://ao-env.example.com")
		}
	})

	t.Run("flag overrides environment variable", func(t *testing.T) {
		t.Setenv("AO_PUBLIC_URL", "https://ao-env.example.com/")
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{"--public-url", "https://ao-flag.example.com/"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if got := cmd.Flag("public-url").Value.String(); got != "https://ao-flag.example.com" {
			t.Errorf("got public-url %q, want %q", got, "https://ao-flag.example.com")
		}
	})
}

func TestServeMaxTurnsValidation(t *testing.T) {
	t.Run("default is 100", func(t *testing.T) {
		t.Setenv("AO_MAX_TURNS", "")
		var opts serveOptions
		cmd := newServeCommandWithOptions(&opts)
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if opts.orchestrator.MaxTurns != 100 {
			t.Errorf("orchestrator.MaxTurns = %d, want 100", opts.orchestrator.MaxTurns)
		}
	})

	t.Run("valid AO_MAX_TURNS", func(t *testing.T) {
		t.Setenv("AO_MAX_TURNS", "42")
		var opts serveOptions
		cmd := newServeCommandWithOptions(&opts)
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if opts.orchestrator.MaxTurns != 42 {
			t.Errorf("orchestrator.MaxTurns = %d, want 42", opts.orchestrator.MaxTurns)
		}
	})

	t.Run("invalid unparseable AO_MAX_TURNS", func(t *testing.T) {
		t.Setenv("AO_MAX_TURNS", "not-a-number")
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		err := cmd.PreRunE(cmd, nil)
		if err == nil {
			t.Fatal("expected error for unparseable AO_MAX_TURNS, got nil")
		}
		if !strings.Contains(err.Error(), "parsing AO_MAX_TURNS") {
			t.Errorf("error = %q, want containing 'parsing AO_MAX_TURNS'", err.Error())
		}
	})

	t.Run("zero AO_MAX_TURNS", func(t *testing.T) {
		t.Setenv("AO_MAX_TURNS", "0")
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		err := cmd.PreRunE(cmd, nil)
		if err == nil {
			t.Fatal("expected error for zero AO_MAX_TURNS, got nil")
		}
		if !strings.Contains(err.Error(), "AO_MAX_TURNS must be positive") {
			t.Errorf("error = %q, want containing 'AO_MAX_TURNS must be positive'", err.Error())
		}
	})

	t.Run("negative AO_MAX_TURNS", func(t *testing.T) {
		t.Setenv("AO_MAX_TURNS", "-5")
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		err := cmd.PreRunE(cmd, nil)
		if err == nil {
			t.Fatal("expected error for negative AO_MAX_TURNS, got nil")
		}
		if !strings.Contains(err.Error(), "AO_MAX_TURNS must be positive") {
			t.Errorf("error = %q, want containing 'AO_MAX_TURNS must be positive'", err.Error())
		}
	})

	t.Run("zero max-turns flag", func(t *testing.T) {
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{"--max-turns", "0"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		err := cmd.PreRunE(cmd, nil)
		if err == nil {
			t.Fatal("expected error for zero --max-turns, got nil")
		}
		if !strings.Contains(err.Error(), "max-turns must be positive") {
			t.Errorf("error = %q, want containing 'max-turns must be positive'", err.Error())
		}
	})

	t.Run("negative max-turns flag", func(t *testing.T) {
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{"--max-turns", "-10"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		err := cmd.PreRunE(cmd, nil)
		if err == nil {
			t.Fatal("expected error for negative --max-turns, got nil")
		}
		if !strings.Contains(err.Error(), "max-turns must be positive") {
			t.Errorf("error = %q, want containing 'max-turns must be positive'", err.Error())
		}
	})

	t.Run("flag overrides AO_MAX_TURNS", func(t *testing.T) {
		t.Setenv("AO_MAX_TURNS", "10")
		var opts serveOptions
		cmd := newServeCommandWithOptions(&opts)
		if err := cmd.ParseFlags([]string{"--max-turns", "25"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
		if opts.orchestrator.MaxTurns != 25 {
			t.Errorf("orchestrator.MaxTurns = %d, want 25", opts.orchestrator.MaxTurns)
		}
	})

	t.Run("execute context aborts before run on invalid AO_MAX_TURNS", func(t *testing.T) {
		t.Setenv("AO_MAX_TURNS", "bad-val")
		cmd := newServeCommand()
		cmd.SetArgs([]string{})
		err := cmd.ExecuteContext(t.Context())
		if err == nil {
			t.Fatal("expected error from ExecuteContext, got nil")
		}
		if !strings.Contains(err.Error(), "parsing AO_MAX_TURNS") {
			t.Errorf("error = %q, want containing 'parsing AO_MAX_TURNS'", err.Error())
		}
	})

	t.Run("flag overrides AO_MAX_SESSION_DEPTH", func(t *testing.T) {
		t.Setenv("AO_MAX_SESSION_DEPTH", "1")
		cmd := newServeCommand()
		if err := cmd.ParseFlags([]string{"--max-session-depth", "5"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		if err := cmd.PreRunE(cmd, nil); err != nil {
			t.Fatalf("PreRunE failed unexpectedly: %v", err)
		}
	})

	t.Run("execute context aborts before run on invalid AO_MAX_SESSION_DEPTH", func(t *testing.T) {
		t.Setenv("AO_MAX_SESSION_DEPTH", "invalid")
		cmd := newServeCommand()
		cmd.SetArgs([]string{})
		err := cmd.ExecuteContext(t.Context())
		if err == nil {
			t.Fatal("expected error from ExecuteContext, got nil")
		}
		if !strings.Contains(err.Error(), "parsing AO_MAX_SESSION_DEPTH") {
			t.Errorf("error = %q, want containing 'parsing AO_MAX_SESSION_DEPTH'", err.Error())
		}
	})
}

func TestServeEgressHostsValidation(t *testing.T) {
	cmd := newServeCommand()
	cmd.SetArgs([]string{"--sandbox-egress-hosts", "192.168.1.1"})
	err := cmd.ExecuteContext(t.Context())
	if err == nil {
		t.Fatal("expected error from ExecuteContext, got nil")
	}
	if !strings.Contains(err.Error(), "192.168.1.1") {
		t.Errorf("error = %q, want containing '192.168.1.1'", err.Error())
	}
}

func TestSelectForge(t *testing.T) {
	ghForge := github.New("ghp_secret")
	fallback := forge.NewGitRemote()
	tests := []struct {
		repoURL     string
		githubForge *github.Forge
		wantForge   string
	}{
		{
			repoURL:     "https://github.com/gke-labs/auto-optimizer",
			githubForge: ghForge,
			wantForge:   "github",
		},
		{
			repoURL:     "https://github.com/gke-labs/auto-optimizer",
			githubForge: nil,
			wantForge:   "gitremote",
		},
		{
			repoURL:     "https://gitlab.com/example/repo",
			githubForge: ghForge,
			wantForge:   "gitremote",
		},
		{
			repoURL:     "/path/to/bare/repo.git",
			githubForge: ghForge,
			wantForge:   "gitremote",
		},
	}

	for _, tt := range tests {
		_, name, err := selectForge(tt.repoURL, tt.githubForge, fallback)
		if err != nil {
			t.Errorf("selectForge(%q) returned unexpected error: %v", tt.repoURL, err)
		}
		if name != tt.wantForge {
			t.Errorf("selectForge(%q) = %q; want %q", tt.repoURL, name, tt.wantForge)
		}
	}
}

func TestServeGitHubAppFailFast(t *testing.T) {
	t.Run("missing private key fails fast", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "12345")
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "")
		t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", "")
		cmd := newServeCommand()
		cmd.SetArgs([]string{"--insecure-no-auth"})
		err := cmd.ExecuteContext(t.Context())
		if err == nil || !strings.Contains(err.Error(), "initializing GitHub forge") {
			t.Fatalf("err = %v, want error containing 'initializing GitHub forge'", err)
		}
	})

	t.Run("invalid private key fails fast", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "12345")
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "not-a-valid-key")
		t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", "")
		cmd := newServeCommand()
		cmd.SetArgs([]string{"--insecure-no-auth"})
		err := cmd.ExecuteContext(t.Context())
		if err == nil || !strings.Contains(err.Error(), "initializing GitHub forge") {
			t.Fatalf("err = %v, want error containing 'initializing GitHub forge'", err)
		}
	})
}

func TestRunListCommand_Execution(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	stateStore := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "latency-reduction",
			Body:     "Reduce latency",
			Hash:     "hash-1",
			Schedule: "on-change",
		},
		Hash:       "hash-1",
		BaseCommit: "commit-abc1234",
	}
	if err := stateStore.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	// Create a Pending run
	scheme := runtime.NewScheme()
	_ = herorunv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).Build()

	pendingRun := &state.Run{
		ID:         "20261030T120000Z-0001",
		Repo:       repo.Key(),
		GoalName:   "latency-reduction",
		Phase:      state.PhasePending,
		BaseCommit: "commit-abc1234",
	}
	hr := runpkg.HeroRunFromRun(pendingRun)
	hr.Namespace = session.DefaultNamespace
	hr.Spec.Repository = repo.URL
	if err := fakeClient.Create(ctx, hr); err != nil {
		t.Fatalf("Create HeroRun: %v", err)
	}

	// Start test gRPC server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer listener.Close()

	sessionStore := session.NewFileStore(dir)
	grpcServer := grpc.NewServer()
	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessionStore,
		StateStore:     stateStore,
		DefaultToolset: "basic",
		KubeClient:     fakeClient,
		Namespace:      session.DefaultNamespace,
		HarnessFactory: func(_ string) (*session.Harness, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	// Execute `ao run list --repo <url> --server <addr>`
	cmd := newRunCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"list", "--repo", repo.URL, "--server", listener.Addr().String()})

	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatalf("cmd.ExecuteContext: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "latency-reduction") {
		t.Errorf("expected output to contain goal name 'latency-reduction', got:\n%s", output)
	}
	if !strings.Contains(output, "Pending") {
		t.Errorf("expected output to contain phase 'Pending', got:\n%s", output)
	}
	if !strings.Contains(output, "commit-abc1234") {
		t.Errorf("expected output to contain base commit 'commit-abc1234', got:\n%s", output)
	}
}

func TestRunGetCommand_Execution(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	stateStore := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	scheme := runtime.NewScheme()
	_ = herorunv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).Build()

	runID := "20261030T100000Z-1234"
	runRecord := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "latency-reduction",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-abc1234",
		SandboxName: "run-20261030t100000z-1234",
	}
	hr := runpkg.HeroRunFromRun(runRecord)
	hr.Namespace = session.DefaultNamespace
	hr.Spec.Repository = repo.URL
	if err := fakeClient.Create(ctx, hr); err != nil {
		t.Fatalf("Create HeroRun: %v", err)
	}

	if err := stateStore.AppendEvent(ctx, repo.Key(), runID,
		state.Event{
			Time:    time.Now().UTC(),
			Phase:   state.PhasePreparing,
			Kind:    "command",
			Message: "Cloned repo",
		},
		state.Event{
			Time:    time.Now().UTC(),
			Phase:   state.PhaseWorking,
			Kind:    "phase",
			Message: "Entered working phase",
		},
	); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	// Start test gRPC server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer listener.Close()

	sessionStore := session.NewFileStore(dir)
	grpcServer := grpc.NewServer()
	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessionStore,
		StateStore:     stateStore,
		DefaultToolset: "basic",
		KubeClient:     fakeClient,
		Namespace:      session.DefaultNamespace,
		HarnessFactory: func(_ string) (*session.Harness, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	// Execute `ao run get <id> --server <addr>`
	cmd := newRunCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"get", runID, "--server", listener.Addr().String()})

	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatalf("cmd.ExecuteContext: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, runID) {
		t.Errorf("expected output to contain run ID %q, got:\n%s", runID, output)
	}
	if !strings.Contains(output, "latency-reduction") {
		t.Errorf("expected output to contain goal 'latency-reduction', got:\n%s", output)
	}
	if !strings.Contains(output, "Working") {
		t.Errorf("expected output to contain phase 'Working', got:\n%s", output)
	}
	if !strings.Contains(output, "run-20261030t100000z-1234") {
		t.Errorf("expected output to contain sandbox name 'run-20261030t100000z-1234', got:\n%s", output)
	}
	if !strings.Contains(output, "Cloned repo") {
		t.Errorf("expected output to contain event message 'Cloned repo', got:\n%s", output)
	}

	// Also verify --json output
	jsonCmd := newRunCommand()
	var jsonBuf bytes.Buffer
	jsonCmd.SetOut(&jsonBuf)
	jsonCmd.SetErr(&jsonBuf)
	jsonCmd.SetArgs([]string{"get", runID, "--server", listener.Addr().String(), "--json"})

	if err := jsonCmd.ExecuteContext(ctx); err != nil {
		t.Fatalf("jsonCmd.ExecuteContext: %v", err)
	}

	jsonOutput := jsonBuf.String()
	if !strings.Contains(jsonOutput, "sandboxName") || !strings.Contains(jsonOutput, "run-20261030t100000z-1234") {
		t.Errorf("expected JSON to contain sandboxName and 'run-20261030t100000z-1234', got:\n%s", jsonOutput)
	}
}

func TestServeFlags_DirectBinding(t *testing.T) {
	var opts serveOptions
	cmd := newServeCommandWithOptions(&opts)

	args := []string{
		"--max-turns", "42",
		"--max-concurrent-runs", "8",
		"--git-author", "Test Author <test@example.com>",
		"--public-url", "https://custom.example.com",
		"--ready-timeout", "5m",
		"--verify-timeout", "15m",
		"--max-run-duration", "2h",
		"--namespace", "custom-ns",
		"--image", "custom-img:tag",
		"--homedir", "/custom/home",
		"--tool-timeout", "4m",
	}

	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}

	// Verify values reached the option types directly without intermediate copies
	if opts.orchestrator.MaxTurns != 42 {
		t.Errorf("orchestrator.MaxTurns = %d, want 42", opts.orchestrator.MaxTurns)
	}
	if opts.orchestrator.MaxConcurrentRuns != 8 {
		t.Errorf("orchestrator.MaxConcurrentRuns = %d, want 8", opts.orchestrator.MaxConcurrentRuns)
	}
	if opts.git.Author != "Test Author <test@example.com>" {
		t.Errorf("git.Author = %q, want 'Test Author <test@example.com>'", opts.git.Author)
	}
	if opts.orchestrator.PublicURL != "https://custom.example.com" {
		t.Errorf("orchestrator.PublicURL = %q, want 'https://custom.example.com'", opts.orchestrator.PublicURL)
	}
	if opts.orchestrator.ReadyTimeout != 5*time.Minute {
		t.Errorf("orchestrator.ReadyTimeout = %v, want 5m", opts.orchestrator.ReadyTimeout)
	}
	if opts.orchestrator.VerifyTimeout != 15*time.Minute {
		t.Errorf("orchestrator.VerifyTimeout = %v, want 15m", opts.orchestrator.VerifyTimeout)
	}
	if opts.orchestrator.DefaultMaxDuration != 2*time.Hour {
		t.Errorf("orchestrator.DefaultMaxDuration = %v, want 2h", opts.orchestrator.DefaultMaxDuration)
	}
	if opts.sandbox.Namespace != "custom-ns" {
		t.Errorf("sandbox.Namespace = %q, want 'custom-ns'", opts.sandbox.Namespace)
	}
	if opts.sandbox.Image != "custom-img:tag" {
		t.Errorf("sandbox.Image = %q, want 'custom-img:tag'", opts.sandbox.Image)
	}
	if opts.sandbox.HomeDir != "/custom/home" {
		t.Errorf("sandbox.HomeDir = %q, want '/custom/home'", opts.sandbox.HomeDir)
	}
	if opts.sandbox.ToolTimeout != 4*time.Minute {
		t.Errorf("sandbox.ToolTimeout = %v, want 4m", opts.sandbox.ToolTimeout)
	}
}

func TestServeSandboxEnvironmentOverrides(t *testing.T) {
	t.Setenv("SANDBOX_NAMESPACE", "env-ns")
	t.Setenv("SANDBOX_IMAGE", "env-img:v1")
	t.Setenv("SANDBOX_HOME_DIR", "/env/home")

	var opts serveOptions
	cmd := newServeCommandWithOptions(&opts)
	if err := cmd.ParseFlags([]string{}); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}

	if opts.sandbox.Namespace != "env-ns" {
		t.Errorf("sandbox.Namespace = %q, want 'env-ns'", opts.sandbox.Namespace)
	}
	if opts.sandbox.Image != "env-img:v1" {
		t.Errorf("sandbox.Image = %q, want 'env-img:v1'", opts.sandbox.Image)
	}
	if opts.sandbox.HomeDir != "/env/home" {
		t.Errorf("sandbox.HomeDir = %q, want '/env/home'", opts.sandbox.HomeDir)
	}
}

func TestSessionRunCommand_DefaultHomeDir(t *testing.T) {
	t.Setenv("SANDBOX_HOME_DIR", "")

	cmd := newSessionRunCommand()
	f := cmd.Flag("homedir")
	if f == nil {
		t.Fatal("expected --homedir flag on session run command")
	}
	if f.DefValue != session.DefaultHomeDir {
		t.Errorf("homedir default = %q, want %q", f.DefValue, session.DefaultHomeDir)
	}
}

func TestSetupControllersWithManager(t *testing.T) {
	mgr, err := runpkg.NewManager(&rest.Config{Host: "http://127.0.0.1:65534"}, session.DefaultNamespace)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	dir := t.TempDir()
	stateStore := state.NewFileStore(dir)
	sessionStore := session.NewFileStore(dir)
	harness := session.NewHarness(nil, nil, nil, session.RunOptions{Namespace: session.DefaultNamespace})

	var opts serveOptions
	opts.InitDefaults()
	opts.toolset = "basic"

	orchOpts := opts.orchestrator
	orchOpts.Store = stateStore
	orchOpts.SessionStore = sessionStore
	orchOpts.Harness = harness
	orchOpts.KubeClient = mgr.GetClient()

	orch, err := runpkg.NewOrchestrator(orchOpts)
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("orchestrator.SetupWithManager: %v", err)
	}
	if err := setupSessionController(mgr, sessionStore, stateStore, func(string) (*session.Harness, error) {
		return harness, nil
	}, orch, opts); err != nil {
		t.Fatalf("setupSessionController: %v", err)
	}
}
