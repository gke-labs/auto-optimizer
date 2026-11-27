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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/forge/github"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/portal"
	"github.com/gke-labs/auto-optimizer/pkg/run"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/sessioncontroller"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/syncer"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
	"github.com/gke-labs/auto-optimizer/pkg/web"
	"github.com/gke-labs/auto-optimizer/pkg/web/auth"
)

type serveOptions struct {
	listen         string
	http           string
	toolset        string
	stateDir       string
	repos          []string
	insecureNoAuth bool

	sandbox      session.SandboxOptions
	git          session.GitOptions
	orchestrator run.OrchestratorOptions
	portal       portal.Options
	github       auth.GitHubOptions
}

func (opts *serveOptions) InitDefaults() {
	opts.sandbox.InitDefaults()
	opts.git.InitDefaults()
	opts.orchestrator.InitDefaults()
	opts.portal.InitDefaults()
	opts.github.InitDefaults()
}

func (opts *serveOptions) Validate() error {
	if opts.github.ClientID == "" && !opts.insecureNoAuth {
		return errors.New("GitHub client ID is required unless --insecure-no-auth is set")
	}
	if opts.github.ClientID != "" {
		if opts.github.PublicURL == "" {
			opts.github.PublicURL = opts.orchestrator.PublicURL
		}
		if err := opts.github.Validate(); err != nil {
			return fmt.Errorf("invalid GitHub auth configuration: %w", err)
		}
		sessionKey := os.Getenv("AO_SESSION_KEY")
		if len(sessionKey) < auth.MinKeyLength {
			return fmt.Errorf("AO_SESSION_KEY is missing or shorter than %d bytes", auth.MinKeyLength)
		}
	}
	return nil
}

func newServeCommand() *cobra.Command {
	var opts serveOptions
	return newServeCommandWithOptions(&opts)
}

func newServeCommandWithOptions(opts *serveOptions) *cobra.Command {
	opts.InitDefaults()
	if envNamespace := os.Getenv("SANDBOX_NAMESPACE"); envNamespace != "" {
		opts.sandbox.Namespace = envNamespace
	}
	if envImage := os.Getenv("SANDBOX_IMAGE"); envImage != "" {
		opts.sandbox.Image = envImage
	}
	if envHomeDir := os.Getenv("SANDBOX_HOME_DIR"); envHomeDir != "" {
		opts.sandbox.HomeDir = envHomeDir
	}

	if envPortalAddr := os.Getenv("PORTAL_CONTROLLER_ADDR"); envPortalAddr != "" {
		opts.portal.ControllerAddr = envPortalAddr
	}
	if envPortalCA := os.Getenv("PORTAL_CONTROLLER_CA_FILE"); envPortalCA != "" {
		opts.portal.CAFile = envPortalCA
	}
	if envPortalToken := os.Getenv("PORTAL_TOKEN_FILE"); envPortalToken != "" {
		opts.portal.TokenFile = envPortalToken
	}
	if envEgressHosts := os.Getenv("SANDBOX_EGRESS_HOSTS"); envEgressHosts != "" {
		for _, h := range strings.Split(envEgressHosts, ",") {
			h = strings.TrimSpace(h)
			if h != "" {
				opts.portal.EgressHosts = append(opts.portal.EgressHosts, h)
			}
		}
	}
	defaultListen := os.Getenv("AO_LISTEN")
	if defaultListen == "" {
		defaultListen = "127.0.0.1:8081"
	}

	defaultHTTP := os.Getenv("AO_HTTP")
	if defaultHTTP == "" {
		defaultHTTP = "127.0.0.1:8080"
	}

	defaultToolset := os.Getenv("TOOLSET")
	if defaultToolset == "" {
		defaultToolset = "basic"
	}

	userHome, err := os.UserHomeDir()
	if err != nil {
		userHome = ""
	}
	defaultStateDir := filepath.Join(userHome, ".local", "auto-optimizer")

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the session API server",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if envMaxTurns := os.Getenv("AO_MAX_TURNS"); envMaxTurns != "" {
				v, err := strconv.Atoi(envMaxTurns)
				if err != nil {
					return fmt.Errorf("parsing AO_MAX_TURNS: %w", err)
				}
				if v <= 0 {
					return fmt.Errorf("AO_MAX_TURNS must be positive, got %d", v)
				}
				if !cmd.Flags().Changed("max-turns") {
					opts.orchestrator.MaxTurns = v
				}
			}
			if opts.orchestrator.MaxTurns <= 0 {
				return fmt.Errorf("max-turns must be positive, got %d", opts.orchestrator.MaxTurns)
			}
			if envMaxDepth := os.Getenv("AO_MAX_SESSION_DEPTH"); envMaxDepth != "" {
				v, err := strconv.Atoi(envMaxDepth)
				if err != nil {
					return fmt.Errorf("parsing AO_MAX_SESSION_DEPTH: %w", err)
				}
				if !cmd.Flags().Changed("max-session-depth") {
					opts.orchestrator.MaxSessionDepth = v
				}
			}
			if !cmd.Flags().Changed("git-author") {
				if envGitAuthor := os.Getenv("AO_GIT_AUTHOR"); envGitAuthor != "" {
					opts.git.Author = envGitAuthor
				}
			}
			if err := opts.git.Validate(); err != nil {
				return fmt.Errorf("invalid --git-author: %w", err)
			}
			if !cmd.Flags().Changed("public-url") {
				if envPublicURL := os.Getenv("AO_PUBLIC_URL"); envPublicURL != "" {
					opts.orchestrator.PublicURL = envPublicURL
				}
			}
			if opts.orchestrator.PublicURL != "" {
				u, err := url.Parse(opts.orchestrator.PublicURL)
				if err != nil {
					return fmt.Errorf("invalid --public-url: %w", err)
				}
				if u.Scheme != "http" && u.Scheme != "https" {
					return fmt.Errorf("invalid --public-url: scheme must be http or https, got %q", u.Scheme)
				}
				if u.Host == "" {
					return fmt.Errorf("invalid --public-url: host must not be empty")
				}
				opts.orchestrator.PublicURL = strings.TrimRight(opts.orchestrator.PublicURL, "/")
			}
			if err := opts.portal.Validate(); err != nil {
				return fmt.Errorf("invalid portal configuration: %w", err)
			}
			if opts.github.PublicURL == "" {
				opts.github.PublicURL = opts.orchestrator.PublicURL
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(cmd.Context(), *opts)
		},
	}

	cmd.Flags().StringVar(&opts.listen, "listen", defaultListen, "Address and port to listen on; must not be exposed to untrusted networks")
	cmd.Flags().StringVar(&opts.http, "http", defaultHTTP, "Address and port for the HTTP web UI; must not be exposed to untrusted networks")
	cmd.Flags().StringVar(&opts.toolset, "toolset", defaultToolset, fmt.Sprintf("Default toolset to expose to the LLM (%s)", strings.Join(toolsets.Names(), ", ")))
	cmd.Flags().StringVar(&opts.stateDir, "state-dir", defaultStateDir, "Directory where session state and snapshots are saved")
	cmd.Flags().StringArrayVar(&opts.repos, "repo", nil, "Repository URL to register with the forge (repeatable, or AO_REPOS env var)")
	cmd.Flags().BoolVar(&opts.insecureNoAuth, "insecure-no-auth", false, "Disable authentication for the web UI (insecure; for development and testing only)")

	opts.sandbox.AddFlags(cmd.Flags())
	opts.git.AddFlags(cmd.Flags())
	opts.orchestrator.AddFlags(cmd.Flags())
	opts.portal.AddFlags(cmd.Flags())
	opts.github.AddFlags(cmd.Flags())

	return cmd
}

func runServe(ctx context.Context, opts serveOptions) error {
	ctrl.SetLogger(klog.NewKlogr())

	log := scoped.Log(ctx)

	if err := opts.Validate(); err != nil {
		return err
	}
	if opts.github.ClientID == "" && opts.insecureNoAuth {
		log.Info("WARNING: running without authentication (--insecure-no-auth)")
	}

	if err := opts.sandbox.Validate(); err != nil {
		return err
	}

	githubForge, err := github.NewFromEnv()
	if err != nil {
		return fmt.Errorf("initializing GitHub forge: %w", err)
	}

	stateDir := opts.stateDir
	if strings.HasPrefix(stateDir, "~/") {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("getting user home directory: %w", err)
		}
		stateDir = filepath.Join(userHome, stateDir[2:])
	}

	var baseOpts session.RunOptions
	baseOpts.InitDefaults()
	baseOpts.ApplySandbox(opts.sandbox)
	baseOpts.StateDir = stateDir

	signingKey, err := session.LoadOrGenerateSigningKey(filepath.Join(stateDir, "sandbox-signing-key"))
	if err != nil {
		return fmt.Errorf("loading sandbox signing key: %w", err)
	}
	baseOpts.SigningKey = signingKey

	llmClient, err := llm.NewFromEnv(baseOpts.ModelName)
	if err != nil {
		return fmt.Errorf("initializing LLM client: %w", err)
	}

	restConfig, err := session.GetRESTConfig()
	if err != nil {
		return fmt.Errorf("getting Kubernetes configuration: %w", err)
	}

	mgr, err := run.NewManager(restConfig, opts.sandbox.Namespace)
	if err != nil {
		return fmt.Errorf("initializing controller manager: %w", err)
	}

	sandboxClient, err := session.NewSandboxClient(restConfig)
	if err != nil {
		return fmt.Errorf("initializing sandbox client: %w", err)
	}

	sessionStore := session.NewFileStore(stateDir)
	stateStore := state.NewFileStore(stateDir)
	gitRemote := forge.NewGitRemote()
	forgeFor := func(repoURL string) (forge.Forge, error) {
		f, _, err := selectForge(repoURL, githubForge, gitRemote)
		return f, err
	}

	var repoURLs []string
	repoURLs = append(repoURLs, opts.repos...)
	if envRepos := os.Getenv("AO_REPOS"); envRepos != "" {
		for _, r := range strings.Split(envRepos, ",") {
			if r = strings.TrimSpace(r); r != "" {
				repoURLs = append(repoURLs, r)
			}
		}
	}

	for _, repoURL := range repoURLs {
		key, err := state.ParseRepoURL(repoURL)
		if err != nil {
			log.Error(err, "invalid repository URL", "url", repoURL)
			continue
		}
		_, err = stateStore.GetRepo(ctx, key)
		if err != nil {
			if errors.Is(err, state.ErrNotFound) {
				repoForge, forgeKind, err := selectForge(repoURL, githubForge, gitRemote)
				if err != nil {
					return fmt.Errorf("selecting forge for %s: %w", repoURL, err)
				}
				defBranch, err := repoForge.DefaultBranch(ctx, repoURL)
				if err != nil {
					log.Error(err, "querying default branch; defaulting to main", "url", repoURL)
					defBranch = "main"
				}
				r := &state.Repo{
					URL:            repoURL,
					Host:           key.Host,
					Owner:          key.Owner,
					Name:           key.Name,
					DefaultBranch:  defBranch,
					ForgeKind:      forgeKind,
					RegisteredTime: time.Now(),
				}
				if err := stateStore.PutRepo(ctx, r); err != nil {
					log.Error(err, "registering repository", "url", repoURL)
				} else {
					log.Info("registered repository", "repo", repoURL, "forge", forgeKind, "defaultBranch", defBranch)
				}
			} else {
				log.Error(err, "checking existing repository", "url", repoURL)
			}
		}
	}

	syncerEngine, err := syncer.New(syncer.Options{
		Store: stateStore,
		Forge: routingForge(forgeFor),
	})
	if err != nil {
		return fmt.Errorf("initializing syncer: %w", err)
	}
	go func() {
		if err := syncerEngine.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error(err, "syncer loop error")
		}
	}()

	harnessFactory := func(toolsetName string) (*session.Harness, error) {
		toolset, err := toolsets.Get(toolsetName)
		if err != nil {
			return nil, err
		}

		harnessOpts := baseOpts
		harnessOpts.SystemPrompt = toolset.SystemPrompt()
		harnessOpts.Image = opts.sandbox.ResolveImage(toolset.DefaultImage())

		toolsRegistry := tools.NewRegistry()
		toolsRegistry.ToolTimeout = harnessOpts.ToolTimeout
		toolset.RegisterTools(toolsRegistry)

		return session.NewHarness(llmClient, sandboxClient, toolsRegistry, harnessOpts), nil
	}

	orchestratorHarness, err := harnessFactory(opts.toolset)
	if err != nil {
		return fmt.Errorf("initializing orchestrator harness: %w", err)
	}

	orchOpts := opts.orchestrator
	orchOpts.Store = stateStore
	orchOpts.SessionStore = sessionStore
	orchOpts.Harness = orchestratorHarness
	orchOpts.KubeClient = mgr.GetClient()
	orchOpts.GitAuthor = opts.git.Author
	orchOpts.ForgeFor = forgeFor
	if opts.portal.Enabled() {
		portalClient, err := portal.New(opts.portal)
		if err != nil {
			return fmt.Errorf("initializing portal client: %w", err)
		}
		defer func() {
			if closeErr := portalClient.Close(); closeErr != nil {
				log.V(2).Info("closing portal client", "error", closeErr)
			}
		}()
		orchOpts.PortalBinder = portalClient
		orchOpts.PolicyFactory = portal.HostListPolicyFactory(opts.portal.EgressHosts)
	}
	orchestrator, err := run.NewOrchestrator(orchOpts)
	if err != nil {
		return fmt.Errorf("initializing orchestrator: %w", err)
	}
	if err := orchestrator.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up orchestrator controller: %w", err)
	}
	if err := setupSessionController(mgr, sessionStore, stateStore, harnessFactory, orchestrator, opts); err != nil {
		return err
	}

	scheduler, err := run.NewScheduler(run.SchedulerOptions{
		Store:        stateStore,
		Orchestrator: orchestrator,
		ForgeFor:     forgeFor,
	})
	if err != nil {
		return fmt.Errorf("initializing scheduler: %w", err)
	}
	go func() {
		if err := scheduler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error(err, "scheduler loop error")
		}
	}()

	go func() {
		if err := mgr.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error(err, "controller manager error")
		}
	}()

	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessionStore,
		StateStore:     stateStore,
		DefaultToolset: opts.toolset,
		HarnessFactory: harnessFactory,
		KubeClient:     mgr.GetClient(),
		Namespace:      opts.sandbox.Namespace,
	})
	if err != nil {
		return fmt.Errorf("initializing session service server: %w", err)
	}

	listener, err := net.Listen("tcp", opts.listen)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", opts.listen, err)
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			log.V(2).Info("failed to close listener", "error", closeErr)
		}
	}()

	grpcServer := grpc.NewServer()
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)

	log.Info("session server listening", "listen", opts.listen, "stateDir", stateDir)

	var httpListener net.Listener
	var httpServer *http.Server
	if opts.http != "" {
		var sessionKey []byte
		if opts.github.ClientID != "" {
			sessionKey = []byte(os.Getenv("AO_SESSION_KEY"))
		}
		webHandler, err := web.NewServer(srv, stateStore, scheduler, web.ServerOptions{
			RequireSignIn: opts.github.ClientID != "",
			SessionKey:    sessionKey,
			Orchestrator:  orchestrator,
			KubeClient:    mgr.GetClient(),
			Namespace:     opts.sandbox.Namespace,
		})
		if err != nil {
			return fmt.Errorf("initializing web server: %w", err)
		}
		var handler http.Handler = webHandler
		if opts.github.ClientID != "" {
			h, err := auth.NewGitHubHandler(opts.github, sessionKey, time.Now, webHandler)
			if err != nil {
				return fmt.Errorf("initializing auth handler: %w", err)
			}
			handler = h
		}
		httpListener, err = net.Listen("tcp", opts.http)
		if err != nil {
			return fmt.Errorf("failed to listen on HTTP %s: %w", opts.http, err)
		}
		defer func() {
			if closeErr := httpListener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				log.V(2).Info("failed to close HTTP listener", "error", closeErr)
			}
		}()
		httpServer = &http.Server{Handler: handler}
		log.Info("web UI listening", "http", opts.http)
	}

	errCh := make(chan error, 2)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
		}
	}()

	if httpServer != nil {
		go func() {
			if err := httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}

	select {
	case <-ctx.Done():
		log.Info("shutting down session server...")
		grpcServer.GracefulStop()
		if httpServer != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := httpServer.Shutdown(shutdownCtx); err != nil {
				log.V(2).Info("HTTP server shutdown error", "error", err)
			}
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func setupSessionController(
	mgr ctrl.Manager,
	sessionStore session.Store,
	stateStore state.Store,
	harnessFactory func(string) (*session.Harness, error),
	orchestrator *run.Orchestrator,
	opts serveOptions,
) error {
	reconciler, err := sessioncontroller.NewReconciler(sessioncontroller.Options{
		Client:          mgr.GetClient(),
		SessionStore:    sessionStore,
		StateStore:      stateStore,
		HarnessFactory:  harnessFactory,
		SandboxOwnerFor: orchestrator.SandboxOwnerForSession,
		DefaultToolset:  opts.toolset,
		DefaultMaxTurns: opts.orchestrator.MaxTurns,
		DefaultMaxDepth: opts.orchestrator.MaxSessionDepth,
		MaxConcurrent:   max(opts.orchestrator.MaxConcurrentRuns*4, 16),
	})
	if err != nil {
		return fmt.Errorf("initializing session controller: %w", err)
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up session controller: %w", err)
	}
	return nil
}

type routingForge func(repoURL string) (forge.Forge, error)

func (fn routingForge) ListRefs(ctx context.Context, repoURL string) (map[string]string, error) {
	f, err := fn(repoURL)
	if err != nil {
		return nil, err
	}
	return f.ListRefs(ctx, repoURL)
}

func (fn routingForge) FetchTree(ctx context.Context, repoURL, commit, prefix string) (fs.FS, error) {
	f, err := fn(repoURL)
	if err != nil {
		return nil, err
	}
	return f.FetchTree(ctx, repoURL, commit, prefix)
}

func (fn routingForge) DefaultBranch(ctx context.Context, repoURL string) (string, error) {
	f, err := fn(repoURL)
	if err != nil {
		return "", err
	}
	return f.DefaultBranch(ctx, repoURL)
}

func selectForge(repoURL string, githubForge *github.Forge, fallback forge.Forge) (forge.Forge, string, error) {
	key, err := state.ParseRepoURL(repoURL)
	if err != nil {
		return nil, "", fmt.Errorf("parsing repository URL %q: %w", repoURL, err)
	}
	if (key.Host == "github.com" || strings.HasSuffix(key.Host, ".github.com")) && githubForge != nil {
		return githubForge, "github", nil
	}
	if fallback != nil {
		return fallback, "gitremote", nil
	}
	return forge.NewGitRemote(), "gitremote", nil
}
