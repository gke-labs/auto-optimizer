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
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

type serveFlags struct {
	listen      string
	namespace   string
	toolset     string
	image       string
	homeDir     string
	toolTimeout time.Duration
	stateDir    string
}

func newServeCommand() *cobra.Command {
	var flags serveFlags

	defaultListen := os.Getenv("AO_LISTEN")
	if defaultListen == "" {
		defaultListen = "127.0.0.1:8081"
	}

	defaultNamespace := os.Getenv("SANDBOX_NAMESPACE")
	if defaultNamespace == "" {
		defaultNamespace = "ao-sandboxes"
	}

	defaultToolset := os.Getenv("TOOLSET")
	if defaultToolset == "" {
		defaultToolset = "basic"
	}

	defaultHomeDir := os.Getenv("SANDBOX_HOME_DIR")
	if defaultHomeDir == "" {
		defaultHomeDir = "/home/clawtainer"
	}

	defaultImage := os.Getenv("SANDBOX_IMAGE")

	userHome, err := os.UserHomeDir()
	if err != nil {
		userHome = ""
	}
	defaultStateDir := filepath.Join(userHome, ".local", "auto-optimizer")

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the session API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServe(cmd.Context(), flags)
		},
	}

	cmd.Flags().StringVar(&flags.listen, "listen", defaultListen, "Address and port to listen on; must not be exposed to untrusted networks")
	cmd.Flags().StringVar(&flags.namespace, "namespace", defaultNamespace, "Kubernetes namespace where sandbox pods are created")
	cmd.Flags().StringVar(&flags.toolset, "toolset", defaultToolset, fmt.Sprintf("Default toolset to expose to the LLM (%s)", strings.Join(toolsets.Names(), ", ")))
	cmd.Flags().StringVar(&flags.image, "image", defaultImage, "Default sandbox container image (overrides toolset default)")
	cmd.Flags().StringVar(&flags.homeDir, "homedir", defaultHomeDir, "Home directory inside the sandbox persisted via snapshot/restore")
	cmd.Flags().DurationVar(&flags.toolTimeout, "tool-timeout", session.DefaultToolTimeout, "Maximum duration for a single tool execution")
	cmd.Flags().StringVar(&flags.stateDir, "state-dir", defaultStateDir, "Directory where session state and snapshots are saved")

	return cmd
}

func runServe(ctx context.Context, flags serveFlags) error {
	log := scoped.Log(ctx)

	if flags.homeDir == "" {
		return errors.New("homedir must not be empty")
	}

	stateDir := flags.stateDir
	if strings.HasPrefix(stateDir, "~/") {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("getting user home directory: %w", err)
		}
		stateDir = filepath.Join(userHome, stateDir[2:])
	}

	var baseOpts session.RunOptions
	baseOpts.InitDefaults()
	baseOpts.Namespace = flags.namespace
	baseOpts.HomeDir = flags.homeDir
	baseOpts.ToolTimeout = flags.toolTimeout
	baseOpts.StateDir = stateDir

	llmClient, err := llm.NewFromEnv(baseOpts.ModelName)
	if err != nil {
		return fmt.Errorf("initializing LLM client: %w", err)
	}

	restConfig, err := session.GetRESTConfig()
	if err != nil {
		return fmt.Errorf("getting Kubernetes configuration: %w", err)
	}

	sandboxClient, err := session.NewSandboxClient(restConfig)
	if err != nil {
		return fmt.Errorf("initializing sandbox client: %w", err)
	}

	sessionStore := session.NewFileStore(stateDir)

	harnessFactory := func(toolsetName string) (*session.Harness, error) {
		toolset, err := toolsets.Get(toolsetName)
		if err != nil {
			return nil, err
		}

		opts := baseOpts
		opts.SystemPrompt = toolset.SystemPrompt()

		image := flags.image
		if image == "" {
			image = toolset.DefaultImage()
		}
		if image == "" {
			image = session.DefaultImage
		}
		opts.Image = image

		toolsRegistry := tools.NewRegistry()
		toolsRegistry.ToolTimeout = opts.ToolTimeout
		toolset.RegisterTools(toolsRegistry)

		return session.NewHarness(llmClient, sandboxClient, toolsRegistry, opts), nil
	}

	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessionStore,
		DefaultToolset: flags.toolset,
		HarnessFactory: harnessFactory,
	})
	if err != nil {
		return fmt.Errorf("initializing session service server: %w", err)
	}

	listener, err := net.Listen("tcp", flags.listen)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", flags.listen, err)
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			log.V(2).Info("failed to close listener", "error", closeErr)
		}
	}()

	grpcServer := grpc.NewServer()
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)

	log.Info("session server listening", "listen", flags.listen, "stateDir", stateDir)

	errCh := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down session server...")
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}
