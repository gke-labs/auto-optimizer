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

// Package main is the entrypoint for the ao CLI.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	rootCmd := newRootCommand()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "ao: %v\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "ao",
		Short:        "auto-optimizer control plane and CLI",
		SilenceUsage: true,
	}

	klogFlagSet := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(klogFlagSet)
	rootCmd.PersistentFlags().AddGoFlagSet(klogFlagSet)

	rootCmd.AddCommand(newServeCommand())
	rootCmd.AddCommand(newSessionCommand())
	return rootCmd
}

func newSessionCommand() *cobra.Command {
	sessionCmd := &cobra.Command{
		Use:   "session",
		Short: "Manage and run agent sessions",
	}

	sessionCmd.AddCommand(newSessionRunCommand())
	sessionCmd.AddCommand(newSessionListCommand())
	sessionCmd.AddCommand(newSessionGetCommand())
	sessionCmd.AddCommand(newSessionSendCommand())
	return sessionCmd
}

type sessionRunFlags struct {
	sessionName string
	namespace   string
	toolset     string
	image       string
	homeDir     string
	toolTimeout time.Duration
	stateDir    string
	prompt      string
	yolo        bool
}

func newSessionRunCommand() *cobra.Command {
	var flags sessionRunFlags

	defaultSessionName := os.Getenv("SESSION_NAME")
	if defaultSessionName == "" {
		defaultSessionName = "default"
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
		Use:   "run",
		Short: "Run an agent session in a sandboxed environment",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSession(cmd.Context(), flags)
		},
	}

	cmd.Flags().StringVar(&flags.sessionName, "session", defaultSessionName, "Session name")
	cmd.Flags().StringVar(&flags.namespace, "namespace", defaultNamespace, "Kubernetes namespace where sandbox pods are created")
	cmd.Flags().StringVar(&flags.toolset, "toolset", defaultToolset, fmt.Sprintf("Toolset to expose to the LLM (%s)", strings.Join(toolsets.Names(), ", ")))
	cmd.Flags().StringVar(&flags.image, "image", defaultImage, "Sandbox container image (overrides toolset default)")
	cmd.Flags().StringVar(&flags.homeDir, "homedir", defaultHomeDir, "Home directory inside the sandbox persisted via snapshot/restore")
	cmd.Flags().DurationVar(&flags.toolTimeout, "tool-timeout", session.DefaultToolTimeout, "Maximum duration for a single tool execution")
	cmd.Flags().StringVar(&flags.stateDir, "state-dir", defaultStateDir, "Directory where session state and snapshots are saved")
	cmd.Flags().StringVar(&flags.prompt, "prompt", "", "Non-interactive single-turn prompt")
	cmd.Flags().BoolVar(&flags.yolo, "yolo", false, "Auto-approve tool calls without prompting")

	return cmd
}

func runSession(ctx context.Context, flags sessionRunFlags) error {
	if flags.homeDir == "" {
		return errors.New("homeDir must not be empty")
	}
	if flags.sessionName == "" {
		return errors.New("sessionName is required")
	}
	if err := session.ValidateSessionName(flags.sessionName); err != nil {
		return fmt.Errorf("invalid sessionName %q: %w", flags.sessionName, err)
	}

	toolset, err := toolsets.Get(flags.toolset)
	if err != nil {
		return err
	}

	stateDir := flags.stateDir
	if strings.HasPrefix(stateDir, "~/") {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("getting user home directory: %w", err)
		}
		stateDir = filepath.Join(userHome, stateDir[2:])
	}

	var opts session.RunOptions
	opts.InitDefaults()
	opts.Namespace = flags.namespace
	opts.HomeDir = flags.homeDir
	opts.ToolTimeout = flags.toolTimeout
	opts.StateDir = stateDir
	opts.SystemPrompt = toolset.SystemPrompt()

	image := flags.image
	if image == "" {
		image = toolset.DefaultImage()
	}
	if image == "" {
		image = session.DefaultImage
	}
	opts.Image = image

	llmClient, err := llm.NewFromEnv(opts.ModelName)
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

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = opts.ToolTimeout
	toolset.RegisterTools(toolsRegistry)

	sessionStore := session.NewFileStore(stateDir)
	harness := session.NewHarness(llmClient, sandboxClient, toolsRegistry, opts)

	sess, err := harness.LoadSession(ctx, sessionStore, flags.sessionName)
	if errors.Is(err, session.ErrNotFound) {
		sess, err = harness.NewSession(ctx, sessionStore, flags.sessionName, session.Metadata{Toolset: toolset.Name()})
	}
	if err != nil {
		return fmt.Errorf("building session: %w", err)
	}

	events := &consoleEvents{
		yolo:   flags.yolo,
		reader: bufio.NewReader(os.Stdin),
	}

	if flags.prompt != "" {
		return runOneShot(ctx, harness, sess, flags.prompt, events)
	}

	return runREPL(ctx, harness, sess, events)
}

func runOneShot(ctx context.Context, harness *session.Harness, sess *session.Session, prompt string, events session.TurnEvents) error {
	if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
		return fmt.Errorf("adding system prompt: %w", err)
	}
	return harness.RunTurn(ctx, sess, prompt, events)
}

func runREPL(ctx context.Context, harness *session.Harness, sess *session.Session, events session.TurnEvents) error {
	isNewSession := len(sess.Messages()) == 0

	if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
		return fmt.Errorf("adding system prompt: %w", err)
	}

	if isNewSession {
		fmt.Println("================================================================================")
		fmt.Println("Welcome to the auto-optimizer session runner!")
		fmt.Printf("Session Name: %s\n", sess.Name)
		fmt.Println("Type your message (or '/exit' or '/quit' to quit):")
		fmt.Println("================================================================================")
	} else {
		fmt.Println("================================================================================")
		fmt.Printf("Resumed session %q with %d messages in history:\n", sess.Name, len(sess.Messages()))
		fmt.Println("================================================================================")
		for _, msg := range sess.Messages() {
			if msg.Role == "user" && msg.Content != nil {
				fmt.Printf("User> %s\n", *msg.Content)
			} else if msg.Role == "assistant" && msg.Content != nil && *msg.Content != "" {
				fmt.Printf("Agent> %s\n", *msg.Content)
			}
		}
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\nUser> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("reading standard input: %w", err)
		}

		input := strings.TrimSpace(line)
		if input == "" {
			continue
		}
		if strings.EqualFold(input, "/exit") || strings.EqualFold(input, "/quit") {
			return nil
		}

		if err := harness.RunTurn(ctx, sess, input, events); err != nil {
			return err
		}
	}
}

type consoleEvents struct {
	yolo   bool
	reader *bufio.Reader
}

func (e *consoleEvents) AssistantMessage(_ context.Context, text string) {
	fmt.Printf("\nAgent> %s\n", text)
}

func (e *consoleEvents) ApproveToolCall(_ context.Context, call llm.ToolCall) (bool, error) {
	if e.yolo {
		return true, nil
	}

	fmt.Printf("\nExecute tool %s with arguments %s? [y/N]: ", call.Function.Name, call.Function.Arguments)
	line, err := e.reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("reading approval response: %w", err)
	}
	resp := strings.TrimSpace(strings.ToLower(line))
	return resp == "y" || resp == "yes", nil
}

func (e *consoleEvents) ToolCallStarted(_ context.Context, _ llm.ToolCall) {}

func (e *consoleEvents) ToolCallFinished(_ context.Context, _ llm.ToolCall, _ error) {}

func (e *consoleEvents) MessageAppended(_ context.Context, _ llm.Message) {}
