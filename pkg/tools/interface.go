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

// Package tools provides the tool interface and registry for sandboxed tool execution.
// Forked from kubernetes-sigs/agent-sandbox/examples/sandboxed-tools (commit f83f2d87c1aeeb90407d8cfe9a0c1fad74044304).
package tools

import (
	"context"
	"io"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
)

// Tool defines the interface for all tools that an LLM can invoke.
type Tool interface {
	// Schema returns the schema of the tool, including its name, description, and parameters.
	// The LLM uses this schema to understand how and when to invoke the tool.
	Schema() llm.Tool

	// Run executes the tool's action inside the provided sandbox and returns the result message.
	Run(ctx context.Context, sandbox Sandbox) (llm.Message, error)
}

// Sandbox executes commands in an isolated environment.
type Sandbox interface {
	// ExecCommand runs a command inside the sandbox container using the specified options.
	// When options omit Stdout or Stderr, ExecCommand captures those streams and returns them
	// in the returned result. ExecCommand returns command exit codes in the result instead
	// of returning them as errors.
	ExecCommand(ctx context.Context, opts ExecCommandOptions) (*ExecCommandResult, error)
}

// ExecCommandResult holds the output and exit code from a run command.
type ExecCommandResult struct {
	// Stdout holds the captured standard output.
	// ExecCommand only populates this field when ExecCommandOptions.Stdout is nil.
	Stdout string

	// Stderr holds the captured standard error.
	// ExecCommand only populates this field when ExecCommandOptions.Stderr is nil.
	Stderr string

	// ExitCode is the exit status that the process returned.
	ExitCode int
}

// ExecCommandOptions specifies the execution settings for a sandbox command.
type ExecCommandOptions struct {
	// Command contains the executable name and its arguments, such as []string{"sh", "-c", "uname -a"}.
	Command []string

	// Stdin provides input to the standard input stream of the command.
	Stdin io.Reader

	// Stdout receives the standard output stream of the command.
	// When Stdout is nil, ExecCommand captures the output and returns it in the result.
	Stdout io.Writer

	// Stderr receives the standard error stream of the command.
	// When Stderr is nil, ExecCommand captures the error output and returns it in the result.
	Stderr io.Writer

	// Stream forces streamed execution using the underlying process service even when all stream fields are nil.
	Stream bool
}
