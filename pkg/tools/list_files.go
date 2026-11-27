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

package tools

import (
	"context"
	"fmt"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
)

// ListFilesTool lists files and directories inside the sandbox.
// This tool implements the [Tool] interface. It wraps the standard 'ls' command.
type ListFilesTool struct {
	// Path specifies the directory to list.
	// If empty, the tool lists the current directory.
	Path string `json:"path"`
}

// Schema returns the JSON schema that describes the file listing tool to the LLM.
func (t *ListFilesTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name:        "ls",
			Description: "List files in a directory.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{
						"type":        "string",
						"description": "Directory to list (default: current directory)",
					},
				},
			},
		},
	}
}

// Run executes the file listing command in the sandbox and returns the list of files.
func (t *ListFilesTool) Run(ctx context.Context, sandbox Sandbox) (llm.Message, error) {
	log := scoped.Log(ctx)

	path := t.Path
	if path == "" {
		path = "."
	}
	log.Info("listing files in sandbox", "path", path)
	res, err := sandbox.ExecCommand(ctx, ExecCommandOptions{
		// The -- separator makes execution more robust e.g. if path is "--help"
		Command: []string{"ls", "-p", "--", path},
	})
	if err != nil {
		return llm.Message{}, err
	}

	content := res.Stdout

	if res.ExitCode != 0 {
		content = fmt.Sprintf("Failed to list %q:\nstdout:\n%s\nstderr:\n%s\nexit_code: %d", path, res.Stdout, res.Stderr, res.ExitCode)
	}

	result := llm.Message{
		Content: &content,
	}

	return result, nil
}
