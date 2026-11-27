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
	"errors"
	"fmt"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
)

// ReadFileTool reads the contents of a specified file from the sandbox.
// This tool implements the [Tool] interface. It wraps the standard 'cat' command.
type ReadFileTool struct {
	// Path specifies the location of the file to read.
	Path string `json:"path"`
}

// Schema returns the JSON schema that describes the file reading tool to the LLM.
func (t *ReadFileTool) Schema() llm.Tool {
	// Area of future investigation: do the models favor certain names (e.g. antigravity uses view_file)
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name:        "read",
			Description: "Read the contents of a file.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{
						"type":        "string",
						"description": "Path to the file to read (relative or absolute)",
					},
				},
				"required": []string{"path"},
			},
		},
	}
}

// Run reads the file in the sandbox and returns its contents.
func (t *ReadFileTool) Run(ctx context.Context, sandbox Sandbox) (llm.Message, error) {
	log := scoped.Log(ctx)

	if t.Path == "" {
		return llm.Message{}, errors.New("path is required")
	}

	log.Info("reading file in sandbox", "path", t.Path)
	res, err := sandbox.ExecCommand(ctx, ExecCommandOptions{
		// The -- separator makes execution more robust e.g. if path is "--help"
		Command: []string{"cat", "--", t.Path},
	})
	if err != nil {
		return llm.Message{}, err
	}

	content := res.Stdout

	if res.ExitCode != 0 {
		content = fmt.Sprintf("Failed to read %q:\nstdout:\n%s\nstderr:\n%s\nexit_code: %d", t.Path, res.Stdout, res.Stderr, res.ExitCode)
	}

	result := llm.Message{
		Content: &content,
	}

	return result, nil
}
