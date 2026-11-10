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

package session

import (
	"slices"
	"strings"
	"testing"
)

func TestConfigureGitIdentity(t *testing.T) {
	tests := []struct {
		name      string
		repoDir   string
		author    string
		commands  map[string]cmdResult
		wantCalls []string
		wantErr   string
	}{
		{
			name:   "parsed name and email with default repoDir",
			author: "Alice <alice@example.org>",
			commands: map[string]cmdResult{
				"git -C /workspace/repo config user.name Alice":              {},
				"git -C /workspace/repo config user.email alice@example.org": {},
			},
			wantCalls: []string{
				"git -C /workspace/repo config user.name Alice",
				"git -C /workspace/repo config user.email alice@example.org",
			},
		},
		{
			name:    "name falls back to address with custom repoDir",
			repoDir: "/custom",
			author:  "bob@example.com",
			commands: map[string]cmdResult{
				"git -C /custom config user.name bob@example.com":  {},
				"git -C /custom config user.email bob@example.com": {},
			},
			wantCalls: []string{
				"git -C /custom config user.name bob@example.com",
				"git -C /custom config user.email bob@example.com",
			},
		},
		{
			name:    "unparsable author returns error before commands run",
			author:  "bad-author<",
			wantErr: "invalid author",
		},
		{
			name:   "failing git config is reported",
			author: "Alice <alice@example.org>",
			commands: map[string]cmdResult{
				"git -C /workspace/repo config user.name Alice": {exitCode: 1, stderr: "permission denied"},
			},
			wantCalls: []string{
				"git -C /workspace/repo config user.name Alice",
			},
			wantErr: "permission denied",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &scriptedProcessServer{commands: tc.commands}
			sb, cleanup := setupScriptedSandbox(t, srv)
			defer cleanup()

			err := sb.ConfigureGitIdentity(t.Context(), tc.repoDir, tc.author)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !slices.Equal(srv.calls, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", srv.calls, tc.wantCalls)
			}
		})
	}
}

func TestGitOptions(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		var opts GitOptions
		opts.InitDefaults()
		if opts.Author != DefaultGitAuthor {
			t.Errorf("Author = %q, want %q", opts.Author, DefaultGitAuthor)
		}
	})

	t.Run("custom author preserved", func(t *testing.T) {
		opts := GitOptions{Author: "Custom <custom@example.com>"}
		opts.InitDefaults()
		if opts.Author != "Custom <custom@example.com>" {
			t.Errorf("Author = %q, want 'Custom <custom@example.com>'", opts.Author)
		}
	})

	t.Run("validate", func(t *testing.T) {
		var nilOpts *GitOptions
		if err := nilOpts.Validate(); err == nil {
			t.Error("expected error for nil options, got nil")
		}

		valid := GitOptions{Author: "Bot <bot@example.org>"}
		if err := valid.Validate(); err != nil {
			t.Errorf("unexpected error for valid author: %v", err)
		}

		invalid := GitOptions{Author: "invalid-author<"}
		if err := invalid.Validate(); err == nil {
			t.Error("expected error for invalid author, got nil")
		}
	})
}
