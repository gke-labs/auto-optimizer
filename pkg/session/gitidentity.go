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
	"context"
	"errors"
	"fmt"
	"net/mail"

	"github.com/spf13/pflag"

	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

// DefaultGitAuthor is the default author identity configured as git user.name and user.email in each run's sandbox.
const DefaultGitAuthor = "auto-optimizer <auto-optimizer@users.noreply.github.com>"

// GitOptions configures git identity and settings for sandbox operations.
type GitOptions struct {
	Author string
}

// InitDefaults fills zero-valued fields with default constants.
func (o *GitOptions) InitDefaults() {
	if o == nil {
		return
	}
	if o.Author == "" {
		o.Author = DefaultGitAuthor
	}
}

// AddFlags binds git flags to fs.
func (o *GitOptions) AddFlags(fs *pflag.FlagSet) {
	o.InitDefaults()
	fs.StringVar(&o.Author, "git-author", o.Author, "Identity configured as git user.name and user.email in each run's sandbox, as `Name <email>`")
}

// Validate verifies that the git options are valid.
func (o *GitOptions) Validate() error {
	if o == nil {
		return errors.New("git options are required")
	}
	if _, err := mail.ParseAddress(o.Author); err != nil {
		return fmt.Errorf("invalid author %q: %w", o.Author, err)
	}
	return nil
}

// ConfigureGitIdentity configures user.name and user.email in the sandbox repository.
// If repoDir is empty, it defaults to "/workspace/repo".
func (s *Sandbox) ConfigureGitIdentity(ctx context.Context, repoDir, author string) error {
	addr, err := mail.ParseAddress(author)
	if err != nil {
		return fmt.Errorf("invalid author %q: %w", author, err)
	}
	name := addr.Name
	if name == "" {
		name = addr.Address
	}

	if repoDir == "" {
		repoDir = "/workspace/repo"
	}

	for _, cfg := range [][2]string{
		{"user.name", name},
		{"user.email", addr.Address},
	} {
		res, err := s.ExecCommand(ctx, tools.ExecCommandOptions{
			Command: []string{"git", "-C", repoDir, "config", cfg[0], cfg[1]},
		})
		if err != nil {
			return fmt.Errorf("git config %s: %w", cfg[0], err)
		}
		if res.ExitCode != 0 {
			return fmt.Errorf("git config %s failed with exit code %d: %s", cfg[0], res.ExitCode, res.Stderr)
		}
	}

	return nil
}
