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
	"errors"
	"time"

	"github.com/spf13/pflag"
)

// DefaultNamespace is the default Kubernetes namespace where sandbox pods are created.
const DefaultNamespace = "ao-sandboxes"

// DefaultHomeDir is the default home directory inside the sandbox persisted via snapshot/restore.
const DefaultHomeDir = "/home/clawtainer"

// SandboxOptions configures a sandbox container environment.
type SandboxOptions struct {
	Namespace   string
	Image       string
	HomeDir     string
	ToolTimeout time.Duration
}

// InitDefaults fills zero-valued fields with default constants.
func (o *SandboxOptions) InitDefaults() {
	if o == nil {
		return
	}
	if o.Namespace == "" {
		o.Namespace = DefaultNamespace
	}
	if o.HomeDir == "" {
		o.HomeDir = DefaultHomeDir
	}
	if o.ToolTimeout == 0 {
		o.ToolTimeout = DefaultToolTimeout
	}
}

// AddFlags binds the sandbox flags to fs.
func (o *SandboxOptions) AddFlags(fs *pflag.FlagSet) {
	o.InitDefaults()
	fs.StringVar(&o.Namespace, "namespace", o.Namespace, "Kubernetes namespace where sandbox pods are created")
	fs.StringVar(&o.Image, "image", o.Image, "Default sandbox container image (overrides toolset default)")
	fs.StringVar(&o.HomeDir, "homedir", o.HomeDir, "Home directory inside the sandbox persisted via snapshot/restore")
	fs.DurationVar(&o.ToolTimeout, "tool-timeout", o.ToolTimeout, "Maximum duration for a single tool execution")
}

// Validate checks that the options are valid.
func (o *SandboxOptions) Validate() error {
	if o == nil {
		return errors.New("sandbox options are required")
	}
	if o.HomeDir == "" {
		return errors.New("homedir must not be empty")
	}
	return nil
}

// ResolveImage returns o.Image if set, else toolsetDefault if set, else DefaultImage.
func (o SandboxOptions) ResolveImage(toolsetDefault string) string {
	if o.Image != "" {
		return o.Image
	}
	if toolsetDefault != "" {
		return toolsetDefault
	}
	return DefaultImage
}

// ApplySandbox applies sandbox options to the harness RunOptions.
func (o *RunOptions) ApplySandbox(s SandboxOptions) {
	if s.Namespace != "" {
		o.Namespace = s.Namespace
	}
	if s.HomeDir != "" {
		o.HomeDir = s.HomeDir
	}
	if s.ToolTimeout != 0 {
		o.ToolTimeout = s.ToolTimeout
	}
	if s.Image != "" {
		o.Image = s.Image
	}
}
