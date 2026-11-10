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
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestSandboxOptions_Defaults(t *testing.T) {
	var opts SandboxOptions
	opts.InitDefaults()

	if opts.Namespace != DefaultNamespace {
		t.Errorf("Namespace = %q, want %q", opts.Namespace, DefaultNamespace)
	}
	if opts.HomeDir != DefaultHomeDir {
		t.Errorf("HomeDir = %q, want %q", opts.HomeDir, DefaultHomeDir)
	}
	if opts.ToolTimeout != DefaultToolTimeout {
		t.Errorf("ToolTimeout = %v, want %v", opts.ToolTimeout, DefaultToolTimeout)
	}
	if opts.Image != "" {
		t.Errorf("Image = %q, want empty", opts.Image)
	}

	customOpts := SandboxOptions{
		Namespace:   "custom-ns",
		HomeDir:     "/custom/home",
		Image:       "custom-image:v1",
		ToolTimeout: 30 * time.Second,
	}
	customOpts.InitDefaults()
	if customOpts.Namespace != "custom-ns" {
		t.Errorf("Namespace = %q, want custom-ns", customOpts.Namespace)
	}
	if customOpts.HomeDir != "/custom/home" {
		t.Errorf("HomeDir = %q, want /custom/home", customOpts.HomeDir)
	}
	if customOpts.Image != "custom-image:v1" {
		t.Errorf("Image = %q, want custom-image:v1", customOpts.Image)
	}
	if customOpts.ToolTimeout != 30*time.Second {
		t.Errorf("ToolTimeout = %v, want 30s", customOpts.ToolTimeout)
	}
}

func TestSandboxOptions_AddFlags(t *testing.T) {
	var opts SandboxOptions
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts.AddFlags(fs)

	if err := fs.Parse([]string{
		"--namespace", "flag-ns",
		"--image", "flag-image:v2",
		"--homedir", "/flag/home",
		"--tool-timeout", "5m",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if opts.Namespace != "flag-ns" {
		t.Errorf("Namespace = %q, want flag-ns", opts.Namespace)
	}
	if opts.Image != "flag-image:v2" {
		t.Errorf("Image = %q, want flag-image:v2", opts.Image)
	}
	if opts.HomeDir != "/flag/home" {
		t.Errorf("HomeDir = %q, want /flag/home", opts.HomeDir)
	}
	if opts.ToolTimeout != 5*time.Minute {
		t.Errorf("ToolTimeout = %v, want 5m", opts.ToolTimeout)
	}
}

func TestSandboxOptions_Validate(t *testing.T) {
	var nilOpts *SandboxOptions
	if err := nilOpts.Validate(); err == nil {
		t.Error("expected error for nil SandboxOptions, got nil")
	}

	opts := SandboxOptions{HomeDir: ""}
	if err := opts.Validate(); err == nil {
		t.Error("expected error for empty HomeDir, got nil")
	}

	valid := SandboxOptions{HomeDir: "/home/clawtainer"}
	if err := valid.Validate(); err != nil {
		t.Errorf("unexpected error for valid SandboxOptions: %v", err)
	}
}

func TestSandboxOptions_ResolveImage(t *testing.T) {
	// Explicit Image takes precedence
	opts := SandboxOptions{Image: "explicit:v1"}
	if got := opts.ResolveImage("toolset:v1"); got != "explicit:v1" {
		t.Errorf("ResolveImage = %q, want explicit:v1", got)
	}

	// Toolset default used when explicit Image is empty
	emptyOpts := SandboxOptions{}
	if got := emptyOpts.ResolveImage("toolset:v1"); got != "toolset:v1" {
		t.Errorf("ResolveImage = %q, want toolset:v1", got)
	}

	// DefaultImage used when both are empty
	if got := emptyOpts.ResolveImage(""); got != DefaultImage {
		t.Errorf("ResolveImage = %q, want %q", got, DefaultImage)
	}
}

func TestRunOptions_ApplySandbox(t *testing.T) {
	var runOpts RunOptions
	runOpts.InitDefaults()

	sandboxOpts := SandboxOptions{
		Namespace:   "applied-ns",
		Image:       "applied-img",
		HomeDir:     "/applied/home",
		ToolTimeout: 30 * time.Second,
	}
	runOpts.ApplySandbox(sandboxOpts)

	if runOpts.Namespace != "applied-ns" {
		t.Errorf("Namespace = %q, want applied-ns", runOpts.Namespace)
	}
	if runOpts.Image != "applied-img" {
		t.Errorf("Image = %q, want applied-img", runOpts.Image)
	}
	if runOpts.HomeDir != "/applied/home" {
		t.Errorf("HomeDir = %q, want /applied/home", runOpts.HomeDir)
	}
	if runOpts.ToolTimeout != 30*time.Second {
		t.Errorf("ToolTimeout = %v, want 30s", runOpts.ToolTimeout)
	}
}
