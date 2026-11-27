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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
)

// Registry manages a collection of tools and routes invocations to them.
type Registry struct {
	tools map[string]Tool

	// ToolTimeout specifies the maximum duration that a single tool invocation can run.
	// A duration less than or equal to zero disables the timeout.
	ToolTimeout time.Duration
}

// NewRegistry creates and returns an empty tool registry.
func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]Tool),
	}
}

// Add registers a tool with the registry.
// The registry panics if the tool is nil or if it is not a pointer.
func (r *Registry) Add(tool Tool) {
	if tool == nil {
		panic("registered tool must not be nil")
	}
	if reflect.TypeOf(tool).Kind() != reflect.Pointer {
		panic(fmt.Sprintf("registered tool %T must be a pointer", tool))
	}
	schema := tool.Schema()
	name := schema.Function.Name
	r.tools[name] = tool
}

// All returns a sorted list of schemas for all registered tools.
func (r *Registry) All() []llm.Tool {
	var list []llm.Tool
	for _, tool := range r.tools {
		list = append(list, tool.Schema())
	}
	slices.SortFunc(list, func(a, b llm.Tool) int {
		if a.Function.Name < b.Function.Name {
			return -1
		}
		if a.Function.Name > b.Function.Name {
			return 1
		}
		return 0
	})
	return list
}

// errToolTimeout is the internal cancellation cause set on the context passed
// to Tool.Run when Registry.ToolTimeout is enabled. context.WithTimeout's
// Err() reports context.DeadlineExceeded both when our own timeout fires and
// when an earlier parent deadline expires first, so Err() alone can't tell
// them apart. Setting this cause via context.WithTimeoutCause lets Call use
// context.Cause to identify precisely which one happened: Cause reports
// errToolTimeout only when our own timer fired first: if the parent expired
// or was cancelled first, Cause reports the parent's cause instead. Never
// exposed outside this package; callers only ever see the wrapped
// context.DeadlineExceeded in the returned error (see Call).
var errToolTimeout = errors.New("tool execution timeout")

// Call executes the specified tool call inside the provided sandbox and returns the LLM tool response message.
func (r *Registry) Call(ctx context.Context, sandbox Sandbox, tc llm.ToolCall) (llm.Message, error) {
	log := scoped.Log(ctx)

	log.Info("llm invoking tool", "tool.name", tc.Function.Name)

	tool := r.tools[tc.Function.Name]
	if tool == nil {
		return llm.Message{}, fmt.Errorf("tool %q not found", tc.Function.Name)
	}

	toolType := reflect.TypeOf(tool).Elem()
	toolInvocation := reflect.New(toolType).Interface().(Tool)

	if err := json.Unmarshal([]byte(tc.Function.Arguments), toolInvocation); err != nil {
		return llm.Message{}, fmt.Errorf("failed to parse arguments: %w", err)
	}

	// runCtx bounds the individual tool invocation. Only wrap ctx when a
	// timeout is configured, so the disabled (ToolTimeout <= 0) case passes
	// the caller's context through unchanged.
	runCtx := ctx
	if r.ToolTimeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeoutCause(ctx, r.ToolTimeout, errToolTimeout)
		defer cancel()
	}

	res, err := toolInvocation.Run(runCtx, sandbox)

	// Populate some default values so the tools don't all have to do it.
	if res.Role == "" {
		res.Role = "tool"
	}
	res.ToolCallID = tc.ID

	if err != nil {
		// Classify via context.Cause(runCtx), not runCtx.Err(): Err() reports
		// context.DeadlineExceeded both when our own timeout fires and when an
		// earlier parent deadline expired first, so checking Err() alone would
		// mislabel a parent deadline as our configured tool timeout. Cause
		// reports errToolTimeout only when runCtx's own timer won the race; it
		// does not depend on whatever error Tool.Run happens to return.
		if r.ToolTimeout > 0 && context.Cause(runCtx) == errToolTimeout {
			return res, fmt.Errorf("tool %q timed out after %s: %w", tc.Function.Name, r.ToolTimeout, err)
		}
		return res, fmt.Errorf("failed to run tool: %w", err)
	}

	log.Info("tool result", "tool.name", tc.Function.Name)

	return res, nil
}
