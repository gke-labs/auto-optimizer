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

// Package goals provides functions to find, parse, and validate goal definitions from Markdown files.
package goals

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"
)

// Kind identifies whether the orchestrator evaluates a goal using a metric command or maintainer judgment.
type Kind string

const (
	// KindMetric indicates a goal that the system evaluates by running and measuring a benchmark command.
	KindMetric Kind = "metric"
	// KindJudged indicates a goal that a maintainer evaluates using judgment rather than an automated benchmark command.
	KindJudged Kind = "judged"
)

// Goal represents a parsed and validated goal definition.
type Goal struct {
	// Name is the unique identifier of the goal.
	// The package derives this identifier from the base name of the goal's definition file.
	Name string `json:"name"`

	// Body contains the raw Markdown content of the goal, excluding any front matter.
	Body string `json:"body"`

	// Hash is the SHA-256 content hash of the original goal file.
	Hash string `json:"hash"`

	// Metric specifies the parameters for benchmark measurement when evaluating a metric-based goal.
	Metric *Metric `json:"metric,omitempty"`

	// Verify is an optional command that validates a candidate solution.
	Verify string `json:"verify,omitempty"`

	// Budget sets the resource and time limits for the goal run.
	Budget *Budget `json:"budget,omitempty"`

	// Schedule determines how frequently the orchestrator triggers goal evaluation.
	// Valid options include daily, weekly, on-change, and manual.
	Schedule string `json:"schedule,omitempty"`

	// Agents lists the names of the coding agents that can work on this goal.
	Agents []string `json:"agents,omitempty"`
}

// Kind returns KindMetric if the goal specifies a metric. Otherwise, it returns KindJudged.
func (g *Goal) Kind() Kind {
	if g.Metric != nil {
		return KindMetric
	}
	return KindJudged
}

// Metric defines the benchmark measurement parameters for a metric goal.
type Metric struct {
	// Command is the shell command that measures the metric.
	Command string `json:"command"`

	// Direction specifies whether a successful change increases or decreases the metric value.
	// Valid values are "higher" or "lower".
	Direction string `json:"direction"`

	// Unit describes the measurement unit of the metric.
	Unit string `json:"unit,omitempty"`

	// MinImprovement is the minimum improvement that a candidate must achieve.
	// This value can be an absolute number or a percentage.
	MinImprovement string `json:"minImprovement,omitempty"`

	// Runs is the number of benchmark executions required to produce a stable measurement.
	Runs int `json:"runs,omitempty"`

	// Timeout is the maximum duration that a single benchmark execution can run before the system terminates it.
	Timeout string `json:"timeout,omitempty"`
}

// Budget defines the resource and time limits that constrain a goal run.
type Budget struct {
	// MaxDuration is the maximum elapsed time that a goal run can take before the orchestrator terminates it.
	MaxDuration string `json:"maxDuration,omitempty"`

	// MaxCostUSD is the maximum monetary budget in US Dollars for LLM API usage during the run.
	MaxCostUSD float64 `json:"maxCostUSD,omitempty"`
}

type rawGoal struct {
	Metric   *rawMetric `json:"metric"`
	Verify   string     `json:"verify"`
	Budget   *rawBudget `json:"budget"`
	Schedule string     `json:"schedule"`
	Agents   []string   `json:"agents"`
}

type rawMetric struct {
	Command        string `json:"command"`
	Direction      string `json:"direction"`
	Unit           string `json:"unit"`
	MinImprovement string `json:"minImprovement"`
	Runs           *int   `json:"runs"`
	Timeout        string `json:"timeout"`
}

type rawBudget struct {
	MaxDuration string   `json:"maxDuration"`
	MaxCostUSD  *float64 `json:"maxCostUSD"`
}

// Parse processes a goal name and its raw file content to construct a validated Goal.
// First, Parse validates that the goal name contains only safe characters.
// Next, it computes a SHA-256 hash of the content for change tracking.
// Finally, it extracts the optional YAML front matter, parses its configuration, and validates all fields.
func Parse(name string, data []byte) (*Goal, error) {
	if !isSafeGoalName(name) {
		return nil, fmt.Errorf("goals: invalid goal name %q: must be safe in a branch name (lower-case letters, digits, '-', '_', '.')", name)
	}

	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])

	yamlBytes, bodyBytes, hasFrontMatter, err := splitFrontMatter(data)
	if err != nil {
		return nil, err
	}

	g := &Goal{
		Name:     name,
		Body:     string(bodyBytes),
		Hash:     hash,
		Schedule: "on-change",
	}

	if !hasFrontMatter || len(bytes.TrimSpace(yamlBytes)) == 0 {
		return g, nil
	}

	var raw rawGoal
	if err := yaml.UnmarshalStrict(yamlBytes, &raw); err != nil {
		return nil, fmt.Errorf("goals: invalid front matter: %w", err)
	}

	// Schedule validation and defaulting
	if raw.Schedule != "" {
		switch raw.Schedule {
		case "daily", "weekly", "on-change", "manual":
			g.Schedule = raw.Schedule
		default:
			return nil, fmt.Errorf("goals: invalid schedule %q: must be one of daily, weekly, on-change, manual", raw.Schedule)
		}
	}

	g.Verify = raw.Verify
	g.Agents = raw.Agents

	if raw.Budget != nil {
		b := &Budget{
			MaxDuration: raw.Budget.MaxDuration,
		}
		if raw.Budget.MaxCostUSD != nil {
			b.MaxCostUSD = *raw.Budget.MaxCostUSD
		}
		g.Budget = b
	}

	if raw.Metric != nil {
		if strings.TrimSpace(raw.Metric.Command) == "" {
			return nil, fmt.Errorf("goals: metric section without command is an error")
		}

		if raw.Metric.Direction != "lower" && raw.Metric.Direction != "higher" {
			return nil, fmt.Errorf("goals: invalid metric direction %q: must be 'lower' or 'higher'", raw.Metric.Direction)
		}

		runs := 1
		if raw.Metric.Runs != nil {
			if *raw.Metric.Runs < 1 {
				return nil, fmt.Errorf("goals: invalid metric runs %d: must be at least 1", *raw.Metric.Runs)
			}
			runs = *raw.Metric.Runs
		}

		if raw.Metric.MinImprovement != "" {
			if !isValidMinImprovement(raw.Metric.MinImprovement) {
				return nil, fmt.Errorf("goals: invalid minImprovement %q: must be a number or percentage", raw.Metric.MinImprovement)
			}
		}

		g.Metric = &Metric{
			Command:        raw.Metric.Command,
			Direction:      raw.Metric.Direction,
			Unit:           raw.Metric.Unit,
			MinImprovement: raw.Metric.MinImprovement,
			Runs:           runs,
			Timeout:        raw.Metric.Timeout,
		}
	}

	return g, nil
}

// LoadDir reads every Markdown file ending with the .md extension in the specified directory.
// The function parses each file into a Goal and returns them sorted alphabetically by name.
// LoadDir ignores directories and files that do not end with the .md extension.
func LoadDir(fsys fs.FS, dir string) ([]*Goal, error) {
	if dir == "" {
		dir = "."
	}
	dir = path.Clean(dir)
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}

	goals := make([]*Goal, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileName := entry.Name()
		if !strings.HasSuffix(fileName, ".md") {
			continue
		}

		goalName := strings.TrimSuffix(fileName, ".md")
		filePath := path.Join(dir, fileName)
		data, err := fs.ReadFile(fsys, filePath)
		if err != nil {
			return nil, err
		}

		g, err := Parse(goalName, data)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", filePath, err)
		}
		goals = append(goals, g)
	}

	sort.Slice(goals, func(i, j int) bool {
		return goals[i].Name < goals[j].Name
	})

	return goals, nil
}

func isSafeGoalName(name string) bool {
	if name == "" {
		return false
	}
	if name[0] == '-' || name[0] == '.' {
		return false
	}
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, ".lock") {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func isValidMinImprovement(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if strings.HasSuffix(s, "%") {
		numStr := strings.TrimSpace(strings.TrimSuffix(s, "%"))
		if numStr == "" {
			return false
		}
		val, err := strconv.ParseFloat(numStr, 64)
		return err == nil && !math.IsNaN(val) && !math.IsInf(val, 0)
	}
	val, err := strconv.ParseFloat(s, 64)
	return err == nil && !math.IsNaN(val) && !math.IsInf(val, 0)
}

func splitFrontMatter(data []byte) (yamlBytes []byte, bodyBytes []byte, hasFrontMatter bool, err error) {
	content := data
	if bytes.HasPrefix(content, []byte("\xef\xbb\xbf")) {
		content = content[3:]
	}

	if len(content) == 0 {
		return nil, content, false, nil
	}

	firstLine := content
	var rest []byte
	idx := bytes.IndexByte(content, '\n')
	if idx != -1 {
		firstLine = content[:idx]
		rest = content[idx+1:]
	}

	if string(bytes.TrimRight(firstLine, " \t\r")) != "---" {
		return nil, content, false, nil
	}

	// Front matter delimiter found on line 1.
	if rest == nil {
		return nil, nil, true, fmt.Errorf("goals: unclosed front matter")
	}

	lineStart := 0
	for lineStart <= len(rest) {
		if lineStart == len(rest) {
			break
		}
		var line []byte
		var nextLineStart int
		newlineIdx := bytes.IndexByte(rest[lineStart:], '\n')
		if newlineIdx == -1 {
			line = rest[lineStart:]
			nextLineStart = len(rest)
		} else {
			line = rest[lineStart : lineStart+newlineIdx]
			nextLineStart = lineStart + newlineIdx + 1
		}

		if string(bytes.TrimRight(line, " \t\r")) == "---" {
			// Found closing delimiter
			yamlBytes = rest[:lineStart]
			bodyBytes = rest[nextLineStart:]
			return yamlBytes, bodyBytes, true, nil
		}

		lineStart = nextLineStart
	}

	return nil, nil, true, fmt.Errorf("goals: unclosed front matter")
}
