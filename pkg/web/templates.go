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

package web

import (
	_ "embed"
	"html/template"
)

//go:embed style.css
var embeddedCSS string

var defaultCSS = template.CSS(embeddedCSS)

// TranscriptView is the data context for rendering the session transcript page.
type TranscriptView struct {
	CSS        template.CSS
	Session    *SessionDetail
	Messages   []MessageView
	ShowFull   bool
	ShowAllURL string
}

// NotFoundView is the data context for rendering the session not found page.
type NotFoundView struct {
	CSS         template.CSS
	SessionName string
}

// SessionListView is the data context for rendering the session list page.
type SessionListView struct {
	CSS      template.CSS
	Sessions []SessionSummary
}

var (
	tmplTranscript = template.Must(template.New("transcript").Parse(transcriptTemplate))
	tmplNotFound   = template.Must(template.New("notFound").Parse(notFoundTemplate))
	tmplList       = template.Must(template.New("list").Parse(listTemplate))
)

const transcriptTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {{- if .Session.Busy }}
  <meta http-equiv="refresh" content="2">
  {{- end }}
  <title>{{ .Session.Name }} · auto-optimizer</title>
  <style>
{{ .CSS }}
  </style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/" class="header-logo">auto-optimizer</a>
      <nav class="nav-links">
        <a href="/" class="nav-link">Sessions</a>
      </nav>
    </div>
  </header>

  <main class="container">
    <div class="card">
      <div class="breadcrumb">
        <a href="/">Sessions</a> / <span class="mono">{{ .Session.Name }}</span>
      </div>

      <div class="card-header">
        <div class="header-title">
          <h2 class="session-title mono">{{ .Session.Name }}</h2>
          {{- if .Session.Busy }}
          <span class="chip chip-busy">busy</span>
          {{- else }}
          <span class="chip chip-idle">idle</span>
          {{- end }}
        </div>
        <div class="meta mono">
          {{- if .Session.Toolset }}
          <span>toolset: <b>{{ .Session.Toolset }}</b></span>
          {{- end }}
          {{- if .Session.Parent }}
          <span>parent: <a href="/sessions/{{ .Session.Parent }}">{{ .Session.Parent }}</a></span>
          {{- end }}
          {{- if not .Session.CreatedAt.IsZero }}
          <span>created: {{ .Session.CreatedAt.Format "2006-01-02 15:04:05 UTC" }}</span>
          {{- end }}
          <span>messages: {{ len .Session.Messages }}</span>
        </div>
      </div>

      <div class="transcript">
        {{- range .Messages }}
        <div class="ev {{ .RoleClass }}">
          <span class="k">{{ .Role }}</span>
          {{- if .IsToolResult }}
          <details>
            <summary>Tool result</summary>
            <pre class="b">{{ .Content }}{{ if .IsTruncated }} <a href="{{ .ShowAllURL }}">show all</a>{{ end }}</pre>
          </details>
          {{- else }}
          <div class="ev-body">
            {{- if .Content }}
            <pre class="b">{{ .Content }}{{ if .IsTruncated }} <a href="{{ .ShowAllURL }}">show all</a>{{ end }}</pre>
            {{- end }}
            {{- range .ToolCalls }}
            <pre class="b tool-call mono">{{ . }}</pre>
            {{- end }}
          </div>
          {{- end }}
        </div>
        {{- else }}
        <div class="empty-state">No messages in this session yet.</div>
        {{- end }}
      </div>

      <div class="card-footer">
        <span>auto-optimizer</span>
      </div>
    </div>
  </main>
</body>
</html>
`

const notFoundTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Not Found · auto-optimizer</title>
  <style>
{{ .CSS }}
  </style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/" class="header-logo">auto-optimizer</a>
      <nav class="nav-links">
        <a href="/" class="nav-link">Sessions</a>
      </nav>
    </div>
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h2 class="session-title">Session Not Found</h2>
      </div>
      <div class="card-body">
        <p>The session <code class="mono">{{ .SessionName }}</code> was not found.</p>
        <p><a href="/">← Back to sessions</a></p>
      </div>
      <div class="card-footer">
        <span>auto-optimizer</span>
      </div>
    </div>
  </main>
</body>
</html>
`

const listTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sessions · auto-optimizer</title>
  <style>
{{ .CSS }}
  </style>
</head>
<body>
  <header class="header-bar">
    <div class="header-brand">
      <a href="/" class="header-logo">auto-optimizer</a>
      <nav class="nav-links">
        <a href="/" class="nav-link active">Sessions</a>
      </nav>
    </div>
  </header>

  <main class="container">
    <div class="card">
      <div class="card-header">
        <h2 class="session-title">Sessions</h2>
      </div>

      <div class="card-body">
        <table class="session-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Status</th>
              <th>Toolset</th>
              <th>Parent</th>
              <th>Created</th>
              <th>Messages</th>
            </tr>
          </thead>
          <tbody>
            {{- range .Sessions }}
            <tr>
              <td class="mono"><a href="/sessions/{{ .Name }}">{{ .Name }}</a></td>
              <td>
                {{- if .Busy }}
                <span class="chip chip-busy">busy</span>
                {{- else }}
                <span class="chip chip-idle">idle</span>
                {{- end }}
              </td>
              <td class="mono">{{ if .Toolset }}{{ .Toolset }}{{ else }}—{{ end }}</td>
              <td class="mono">{{ if .Parent }}<a href="/sessions/{{ .Parent }}">{{ .Parent }}</a>{{ else }}—{{ end }}</td>
              <td class="mono">{{ if not .CreatedAt.IsZero }}{{ .CreatedAt.Format "2006-01-02 15:04:05 UTC" }}{{ else }}—{{ end }}</td>
              <td>{{ .MessageCount }}</td>
            </tr>
            {{- else }}
            <tr>
              <td colspan="6" class="empty-state">No sessions found.</td>
            </tr>
            {{- end }}
          </tbody>
        </table>
      </div>

      <div class="card-footer">
        <span>auto-optimizer</span>
      </div>
    </div>
  </main>
</body>
</html>
`
