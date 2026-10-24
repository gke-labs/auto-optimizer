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

// Package web provides the server-rendered HTML user interface for auto-optimizer.
package web

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
)

//go:embed static/*
var staticFS embed.FS

//go:embed templates/*
var templateFS embed.FS

const (
	csrfCookieName = "ao_session"
	maxTruncateLen = 500
)

// SessionService is the interface web requires from the session backend.
type SessionService interface {
	ListSessionSummaries(ctx context.Context) ([]sessionapi.SessionSummary, error)
	GetSessionDetail(ctx context.Context, name string) (*sessionapi.SessionDetail, error)
	CreateNewSession(ctx context.Context, name, toolset string) error
	StartTurn(ctx context.Context, name, text string) error
}

// Server handles HTTP requests for the web UI.
type Server struct {
	service   SessionService
	templates map[string]*template.Template
}

// NewServer creates a new web Server backed by the given SessionService.
func NewServer(service SessionService) (*Server, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}

	templates := make(map[string]*template.Template)
	pages := []string{"list", "transcript", "404", "409"}
	for _, page := range pages {
		tmpl, err := template.ParseFS(templateFS, "templates/layout.html", "templates/"+page+".html")
		if err != nil {
			return nil, fmt.Errorf("parsing template %s: %w", page, err)
		}
		templates[page] = tmpl
	}

	return &Server{
		service:   service,
		templates: templates,
	}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok\n")); err != nil {
			scoped.Log(r.Context()).V(4).Info("writing healthz response", "error", err)
		}
		return
	}

	if strings.HasPrefix(r.URL.Path, "/static/") && r.Method == http.MethodGet {
		sub, err := fs.Sub(staticFS, "static")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.StripPrefix("/static/", http.FileServer(http.FS(sub))).ServeHTTP(w, r)
		return
	}

	// Ensure SameSite=Strict cookie on GET requests
	if r.Method == http.MethodGet {
		s.ensureCSRFCookie(w, r)
	}

	// Validate CSRF on POST requests
	if r.Method == http.MethodPost {
		if !s.validateCSRF(r) {
			http.Error(w, "Forbidden: CSRF cookie missing or invalid", http.StatusForbidden)
			return
		}
	}

	switch {
	case r.URL.Path == "/" && r.Method == http.MethodGet:
		s.handleListSessions(w, r)
	case r.URL.Path == "/sessions" && r.Method == http.MethodPost:
		s.handleCreateSession(w, r)
	case strings.HasPrefix(r.URL.Path, "/sessions/"):
		rest := strings.TrimPrefix(r.URL.Path, "/sessions/")
		parts := strings.Split(rest, "/")
		if len(parts) == 1 && r.Method == http.MethodGet {
			s.handleGetSession(w, r, parts[0])
			return
		}
		if len(parts) == 2 && parts[1] == "messages" && r.Method == http.MethodPost {
			s.handlePostMessage(w, r, parts[0])
			return
		}
		s.render404(w, r, "Page not found")
	default:
		s.render404(w, r, "Page not found")
	}
}

func (s *Server) ensureCSRFCookie(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(csrfCookieName); err == nil && c.Value != "" {
		return
	}
	tokenBytes := make([]byte, 16)
	_, _ = rand.Read(tokenBytes)
	token := hex.EncodeToString(tokenBytes)

	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    token,
		Path:     "/",
		SameSite: http.SameSiteStrictMode,
		HttpOnly: true,
	})
}

func (s *Server) validateCSRF(r *http.Request) bool {
	cookie, err := r.Cookie(csrfCookieName)
	return err == nil && cookie.Value != ""
}

type listViewModel struct {
	Title    string
	InFlight bool
	Sessions []sessionRowViewModel
}

type sessionRowViewModel struct {
	Name               string
	Toolset            string
	Parent             string
	FormattedCreatedAt string
	MessageCount       int
	InFlight           bool
}

type transcriptViewModel struct {
	Title              string
	InFlight           bool
	Full               bool
	Summary            sessionapi.SessionSummary
	FormattedCreatedAt string
	Messages           []messageViewModel
}

type messageViewModel struct {
	Role           string
	Content        string
	DisplayContent string
	Truncated      bool
	CanTruncate    bool
	IsTool         bool
	ToolCallID     string
	ToolCalls      []sessionapi.ToolCall
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	summaries, err := s.service.ListSessionSummaries(r.Context())
	if err != nil {
		scoped.Log(r.Context()).Error(err, "listing session summaries")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var rows []sessionRowViewModel
	for _, sum := range summaries {
		rows = append(rows, sessionRowViewModel{
			Name:               sum.Name,
			Toolset:            sum.Toolset,
			Parent:             sum.Parent,
			FormattedCreatedAt: sum.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC"),
			MessageCount:       sum.MessageCount,
			InFlight:           sum.InFlight,
		})
	}

	vm := listViewModel{
		Title:    "Sessions",
		InFlight: false,
		Sessions: rows,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates["list"].Execute(w, vm); err != nil {
		scoped.Log(r.Context()).Error(err, "rendering list template")
	}
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request, name string) {
	detail, err := s.service.GetSessionDetail(r.Context(), name)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			s.render404(w, r, fmt.Sprintf("Session %q not found", name))
			return
		}
		scoped.Log(r.Context()).Error(err, "getting session detail", "session", name)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	full := r.URL.Query().Get("full") == "1"
	var msgs []messageViewModel
	for _, m := range detail.Messages {
		content := m.Content
		isTool := m.Role == "tool" || m.ToolCallID != ""
		canTruncate := len(content) > maxTruncateLen
		truncated := !full && canTruncate

		displayContent := content
		if truncated {
			displayContent = content[:maxTruncateLen] + "..."
		}

		msgs = append(msgs, messageViewModel{
			Role:           m.Role,
			Content:        content,
			DisplayContent: displayContent,
			Truncated:      truncated,
			CanTruncate:    canTruncate,
			IsTool:         isTool,
			ToolCallID:     m.ToolCallID,
			ToolCalls:      m.ToolCalls,
		})
	}

	vm := transcriptViewModel{
		Title:              "Session " + detail.Summary.Name,
		InFlight:           detail.Summary.InFlight,
		Full:               full,
		Summary:            detail.Summary,
		FormattedCreatedAt: detail.Summary.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC"),
		Messages:           msgs,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.templates["transcript"].Execute(w, vm); err != nil {
		scoped.Log(r.Context()).Error(err, "rendering transcript template", "session", name)
	}
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data: "+err.Error(), http.StatusBadRequest)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	toolset := strings.TrimSpace(r.FormValue("toolset"))
	if toolset == "" {
		toolset = "basic"
	}

	if err := session.ValidateSessionName(name); err != nil {
		http.Error(w, "invalid session name: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.service.CreateNewSession(r.Context(), name, toolset); err != nil {
		http.Error(w, "creating session: "+err.Error(), http.StatusBadRequest)
		return
	}

	http.Redirect(w, r, "/sessions/"+url.PathEscape(name), http.StatusSeeOther)
}

func (s *Server) handlePostMessage(w http.ResponseWriter, r *http.Request, name string) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data: "+err.Error(), http.StatusBadRequest)
		return
	}

	text := strings.TrimSpace(r.FormValue("text"))
	if text == "" {
		http.Error(w, "message text is required", http.StatusBadRequest)
		return
	}

	if err := s.service.StartTurn(r.Context(), name, text); err != nil {
		if errors.Is(err, sessionapi.ErrBusy) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusConflict)
			vm := struct {
				Title    string
				InFlight bool
				Name     string
			}{
				Title:    "409 Conflict",
				InFlight: false,
				Name:     name,
			}
			if execErr := s.templates["409"].Execute(w, vm); execErr != nil {
				scoped.Log(r.Context()).V(4).Info("rendering 409 template", "error", execErr)
			}
			return
		}
		http.Error(w, "starting turn: "+err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/sessions/"+url.PathEscape(name), http.StatusSeeOther)
}

func (s *Server) render404(w http.ResponseWriter, r *http.Request, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	vm := struct {
		Title    string
		InFlight bool
		Message  string
	}{
		Title:    "404 Not Found",
		InFlight: false,
		Message:  message,
	}
	if execErr := s.templates["404"].Execute(w, vm); execErr != nil {
		scoped.Log(r.Context()).V(4).Info("rendering 404 template", "error", execErr)
	}
}
