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

// Package web implements the server-rendered web UI for auto-optimizer.
package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
)

// ErrNotFound is returned when a requested session is not found.
var ErrNotFound = session.ErrNotFound

// SessionSummary describes a session in the session list view.
type SessionSummary struct {
	Name         string
	Toolset      string
	Parent       string
	CreatedAt    time.Time
	MessageCount int
	Busy         bool
}

// SessionDetail represents full details of a session and its message history.
type SessionDetail struct {
	Name      string
	Toolset   string
	Parent    string
	CreatedAt time.Time
	Busy      bool
	Messages  []llm.Message
}

// SessionSource provides access to session metadata and message transcripts.
type SessionSource interface {
	ListSessions(ctx context.Context) ([]SessionSummary, error)
	GetSession(ctx context.Context, name string) (*SessionDetail, error)
}

// StoreSessionSource adapts a session.Store to the SessionSource interface.
type StoreSessionSource struct {
	store  session.Store
	isBusy func(name string) bool
}

// NewStoreSessionSource creates a SessionSource backed by a session.Store.
func NewStoreSessionSource(store session.Store, isBusy func(name string) bool) *StoreSessionSource {
	return &StoreSessionSource{
		store:  store,
		isBusy: isBusy,
	}
}

// GetSession loads a session from the underlying session.Store.
func (s *StoreSessionSource) GetSession(ctx context.Context, name string) (*SessionDetail, error) {
	if s.store == nil {
		return nil, errors.New("session store is nil")
	}
	messages, meta, err := s.store.LoadSession(ctx, name)
	if err != nil {
		return nil, err
	}
	busy := false
	if s.isBusy != nil {
		busy = s.isBusy(name)
	}
	toolset := ""
	parent := ""
	var createdAt time.Time
	if meta != nil {
		toolset = meta.Toolset
		parent = meta.Parent
		createdAt = meta.CreatedAt
	}
	return &SessionDetail{
		Name:      name,
		Toolset:   toolset,
		Parent:    parent,
		CreatedAt: createdAt,
		Busy:      busy,
		Messages:  messages,
	}, nil
}

// ListSessions lists sessions if the store implements listing.
func (s *StoreSessionSource) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	if s.store == nil {
		return nil, errors.New("session store is nil")
	}
	type summaryLister interface {
		ListSessions(ctx context.Context) ([]SessionSummary, error)
	}
	if sl, ok := s.store.(summaryLister); ok {
		return sl.ListSessions(ctx)
	}
	return nil, nil
}

// MessageView represents a message prepared for HTML rendering.
type MessageView struct {
	Role         string
	RoleClass    string
	Content      string
	IsTruncated  bool
	ShowAllURL   string
	ToolCalls    []string
	IsToolResult bool
}

// DefaultMaxContentLength is the maximum byte count rendered before content
// is truncated in transcript views.
const DefaultMaxContentLength = 1000

// ServerOptions configures the web Server.
type ServerOptions struct {
	Sessions         SessionSource
	MaxContentLength int
}

// Server serves the auto-optimizer web UI.
type Server struct {
	sessions         SessionSource
	maxContentLength int
	mux              *http.ServeMux
}

// NewServer creates a new web Server.
func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Sessions == nil {
		return nil, errors.New("sessions source is required")
	}
	maxLen := opts.MaxContentLength
	if maxLen <= 0 {
		maxLen = DefaultMaxContentLength
	}
	s := &Server{
		sessions:         opts.Sessions,
		maxContentLength: maxLen,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /sessions/{name}", s.handleSessionTranscript)
	mux.HandleFunc("GET /{$}", s.handleSessionList)
	s.mux = mux

	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := scoped.Log(ctx)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, "ok\n"); err != nil {
		log.Error(err, "failed to write healthz response")
	}
}

func (s *Server) handleSessionTranscript(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := scoped.Log(ctx)
	name := r.PathValue("name")
	if name == "" {
		s.renderNotFound(w, r, name)
		return
	}

	detail, err := s.sessions.GetSession(ctx, name)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) || errors.Is(err, ErrNotFound) {
			s.renderNotFound(w, r, name)
			return
		}
		log.Error(err, "failed to get session", "session", name)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if detail == nil {
		s.renderNotFound(w, r, name)
		return
	}

	showFull := r.URL.Query().Get("full") == "1"
	escapedName := url.PathEscape(detail.Name)
	showAllURL := fmt.Sprintf("/sessions/%s?full=1", escapedName)

	var messageViews []MessageView
	for _, msg := range detail.Messages {
		mv := s.buildMessageView(msg, showFull, showAllURL)
		messageViews = append(messageViews, mv)
	}

	view := TranscriptView{
		CSS:        defaultCSS,
		Session:    detail,
		Messages:   messageViews,
		ShowFull:   showFull,
		ShowAllURL: showAllURL,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplTranscript.Execute(w, view); err != nil {
		log.Error(err, "failed to execute transcript template", "session", name)
	}
}

func (s *Server) buildMessageView(msg llm.Message, showFull bool, showAllURL string) MessageView {
	role := msg.Role
	roleClass := strings.ToLower(role)

	var rawContent string
	if msg.Content != nil {
		rawContent = *msg.Content
	}

	isToolResult := role == "tool"

	var content string
	var isTruncated bool

	if showFull {
		content = rawContent
		isTruncated = false
	} else {
		content, isTruncated = truncateContent(rawContent, s.maxContentLength)
	}

	var toolCalls []string
	if role == "assistant" && len(msg.ToolCalls) > 0 {
		for _, tc := range msg.ToolCalls {
			formatted := fmt.Sprintf("%s(%s)", tc.Function.Name, tc.Function.Arguments)
			toolCalls = append(toolCalls, formatted)
		}
	}

	return MessageView{
		Role:         role,
		RoleClass:    roleClass,
		Content:      content,
		IsTruncated:  isTruncated,
		ShowAllURL:   showAllURL,
		ToolCalls:    toolCalls,
		IsToolResult: isToolResult,
	}
}

func truncateContent(s string, maxLen int) (string, bool) {
	if maxLen <= 0 || len(s) <= maxLen {
		return s, false
	}
	for maxLen > 0 && !utf8.RuneStart(s[maxLen]) {
		maxLen--
	}
	return s[:maxLen] + "...", true
}

func (s *Server) renderNotFound(w http.ResponseWriter, r *http.Request, name string) {
	ctx := r.Context()
	log := scoped.Log(ctx)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	view := NotFoundView{
		CSS:         defaultCSS,
		SessionName: name,
	}
	if err := tmplNotFound.Execute(w, view); err != nil {
		log.Error(err, "failed to execute not found template", "session", name)
	}
}

func (s *Server) handleSessionList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := scoped.Log(ctx)

	sessions, err := s.sessions.ListSessions(ctx)
	if err != nil {
		log.Error(err, "failed to list sessions")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	view := SessionListView{
		CSS:      defaultCSS,
		Sessions: sessions,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmplList.Execute(w, view); err != nil {
		log.Error(err, "failed to execute list template")
	}
}
