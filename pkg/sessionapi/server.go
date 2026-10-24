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

// Package sessionapi implements the gRPC SessionService for auto-optimizer.
package sessionapi

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrBusy is returned when a turn is already in flight for the session.
var ErrBusy = errors.New("session is busy")

// SessionSummary summarizes an agent session for in-process consumers such as the web UI.
type SessionSummary struct {
	Name         string
	Toolset      string
	Parent       string
	CreatedAt    time.Time
	MessageCount int
	InFlight     bool
}

// ToolCall represents an assistant tool call.
type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

// Message represents a single message in a session transcript.
type Message struct {
	Role       string
	Content    string
	ToolCallID string
	ToolCalls  []ToolCall
}

// SessionDetail represents the full session transcript and metadata.
type SessionDetail struct {
	Summary  SessionSummary
	Messages []Message
}

// ServerOptions configures a SessionService Server.
type ServerOptions struct {
	Store          session.Store
	DefaultToolset string
	// Harness is used when all sessions use a single harness (e.g. in tests).
	Harness *session.Harness
	// HarnessFactory resolves or constructs a Harness for a given toolset name.
	// If nil and Harness is provided, HarnessFactory defaults to returning Harness.
	HarnessFactory func(toolsetName string) (*session.Harness, error)
}

// Server implements sessionv1.SessionServiceServer.
type Server struct {
	sessionv1.UnimplementedSessionServiceServer

	store          session.Store
	defaultToolset string
	harnessFactory func(toolsetName string) (*session.Harness, error)

	mu             sync.Mutex
	busySessions   map[string]bool
	activeSessions map[string]*session.Session
}

// NewServer creates a new SessionService Server.
func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Store == nil {
		return nil, errors.New("Store is required")
	}

	defaultToolset := opts.DefaultToolset
	if defaultToolset == "" {
		defaultToolset = "basic"
	}

	factory := opts.HarnessFactory
	if factory == nil {
		if opts.Harness != nil {
			factory = func(_ string) (*session.Harness, error) {
				return opts.Harness, nil
			}
		} else {
			return nil, errors.New("either Harness or HarnessFactory must be provided")
		}
	}

	return &Server{
		store:          opts.Store,
		defaultToolset: defaultToolset,
		harnessFactory: factory,
		busySessions:   make(map[string]bool),
		activeSessions: make(map[string]*session.Session),
	}, nil
}

// CreateSession creates a new agent session.
func (s *Server) CreateSession(ctx context.Context, req *sessionv1.CreateSessionRequest) (*sessionv1.Session, error) {
	log := scoped.Log(ctx)

	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}

	toolset := req.GetToolset()
	if toolset == "" {
		toolset = s.defaultToolset
	}
	if _, err := toolsets.Get(toolset); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unknown toolset %q: %v", toolset, err)
	}

	parent := req.GetParent()
	if parent != "" {
		if err := session.ValidateSessionName(parent); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid parent session name %q: %v", parent, err)
		}
	}

	harness, err := s.harnessFactory(toolset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolving harness for toolset %q: %v", toolset, err)
	}

	meta := session.Metadata{
		Toolset:   toolset,
		Parent:    parent,
		CreatedAt: time.Now(),
	}

	sess, err := harness.NewSession(ctx, s.store, name, meta)
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "session %q already exists or cannot be created: %v", name, err)
	}

	s.mu.Lock()
	s.activeSessions[name] = sess
	s.mu.Unlock()

	log.Info("created session", "session", name, "toolset", toolset, "parent", parent)

	return &sessionv1.Session{
		Name:         sess.Name,
		Toolset:      sess.Metadata.Toolset,
		Parent:       sess.Metadata.Parent,
		CreationTime: timestamppb.New(sess.Metadata.CreatedAt),
		MessageCount: 0,
	}, nil
}

// ListSessions lists all known sessions and their summaries.
func (s *Server) ListSessions(ctx context.Context, _ *sessionv1.ListSessionsRequest) (*sessionv1.ListSessionsResponse, error) {
	summaries, err := s.store.ListSessions(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing sessions: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var sessions []*sessionv1.Session
	for _, sum := range summaries {
		msgCount := sum.MessageCount
		if act, ok := s.activeSessions[sum.Name]; ok {
			msgCount = len(act.Messages())
		}
		sessions = append(sessions, &sessionv1.Session{
			Name:         sum.Name,
			Toolset:      sum.Metadata.Toolset,
			Parent:       sum.Metadata.Parent,
			CreationTime: timestamppb.New(sum.Metadata.CreatedAt),
			MessageCount: int32(msgCount),
		})
	}

	return &sessionv1.ListSessionsResponse{Sessions: sessions}, nil
}

// GetSession returns details and message count for a single session.
func (s *Server) GetSession(ctx context.Context, req *sessionv1.GetSessionRequest) (*sessionv1.Session, error) {
	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}

	s.mu.Lock()
	sess, ok := s.activeSessions[name]
	s.mu.Unlock()

	if ok {
		return &sessionv1.Session{
			Name:         sess.Name,
			Toolset:      sess.Metadata.Toolset,
			Parent:       sess.Metadata.Parent,
			CreationTime: timestamppb.New(sess.Metadata.CreatedAt),
			MessageCount: int32(len(sess.Messages())),
		}, nil
	}

	messages, meta, err := s.store.LoadSession(ctx, name)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "session %q not found", name)
		}
		return nil, status.Errorf(codes.Internal, "loading session %q: %v", name, err)
	}

	var metadata session.Metadata
	if meta != nil {
		metadata = *meta
	}

	return &sessionv1.Session{
		Name:         name,
		Toolset:      metadata.Toolset,
		Parent:       metadata.Parent,
		CreationTime: timestamppb.New(metadata.CreatedAt),
		MessageCount: int32(len(messages)),
	}, nil
}

// ListMessages returns all messages for a session.
func (s *Server) ListMessages(ctx context.Context, req *sessionv1.ListMessagesRequest) (*sessionv1.ListMessagesResponse, error) {
	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}

	s.mu.Lock()
	sess, ok := s.activeSessions[name]
	s.mu.Unlock()

	var messages []llm.Message
	if ok {
		messages = sess.Messages()
	} else {
		var err error
		messages, _, err = s.store.LoadSession(ctx, name)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "session %q not found", name)
			}
			return nil, status.Errorf(codes.Internal, "loading session %q: %v", name, err)
		}
	}

	var pbMessages []*sessionv1.Message
	for _, m := range messages {
		pbMessages = append(pbMessages, toProtoMessage(m))
	}

	return &sessionv1.ListMessagesResponse{Messages: pbMessages}, nil
}

// SendMessage appends a user message and runs one turn of the agentic loop.
func (s *Server) SendMessage(req *sessionv1.SendMessageRequest, stream sessionv1.SessionService_SendMessageServer) error {
	ctx := stream.Context()
	log := scoped.Log(ctx)

	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}
	text := req.GetText()
	if text == "" {
		return status.Errorf(codes.InvalidArgument, "message text is required")
	}

	// Only one turn at a time per session.
	s.mu.Lock()
	if s.busySessions[name] {
		s.mu.Unlock()
		return status.Errorf(codes.FailedPrecondition, "session %q is busy", name)
	}
	s.busySessions[name] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.busySessions, name)
		s.mu.Unlock()
	}()

	sess, harness, err := s.getOrLoadSession(ctx, name)
	if err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	// If this is a new session without messages, ensure the system prompt is set and stream it.
	if len(sess.Messages()) == 0 {
		if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
			return status.Errorf(codes.Internal, "ensuring system prompt: %v", err)
		}
		for _, m := range sess.Messages() {
			if err := stream.Send(&sessionv1.SessionEvent{
				Event: &sessionv1.SessionEvent_MessageAppended{
					MessageAppended: &sessionv1.MessageAppended{
						Message: toProtoMessage(m),
					},
				},
			}); err != nil {
				return err
			}
		}
	}

	events := &streamTurnEvents{stream: stream}
	if err := harness.RunTurn(ctx, sess, text, events); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Error(err, "run turn failed", "session", name)
		if sendErr := stream.Send(&sessionv1.SessionEvent{
			Event: &sessionv1.SessionEvent_Error{
				Error: &sessionv1.Error{
					Message: err.Error(),
				},
			},
		}); sendErr != nil {
			log.V(2).Info("failed to stream error event", "error", sendErr)
		}
		return nil
	}

	if events.sendErr != nil {
		return events.sendErr
	}

	return nil
}

type streamTurnEvents struct {
	stream  sessionv1.SessionService_SendMessageServer
	sendErr error
}

func (e *streamTurnEvents) AssistantMessage(_ context.Context, text string) {
	if err := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_TurnFinished{
			TurnFinished: &sessionv1.TurnFinished{
				Text: text,
			},
		},
	}); err != nil && e.sendErr == nil {
		e.sendErr = err
	}
}

func (e *streamTurnEvents) ApproveToolCall(_ context.Context, _ llm.ToolCall) (bool, error) {
	// Tool calls are auto-approved by the server in this issue.
	if e.sendErr != nil {
		return false, e.sendErr
	}
	return true, nil
}

func (e *streamTurnEvents) ToolCallStarted(_ context.Context, call llm.ToolCall) {
	if err := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_ToolCallStarted{
			ToolCallStarted: &sessionv1.ToolCallStarted{
				ToolCall: toProtoToolCall(call),
			},
		},
	}); err != nil && e.sendErr == nil {
		e.sendErr = err
	}
}

func (e *streamTurnEvents) ToolCallFinished(_ context.Context, call llm.ToolCall, err error) {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	if sendErr := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_ToolCallFinished{
			ToolCallFinished: &sessionv1.ToolCallFinished{
				ToolCall: toProtoToolCall(call),
				Error:    errStr,
			},
		},
	}); sendErr != nil && e.sendErr == nil {
		e.sendErr = sendErr
	}
}

func (e *streamTurnEvents) MessageAppended(_ context.Context, msg llm.Message) {
	if err := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_MessageAppended{
			MessageAppended: &sessionv1.MessageAppended{
				Message: toProtoMessage(msg),
			},
		},
	}); err != nil && e.sendErr == nil {
		e.sendErr = err
	}
}

func toProtoMessage(msg llm.Message) *sessionv1.Message {
	var content string
	if msg.Content != nil {
		content = *msg.Content
	}
	pbMsg := &sessionv1.Message{
		Role:       msg.Role,
		Content:    content,
		ToolCallId: msg.ToolCallID,
	}
	for _, tc := range msg.ToolCalls {
		pbMsg.ToolCalls = append(pbMsg.ToolCalls, toProtoToolCall(tc))
	}
	return pbMsg
}

func toProtoToolCall(tc llm.ToolCall) *sessionv1.ToolCall {
	return &sessionv1.ToolCall{
		Id:   tc.ID,
		Type: tc.Type,
		Function: &sessionv1.FunctionCall{
			Name:      tc.Function.Name,
			Arguments: tc.Function.Arguments,
		},
	}
}

func (s *Server) getOrLoadSession(ctx context.Context, name string) (*session.Session, *session.Harness, error) {
	s.mu.Lock()
	sess := s.activeSessions[name]
	s.mu.Unlock()

	if sess == nil {
		defaultHarness, err := s.harnessFactory(s.defaultToolset)
		if err != nil {
			return nil, nil, fmt.Errorf("resolving default harness: %w", err)
		}
		loaded, err := defaultHarness.LoadSession(ctx, s.store, name)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				loaded, err = defaultHarness.NewSession(ctx, s.store, name, session.Metadata{
					Toolset:   s.defaultToolset,
					CreatedAt: time.Now(),
				})
				if err != nil {
					return nil, nil, fmt.Errorf("creating session %q: %w", name, err)
				}
				scoped.Log(ctx).Info("auto-created session on send", "session", name, "toolset", s.defaultToolset)
			} else {
				return nil, nil, fmt.Errorf("loading session %q: %w", name, err)
			}
		}
		sess = loaded
		s.mu.Lock()
		s.activeSessions[name] = sess
		s.mu.Unlock()
	}

	toolset := sess.Metadata.Toolset
	if toolset == "" {
		toolset = s.defaultToolset
	}
	harness, err := s.harnessFactory(toolset)
	if err != nil {
		return nil, nil, fmt.Errorf("resolving harness for toolset %q: %w", toolset, err)
	}

	return sess, harness, nil
}

// ListSessionSummaries returns summaries for all known sessions, ordered newest first.
func (s *Server) ListSessionSummaries(ctx context.Context) ([]SessionSummary, error) {
	summaries, err := s.store.ListSessions(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing sessions: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result []SessionSummary
	for _, sum := range summaries {
		msgCount := sum.MessageCount
		if act, ok := s.activeSessions[sum.Name]; ok {
			msgCount = len(act.Messages())
		}
		result = append(result, SessionSummary{
			Name:         sum.Name,
			Toolset:      sum.Metadata.Toolset,
			Parent:       sum.Metadata.Parent,
			CreatedAt:    sum.Metadata.CreatedAt,
			MessageCount: msgCount,
			InFlight:     s.busySessions[sum.Name],
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	return result, nil
}

// GetSessionDetail returns full metadata and messages for a given session.
func (s *Server) GetSessionDetail(ctx context.Context, name string) (*SessionDetail, error) {
	if err := session.ValidateSessionName(name); err != nil {
		return nil, fmt.Errorf("invalid session name %q: %w", name, err)
	}

	s.mu.Lock()
	sess, ok := s.activeSessions[name]
	inFlight := s.busySessions[name]
	s.mu.Unlock()

	var messages []llm.Message
	var meta session.Metadata
	if ok {
		meta = sess.Metadata
		messages = sess.Messages()
	} else {
		var loadedMeta *session.Metadata
		var err error
		messages, loadedMeta, err = s.store.LoadSession(ctx, name)
		if err != nil {
			return nil, err
		}
		if loadedMeta != nil {
			meta = *loadedMeta
		}
	}

	var webMsgs []Message
	for _, m := range messages {
		var content string
		if m.Content != nil {
			content = *m.Content
		}
		var tcs []ToolCall
		for _, tc := range m.ToolCalls {
			tcs = append(tcs, ToolCall{
				ID:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			})
		}
		webMsgs = append(webMsgs, Message{
			Role:       m.Role,
			Content:    content,
			ToolCallID: m.ToolCallID,
			ToolCalls:  tcs,
		})
	}

	return &SessionDetail{
		Summary: SessionSummary{
			Name:         name,
			Toolset:      meta.Toolset,
			Parent:       meta.Parent,
			CreatedAt:    meta.CreatedAt,
			MessageCount: len(messages),
			InFlight:     inFlight,
		},
		Messages: webMsgs,
	}, nil
}

// CreateNewSession creates a session with the specified name and toolset.
func (s *Server) CreateNewSession(ctx context.Context, name, toolset string) error {
	_, err := s.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    name,
		Toolset: toolset,
	})
	return err
}

// StartTurn starts one agent turn asynchronously in the background.
// If a turn is already running for the session, ErrBusy is returned.
func (s *Server) StartTurn(ctx context.Context, name, text string) error {
	if err := session.ValidateSessionName(name); err != nil {
		return fmt.Errorf("invalid session name %q: %w", name, err)
	}
	if text == "" {
		return errors.New("message text is required")
	}

	s.mu.Lock()
	if s.busySessions[name] {
		s.mu.Unlock()
		return ErrBusy
	}
	s.busySessions[name] = true
	s.mu.Unlock()

	sess, harness, err := s.getOrLoadSession(ctx, name)
	if err != nil {
		s.mu.Lock()
		delete(s.busySessions, name)
		s.mu.Unlock()
		return err
	}

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.busySessions, name)
			s.mu.Unlock()
		}()

		turnCtx := context.Background()
		log := scoped.Log(turnCtx)

		if len(sess.Messages()) == 0 {
			if err := harness.EnsureSystemPrompt(turnCtx, sess); err != nil {
				log.Error(err, "ensuring system prompt failed", "session", name)
				return
			}
		}

		events := &autoApprovedTurnEvents{ctx: turnCtx}
		if err := harness.RunTurn(turnCtx, sess, text, events); err != nil {
			log.Error(err, "run turn failed", "session", name)
		}
	}()

	return nil
}

type autoApprovedTurnEvents struct {
	ctx context.Context
}

func (e *autoApprovedTurnEvents) AssistantMessage(ctx context.Context, text string) {
	scoped.Log(ctx).V(2).Info("assistant replied", "text", text)
}

func (e *autoApprovedTurnEvents) ApproveToolCall(_ context.Context, _ llm.ToolCall) (bool, error) {
	return true, nil
}

func (e *autoApprovedTurnEvents) ToolCallStarted(ctx context.Context, call llm.ToolCall) {
	scoped.Log(ctx).V(2).Info("tool call started", "tool", call.Function.Name)
}

func (e *autoApprovedTurnEvents) ToolCallFinished(ctx context.Context, call llm.ToolCall, err error) {
	scoped.Log(ctx).V(2).Info("tool call finished", "tool", call.Function.Name, "error", err)
}

func (e *autoApprovedTurnEvents) MessageAppended(ctx context.Context, msg llm.Message) {
	scoped.Log(ctx).V(3).Info("message appended", "role", msg.Role)
}
