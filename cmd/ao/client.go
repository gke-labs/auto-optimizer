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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
)

func defaultServerAddress() string {
	if s := os.Getenv("AO_SERVER"); s != "" {
		return s
	}
	return "127.0.0.1:8081"
}

func closeConnection(ctx context.Context, conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil {
		scoped.Log(ctx).V(4).Info("closing gRPC connection", "error", err)
	}
}

func newSessionListCommand() *cobra.Command {
	var serverAddr string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all agent sessions on the server",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("connecting to server %s: %w", serverAddr, err)
			}
			defer closeConnection(cmd.Context(), conn)

			client := sessionv1.NewSessionServiceClient(conn)
			resp, err := client.ListSessions(cmd.Context(), &sessionv1.ListSessionsRequest{})
			if err != nil {
				return fmt.Errorf("listing sessions: %w", err)
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "NAME\tTOOLSET\tPARENT\tCREATED\tMESSAGES"); err != nil {
				return err
			}
			for _, s := range resp.Sessions {
				created := ""
				if s.CreationTime != nil {
					created = s.CreationTime.AsTime().Format(time.RFC3339)
				}
				if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n", s.Name, s.Toolset, s.Parent, created, s.MessageCount); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}

	cmd.Flags().StringVar(&serverAddr, "server", defaultServerAddress(), "gRPC server address")
	return cmd
}

func newSessionGetCommand() *cobra.Command {
	var serverAddr string
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get and print messages in an agent session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("connecting to server %s: %w", serverAddr, err)
			}
			defer closeConnection(cmd.Context(), conn)

			client := sessionv1.NewSessionServiceClient(conn)
			resp, err := client.ListMessages(cmd.Context(), &sessionv1.ListMessagesRequest{Name: name})
			if err != nil {
				return fmt.Errorf("getting session messages: %w", err)
			}

			if jsonOutput {
				m := protojson.MarshalOptions{
					Multiline: true,
					Indent:    "  ",
				}
				data, err := m.Marshal(resp)
				if err != nil {
					return fmt.Errorf("serializing messages to json: %w", err)
				}
				fmt.Println(string(data))
				return nil
			}

			for _, msg := range resp.Messages {
				content := msg.Content
				if content == "" && len(msg.ToolCalls) > 0 {
					var calls []string
					for _, tc := range msg.ToolCalls {
						calls = append(calls, fmt.Sprintf("%s(%s)", tc.Function.Name, tc.Function.Arguments))
					}
					content = fmt.Sprintf("call: %s", strings.Join(calls, ", "))
				}
				fmt.Printf("%s: %s\n", msg.Role, strings.TrimSpace(content))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&serverAddr, "server", defaultServerAddress(), "gRPC server address")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output messages as JSON")
	return cmd
}

func newSessionSendCommand() *cobra.Command {
	var serverAddr string

	cmd := &cobra.Command{
		Use:   "send <name> <text>",
		Short: "Send a message to an agent session and stream the turn",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			text := args[1]

			conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("connecting to server %s: %w", serverAddr, err)
			}
			defer closeConnection(cmd.Context(), conn)

			client := sessionv1.NewSessionServiceClient(conn)
			stream, err := client.SendMessage(cmd.Context(), &sessionv1.SendMessageRequest{
				Name: name,
				Text: text,
			})
			if err != nil {
				return fmt.Errorf("sending message: %w", err)
			}

			var hadError bool
			var errorMsg string
			var finalAssistantText string

			for {
				event, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return fmt.Errorf("receiving event: %w", err)
				}

				switch e := event.Event.(type) {
				case *sessionv1.SessionEvent_MessageAppended:
					// Message appended to session history.
				case *sessionv1.SessionEvent_ToolCallStarted:
					tc := e.ToolCallStarted.ToolCall
					fmt.Fprintf(os.Stderr, "==> Tool call: %s %s\n", tc.Function.Name, tc.Function.Arguments)
				case *sessionv1.SessionEvent_ToolCallFinished:
					tc := e.ToolCallFinished.ToolCall
					if e.ToolCallFinished.Error != "" {
						fmt.Fprintf(os.Stderr, "==> Tool failed: %s: %s\n", tc.Function.Name, e.ToolCallFinished.Error)
					} else {
						fmt.Fprintf(os.Stderr, "==> Tool finished: %s\n", tc.Function.Name)
					}
				case *sessionv1.SessionEvent_TurnFinished:
					finalAssistantText = e.TurnFinished.Text
				case *sessionv1.SessionEvent_Error:
					hadError = true
					errorMsg = e.Error.Message
					fmt.Fprintf(os.Stderr, "Error: %s\n", errorMsg)
				}
			}

			if finalAssistantText != "" {
				fmt.Println(finalAssistantText)
			}

			if hadError {
				return fmt.Errorf("turn failed: %s", errorMsg)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&serverAddr, "server", defaultServerAddress(), "gRPC server address")
	return cmd
}
