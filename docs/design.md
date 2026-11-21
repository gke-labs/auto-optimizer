# auto-optimizer design

Status: v1 design, 2026-10-23. This document is the source of truth for the
architecture. It replaces the design of a 2026-10-15 spike that is not part of
this repository. The section [Ideas from the spike](#ideas-from-the-spike)
summarises what we kept from it. Keep this document current when the code
changes. Record changes of direction in the Decisions table with a date.

## What it is

You put goals in a `.goals/` folder in your repository and push to GitHub.
auto-optimizer notices, runs coding agents in Kubernetes sandboxes against a
clone of your repository, and sends pull requests that make progress on the
goals. You review, give feedback, and merge.

The canonical goal is a performance benchmark. It names a command that prints
a number, a direction, and a threshold. Goals can also be less concrete, such
as a code quality bar, a feature list, or a migration. For a benchmark goal the
harness re-measures the candidate and decides whether to open a PR. For a soft
goal the PR is a proposal and the maintainer is the judge.

## Terms

- A **goal** is one Markdown file under `.goals/`.
- A **run** is one attempt to make progress on one goal from one base commit.
  A run ends in a PR, or in a recorded reason why there is none.
- A **session** is one LLM conversation with tools. In phase 1 a run has one
  session. Later a run has a tree of sessions.
- A **sandbox** is a `Sandbox` object from kubernetes-sigs/agent-sandbox. Each
  session has its own sandbox.
- The **workspace** is the sandbox's `/workspace` volume. It holds the clone
  of the repository and the agent's working state.
- The **control plane** is the `ao serve` Deployment. It watches GitHub, runs
  sessions, and serves the UI.
- A **forge** is a source of repository and pull request information, such as
  github.com or a bare git remote.
- The **portal** is the gke-labs/service-portals sidecar. It lets a sandbox
  reach outside services without holding any credentials.

## Decisions

| Date | Topic | Decision |
|---|---|---|
| 2026-11-17 | Web UI authentication | Users sign in with GitHub through the OAuth authorization-code flow; `ao` keeps a signed session cookie (named `session_id`, always setting `Secure`) and stores no GitHub token; an allowlist of GitHub logins decides who may sign in; the gRPC session API on the `--listen` port is not covered and must stay on a trusted network. |
| 2026-11-09 | Sandbox lifetime | A run's Sandbox gets one shutdownTime at creation (the run deadline plus a grace period), and nothing extends it afterwards. The orchestrator deletes the Sandbox when the run ends. |
| 2026-10-23 | Sandbox transport | The control plane drives sandboxes through agent-sandbox's `sandboxd`. sandboxd serves a gRPC `ProcessService` on port 9090 and a REST filesystem API on port 8080. We write no custom in-sandbox server and we do not use `pods/exec`. We also maintain agent-sandbox, so we fix the gaps we find upstream instead of working around them. |
| 2026-10-23 | Agent loop | A Go orchestrator watches GitHub and starts sessions. The session loop is the one from agent-sandbox's `examples/sandboxed-tools`, forked into this repository. The LLM loop runs in the control plane, and every tool call executes in the session's sandbox. In phase 1 each run has one all-in-one session that does the work and opens the PR. Later an outer session gets a tool to start inner sessions, and each inner session runs an agent personality such as gemini-cli, Claude Code, codex, opencode, pi, or a native toolset. |
| 2026-10-23 | Goal format | A goal is `.goals/<name>.md`. The Markdown body is for the agent. Optional YAML front matter holds the machine-readable fields, which are the metric, the verify command, the budget, and the schedule. More goal types come later. |
| 2026-10-23 | GitHub | Production uses a GitHub App. Development must not require one. A forge abstraction, adapted from gke-labs/eval-runner, has a `gitremote` implementation that polls `git ls-remote`, a `github` implementation that works with a personal access token or an App, and a `fake` for tests. |
| 2026-10-23 | Credentials and egress | Every sandbox pod uses gke-labs/service-portals in per-node NRI mode rather than a sidecar. Sandbox pods are labeled for interception, and the orchestrator binds an egress policy via the Service Portal Controller before commands run. The sandbox holds no real tokens. The node agent enforces egress allowlists transparently, terminates TLS, injects credentials per host, and logs LLM usage. |
| 2026-10-23 | Filesystem storage | gke-labs/in-cluster-storage, called ObjectFS, provides the workspace volume. A workspace therefore survives sandbox recreation, and a session can resume. This replaces the tar snapshot and restore of the upstream example. Control-plane state is files under one state directory. In production that directory is an ObjectFS volume. |
| 2026-10-23 | Control plane | One Go binary, `ao`. `ao serve` runs as a normal Deployment on the cluster that has agent-sandbox installed. The UI is server-rendered with `html/template` and uses no JavaScript framework. |
| 2026-10-23 | Metric trust | The agent never decides that a PR is good. For metric goals the orchestrator re-measures the candidate in a clean sandbox before it opens a PR. |
| 2026-10-15 | Billing | The platform holds the LLM keys. The portal injects them, so they never enter the sandbox. Usage is metered per run and per repository, with hard budget caps. Stripe comes later. |
| 2026-10-15 | Deployment | kind for local development. The GKE cluster that already runs agent-sandbox for real use. runc first, gVisor through a RuntimeClass later. |

Three things changed since the spike. The agent loop moved from inside the
sandbox to the control plane, so there is no in-sandbox worker entrypoint and
no CLI tool belt for a supervisor agent. A plain state directory on a cluster
filesystem replaced the blob-store SDKs. The forge abstraction replaced a
token-only first cut, so a token and an App both work from day one.

## Architecture

```
            GitHub (github.com or any git remote)
              ^  webhooks (App) or git ls-remote polling (forge)
              |  PRs, comments, checks (forge Reporter)
              v
 +------------------------------------------------------------------+
 |  ao serve  (Deployment, namespace ao-system)                      |
 |                                                                  |
 |   forge syncer --> goal discovery --> run scheduler              |
 |                                           |                      |
 |   state dir (ObjectFS volume)             v                      |
 |   repos/<host>/<owner>/<name>/...   session manager              |
 |                                     (LLM loop, tool registry)    |
 |   web UI (html/template)                  |                      |
 +-------------------------------------------|----------------------+
                                             | sandboxd gRPC :9090 + REST :8080
                                             | (signed token; #102 tracks TLS)
                                             v
 +------------------------------------------------------------------+
 |  Sandbox (agents.x-k8s.io/v1beta1), namespace ao-sandboxes        |
 |  label: service-portal.gke.io/intercept: "true"                  |
 |  (egress policy bound dynamically via Service Portal Controller) |
 |                                                                  |
 |   init: install-sandboxd      (copies sandboxd into /ao-bin)     |
 |   container worker: tool-rich image, runs sandboxd as PID 1      |
 |       /workspace  <- ObjectFS PVC (exclusive-local, RWOP)        |
 |       repo clone, git worktrees, agent CLIs (inner sessions)     |
 +------------------------------------------------------------------+
```

### Components

| Package | Origin | Purpose |
|---|---|---|
| `cmd/ao` | new | The single binary. `ao serve` runs the control plane and the UI. `ao run` runs one run from a laptop against kind. `ao goal validate` checks a goal file. |
| `pkg/forge` | adapted from eval-runner `pkg/forge` | Defines the `Forge` interface for repositories, branches, change requests, and commits, and the `Reporter` interface for comments and statuses. We add a `ChangeRequestCreator` interface for opening PRs. Implementations are `github`, which authenticates with a token or an App installation token, `gitremote`, which lists refs in pure Go without credentials, and `fake`. The `syncer` polls and emits events. The `store` is a rebuildable local cache. |
| `pkg/goals` | new | Finds `.goals/*.md` at the head of the default branch, parses the front matter, validates it, and computes a content hash so that an unchanged goal is not re-run needlessly. |
| `pkg/session` | forked from agent-sandbox `examples/sandboxed-tools/pkg/{agent,sessions}` | Runs the agentic loop. It holds the messages, dispatches tool calls, asks approval hooks, persists the transcript, and extends the sandbox lifetime. We changed it to talk to sandboxd and to use a workspace volume instead of tar snapshots. |
| `pkg/sessionapi` | new | Serves the Session gRPC API (CreateSession, ListSessions, GetSession, ListMessages, SendMessage). Runs agent turns via `pkg/session`, manages session concurrency, and streams turn events. |
| `pkg/llm` | forked from `sandboxed-tools/pkg/llm` | A minimal OpenAI-compatible chat client with tool calling, plus a `fake` for tests. Gemini and Anthropic both offer OpenAI-compatible endpoints. |
| `pkg/tools`, `pkg/toolsets` | forked from `sandboxed-tools/pkg/{tools,toolsets}` | The tool interface, the registry, output limiting, and the `basic` and `geminicli` toolsets. Our worker image ships the `geminicli-toolbox` helper binary. The new `ao` toolset adds `measure`, `open_pull_request`, `finish`, and later `start_session`. |
| `pkg/acp` | forked from `examples/agentclientprotocol/pkg/acp` | An Agent Client Protocol client. We use it to drive inner CLI agents that speak ACP, such as gemini-cli with `--acp`, over a sandboxd `Start` stream. This gives us streamed tool calls and permission gating without parsing terminal output. |
| `pkg/agents` | new, idea from the spike | Declarative definitions of CLI agent personalities. Each definition has a command template, a rule for how the prompt is delivered, and an output parser that extracts the summary and the token usage. |
| `pkg/sandbox` | new | Builds the `Sandbox` object for a run, or a `SandboxClaim` once we use warm pools. Waits for readiness, holds the sandboxd client with its bearer token and address, extends `shutdownTime`, and deletes the sandbox. It wraps `sigs.k8s.io/agent-sandbox/clients/go/sandbox` where that fits. |
| `pkg/portal` | new | Renders the per-run `PortalRule`s, which say which hosts the sandbox may reach and which token each host gets, into the Secret that the sidecar reads. Parses the portal's usage logs for metering. |
| `pkg/measure` | new, idea from the spike | Implements the metric protocol, the median of N runs, the verify gate, and the improvement arithmetic. It runs commands through sandboxd `Execute`. |
| `pkg/run` | new | Reconciles runs with a typed controller-runtime reconciler instead of a timer loop. controller-runtime was chosen for its work queue with de-duplication and backoff, channel source for internal notifications, typed watch sources for Kubernetes resources, and bounded worker concurrency. |
| `pkg/state` | new | Defines the file layout under the state directory, atomic JSON writes, and append-only JSONL. |
| `pkg/web` | new | Serves the repository list, the repository page with goals and run history, and the run page with the session transcript, tool calls, measurements, and PR link. Draws an inline SVG chart of the metric over time. Web pages follow the one-function-per-page convention (`RenderXPage`) with their template and data type in the same file. |

We build and deploy with the gke-labs `ap` tool and its conventions, which are
a `.ap/` directory and `dev/tasks/*` scripts. eval-runner and service-portals
do the same.

## Goals

A goal is a Markdown file under `.goals/` on the default branch. The file name
is the goal name, so `.goals/sort-benchmark.md` defines the goal
`sort-benchmark`. The name appears in branch names, PR titles, and the UI. The
agent sees the body verbatim. The front matter is optional and holds
everything the harness needs to measure and budget the goal.

```markdown
---
metric:
  command: ./hack/bench.sh      # run from the repo root; last stdout line is the number
  direction: lower               # lower | higher
  unit: ns/op
  minImprovement: 1%             # absolute (5) or relative (1%); default: any improvement
  runs: 3                        # repeat, take the median; default 1
  timeout: 10m
verify: go test ./...            # must exit 0 for a candidate to count
budget:
  maxDuration: 2h
  maxCostUSD: 20
schedule: daily                  # daily | weekly | on-change (default) | manual
agents:                          # optional allowlist of inner-session personalities
  - gemini
  - claude
---
# Make the sort benchmark faster

Do not change the public API of `pkg/sort`. The hot path is `quick.go`.
Measure with `./hack/bench.sh`.
```

The front matter decides the kind of goal.

- A **metric goal** has a `metric` section. The harness measures the baseline
  and the candidate in clean sandboxes. It opens a PR only if `verify` passes
  and the candidate clears `minImprovement`. The PR body carries the before
  and after numbers.
- A **judged goal** has no `metric` section. The session works until it calls
  `finish`. The harness runs `verify` if the goal has one, checks that the
  diff is not empty, and opens the PR as a proposal. Feature lists and code
  quality goals use this kind until we add rubric and checklist goal types.

The metric protocol is simple. The metric command runs from the repository
root. Its last non-empty line on stdout is the measurement. That line is a
bare number such as `1234.5`, a number with a unit such as `1234.5 ns/op`, or
JSON such as `{"value": 1234.5, "unit": "ns/op"}`. A non-zero exit code is a
failed measurement, never a number. The run keeps everything else the command
prints in its logs, so a metric script can be chatty. `minImprovement` is
absolute, such as `5`, or relative, such as `1%`, and the harness applies it
in the goal's direction.

A goal never changes what is measured during a run. The orchestrator freezes
the goal file at the base commit. If an attempt edits `.goals/` or the metric
script, the orchestrator rejects it.

## Watching GitHub: the forge

The forge layer isolates everything GitHub-specific. It also makes
development possible without a GitHub App.

- `gitremote` lists refs with go-git's ref advertisement and fetches trees at
  depth 1. It is free, unmetered, and works for any HTTPS git remote, including
  a local git daemon in tests. It cannot open PRs.
- `github` is a REST client using `net/http` only. It authenticates with an App
  installation token or a personal access token (`GITHUB_TOKEN`). It reuses
  `gitremote` for `ListRefs`, `FetchTree`, and `DefaultBranch` with HTTP basic auth,
  and implements `ChangeRequestCreator` and `PushCredentialer`. Its reads use
  conditional requests and respect a rate-limit floor.
- `fake` is in-memory and serves unit tests.

```go
type Forge interface {
	ListRefs(ctx context.Context, repoURL string) (map[string]string, error)
	FetchTree(ctx context.Context, repoURL string, commit string, prefix string) (fs.FS, error)
	DefaultBranch(ctx context.Context, repoURL string) (string, error)
}

type ChangeRequestCreator interface {
	OpenPullRequest(ctx context.Context, repoURL, head, base, title, body string) (*PullRequest, error)
	ListPullRequests(ctx context.Context, repoURL, headPrefix string) ([]PullRequest, error)
}

type PushCredentialer interface {
	PushCredentials(repoURL string) (username, password string)
}
```

Polling follows eval-runner's three tiers. The syncer runs `ls-remote` every
60 seconds with jitter to detect changes. It calls the API only for refs that
moved. Once an hour it runs a full conditional reconcile. Webhooks from the
App for `push`, `pull_request`, and `issue_comment` events feed the same
consumer, so they speed things up without adding a second code path. The
webhook handler holds no credentials and is idempotent.

The consumer acts on these events.

| Event | Action |
|---|---|
| The default branch advanced. | Re-read `.goals/`. Create or update the goal records. Schedule a run for each goal whose `schedule` is `on-change` and whose content hash or base commit changed. |
| One of our PRs was closed or merged. | Mark the run. For a merged PR, record the new best value. Release the workspace volume after the retention window. |
| Someone commented on one of our PRs. (later) | Resume the run's session with the review comments as the next user turn, and push to the same branch. |
| A collaborator commented `/ao run <goal>`. (later) | Start a run by hand. |

Registration works in two ways. In production, installing the App on a
repository registers it. In development, `ao serve --repo <url>` or the UI
form registers a repository against the `gitremote` forge or the token-backed
`github` forge.

## Sessions

A session is one LLM conversation with tools, bound to one sandbox and one
workspace volume. The loop is the upstream `Harness.RunTurn`. It calls the
model, executes the tool calls in the sandbox, appends the results, and
repeats until the model answers without a tool call. We keep the upstream
`TurnEvents` hook, so the same loop can stream the transcript to the state
directory for the UI and can be observed in tests.

We change the upstream example in five ways.

- **Execution** goes through sandboxd. Short tool calls use `Execute`. Long
  ones such as benchmarks, test suites, and inner agents use the `Start`
  stream. A tool timeout sends `SendSignal`. The existing limited writer caps
  what the model sees, and the full output goes to the run's state directory.
- **Filesystem** operations use the sandboxd REST API for read, write, and
  list. The `geminicli-toolbox` binary provides glob, grep, and replace.
- **Persistence** comes from the workspace volume. There is no snapshot step.
  If the sandbox is recreated after expiry, node loss, deletion, or a resume for review
  comments, the new pod mounts the same volume. A run's session gets its Sandbox
  on demand: a Sandbox that disappears or fails during `Working` is not an event
  that fails the run. The next tool call creates a Sandbox on demand (with the run's
  labels, owner, and workspace claim, portal egress policy bound, repo clone present,
  and git identity configured), and a tool call interrupted by sandbox loss is retried.
- **Lifetime** is set once at creation to the run deadline plus a 15-minute
  grace period, and nothing extends it afterwards. The orchestrator deletes
  the Sandbox when the run ends. The run deadline remains the bound: a run past
  its deadline ends as failed with `deadline_exceeded`. `shutdownPolicy: Delete` is the
  safety net when the control plane dies.
- **Model access** differs by session kind. The control plane calls the model
  with the platform keys for native sessions. Inner CLI agents inside the
  sandbox reach the model APIs through the portal sidecar.

### Session API

The control plane exposes a gRPC service defined in `api/session/v1/session.proto`
and served by `ao serve`. It provides five RPCs: `CreateSession`, `ListSessions`,
`GetSession`, `ListMessages`, and `SendMessage`.

`SendMessage` appends a user message and runs one turn of the agentic loop,
streaming events (`MessageAppended`, `ToolCallStarted`, `ToolCallFinished`,
`TurnFinished`, `Error`) until the turn completes. Only one turn runs at a time
per session; a second `SendMessage` call on a busy session returns
`FAILED_PRECONDITION`. Tool calls are auto-approved by the server. Sandboxes are
created lazily on the first tool call and reused across turns of the same session.

The server binds to localhost by default (`127.0.0.1:8081`) and provides no
authentication in this phase; it must not be exposed to untrusted networks.
Authentication and authorization will be added alongside the GitHub App.

### The `ao` toolset for phase 1

The system prompt contains the goal body, facts about the repository such as
the default branch, the base commit, and the detected languages, the metric
and verify commands, the budget, and the rules. The rules say: never edit
`.goals/` or the metric script, keep changes focused, and end with a summary
written for maintainers.

| Tool | Where it runs | Notes |
|---|---|---|
| `run_command`, `read_file`, `write_file`, `replace`, `glob`, `grep_search`, `list_directory` | sandbox | These come from the `geminicli` toolset. |
| `measure` | control plane, which executes commands in the sandbox | Runs `verify` and `metric` on the current tree, records a measurement in the run, and returns a compact result next to the baseline. |
| `open_pull_request(title, body)` | control plane | Commits any uncommitted changes, pushes the branch `ao/<goal>/<run-id>` through the portal, and hands over to the orchestrator's select step. The tool result tells the model whether a PR was opened, and if not, why. |
| `finish(summary)` | control plane | Ends the session without a PR. |
| `start_session(personality, task, branch)` | control plane, phase 2 | Starts an inner session and returns its summary and branch. |

### Inner sessions and personalities for phase 2

The outer session is a planner with a small context budget. It delegates to
inner sessions. Each inner session has its own sandbox and workspace volume,
cloned from the repository at a branch the outer session names. An inner
session hands work back as a pushed branch plus a short summary. The outer
session measures and decides what to submit.

There are two kinds of personality.

- A **native** personality is the same Go loop with a different toolset,
  system prompt, and model. A "Gemini reviewer" or a "Claude implementer" are
  examples.
- A **CLI** personality is an agent CLI from `pkg/agents`, such as gemini-cli,
  Claude Code, codex, opencode, or pi. The session manager starts it with
  sandboxd `Start` in the inner sandbox, writes the prompt to stdin, and
  streams the transcript back. Where the CLI speaks ACP, `pkg/acp` drives it
  over the same stream, so tool calls are visible and can be gated.

This is the comparison we care about. The same goal and the same budget go to
different personalities, and the same harness measures the results.

## Sandboxes

Each worker image has one `SandboxTemplate`, named `ao-worker`. A run creates
a `Sandbox` from it. Once startup latency matters, a run creates a
`SandboxClaim` against a `SandboxWarmPool` instead.

The pod runs as uid 1000 with `automountServiceAccountToken: false`, every
capability dropped, and `allowPrivilegeEscalation: false`. Resource limits
come from the goal's budget tier. `runtimeClassName: gvisor` is optional.

The **worker** container runs our tool-rich image. The image has git, Go,
Node, Python, build essentials, the agent CLIs, and `geminicli-toolbox`. An
init container copies sandboxd into the pod, which is topology B in the
sandboxd user guide, so commands run in the tool-rich root filesystem. The
image ships `tini`, so sandboxd is not PID 1 in a long-lived sandbox.
`/workspace` is the sandboxd root. It is an ObjectFS volume: the orchestrator
creates the claim directly with the `objectfs-exclusive-local` StorageClass,
`ReadWriteOncePod` access, and a periodic checkpoint, and the run's `HeroRun`
owns it. sandboxd runs with
`--auth-public-key` and `--auth-audience`. The control plane signs short-lived
tokens per call with its ed25519 key, and no Secrets are created for sandbox
authentication.

The **portal** integration uses service-portals in per-node NRI mode rather than
a sidecar. Sandbox pods are created with the `service-portal.gke.io/intercept: "true"`
label, and the node agent intercepts network traffic transparently. When the Sandbox
becomes ready, the orchestrator binds an egress policy via the Service Portal Controller
before any commands run. Only the allowed egress hosts, such as `api.github.com`,
`github.com`, and package registries, may be reached. The node agent denies all egress
until the policy is bound. Agent CLIs therefore need no base-URL changes.

There is no per-sandbox ingress **NetworkPolicy** (removed as too expensive at
sandbox scale); the signed token gates access to sandboxd, and transport
security (TLS) is tracked in #102. The portal's per-node NRI interception and
deny-by-default policy handle egress.

The control plane connects to sandboxd in-cluster, by Pod IP or by the
headless Service that `spec.service: true` creates. We do not use
`sandbox-router`, because it cannot carry gRPC today. Readiness is the Sandbox
`Ready` condition, which reflects sandboxd's `/v1/health` probe.

Git access goes over HTTPS through the portal. The portal injects the
installation token or the personal access token. The token's scope limits
pushes to `contents: write` on the one repository, and the portal can further
restrict methods and paths. The portal's `gitproxy-server` can cache clones of
large repositories across runs.

## Run lifecycle

Runs are driven by a typed controller-runtime reconciler (`pkg/run`) rather than
a polling loop or timer. The unit of reconciliation is one run, identified by its
repository key and run ID. Reconcile loads `run.json`, inspects the current phase,
takes one step (such as preparing the sandbox or executing an agent session turn),
persists state and events, and returns.

```
Pending -> Preparing -> Working -> Selecting -> OpeningPR -> Succeeded
                                     |              |
                                     |              +-> NoImprovement
                                     +-> Failed / Cancelled / Expired
```

### Events and concurrency

Runs are reconciled in response to events:
- **Scheduler creation:** When the scheduler creates a `Pending` run in the state store, it calls `Orchestrator.Notify(repo, runID)` to enqueue the run immediately.
- **Sandbox watch and readiness:** The controller watches Sandbox resources in the sandbox namespace via a generated shared informer, mapped to runs through the `auto-optimizer.labs.gke.io/run` label. When a Sandbox status changes or the Sandbox is deleted, the run is enqueued. Once the Sandbox becomes `Ready`, the second step of `Preparing` clones the repository at the base commit, moves the run to `Working`, and calls `Orchestrator.Notify(repo, runID)` to trigger session execution.
- **Worker concurrency:** The reconciler runs with configurable concurrency via `--max-concurrent-runs` (default 4), so runs in different repositories proceed in parallel without blocking each other. The controller-runtime workqueue de-duplicates notifications for the same run.

### Restart and shutdown semantics

- **Startup:** When the manager starts, the orchestrator enqueues all existing runs that are not in a terminal phase. Runs in `Preparing`, `Working`, or other active phases are re-enqueued for reconciliation.
- **Shutdown:** When the control plane shuts down (context cancelled), `workRun` returns cleanly without changing the run phase. The run remains `Working` in the state store rather than failing with a context-cancelled error, allowing the next server start to pick it back up.

### Phases

1. **Pending.** A goal needs a run because its schedule is due, its content
   changed, or someone asked. The scheduler allows one active run per goal
   and enforces the repository's budget cap.
2. **Preparing.** Split into two reconcile steps without blocking a worker:
   - *Step 1 (creation):* When a workspace StorageClass is configured, the
     orchestrator creates the claim directly and the run's `HeroRun` owns it
     (otherwise the workspace uses an `emptyDir`). The orchestrator creates the
     Sandbox with a label naming the run (and with `service-portal.gke.io/intercept`
     label when a portal binder is configured), records `SandboxName` and a
     `ReadyDeadline` on the run, appends an event, and returns with `RequeueAfter`.
   - *Step 2 (readiness):* A watch on Sandbox resources enqueues the run when
     its status changes. When the Sandbox becomes `Ready`, if a portal binder is
     configured the orchestrator reads the pod UID and binds an egress policy via
     the Service Portal Controller before any commands run in the sandbox
     (retrying on error until `ReadyDeadline`, failing with `portal_bind_failed`
     on timeout, and recording `portal_bound` with the host list on success).
     Then the orchestrator clones the repository at the base commit into
     `/workspace/repo`, freezes the goal file, measures the baseline for a
     metric goal, moves the run to `Working`, and enqueues it. If the Sandbox
     disappears or reports a failure before becoming ready, or if the ready
     deadline passes without the Sandbox becoming ready, the run transitions
     to `Failed`.
3. **Working.** The session runs turn by turn in the control plane, obtaining its
   Sandbox on demand when tool calls execute. If a Sandbox is deleted or dies in the
   middle of a turn, the interrupted tool call runs again in a new Sandbox on the same
   workspace claim (or cloned anew if no claim is used) and the run carries on. The orchestrator
   ends the run at the deadline if it has not finished. The session ends
   when the agent calls the `finish` or `open_pull_request` tool, when the model
   stops, or when a run limit is reached. The limits are `maxDuration`,
   `maxCostUSD`, a maximum number of turns, and the tool timeout. The agent
   commits its own work before asking for a pull request. The control plane sets
   the agent's git identity during preparation. Every tool call, its truncated
   result, and its full output go to the state directory as they happen, so the
   UI shows progress live.
4. **Selecting.** When the goal specifies a `verify` command, the orchestrator
   runs it in the sandbox bounded by `--verify-timeout`. A failing command ends
   the run as `NoImprovement` with the event `verify_failed`. Execution errors
   fail the run with `verify_error`. When verification succeeds or when the goal
   lacks a verify command, the control plane exports a git packfile containing
   the commits from the base commit to HEAD out of the sandbox. The control
   plane enforces refusals on the candidate commits. A run with no changes
   against the base commit ends in `NoImprovement` with the event `no_changes`.
   The control plane refuses changes under `.goals/` as `NoImprovement` with
   the event `refused`. Uncommitted changes in the sandbox fail the run. Next,
   the control plane pushes the branch through the forge with a lease on the
   base commit. If the branch moved on the remote forge, the run fails with the
   event `branch_moved`. Other push errors fail the run with the event
   `push_failed`. The orchestrator deletes the sandbox when the run leaves
   `Selecting`. Measuring metric goals in a clean sandbox is planned, not built
   (epic #32).
5. **OpeningPR.** When the forge supports change requests, the orchestrator
   checks for an existing open pull request for the branch. If an open pull
   request already exists, the orchestrator reuses it. Otherwise, the
   orchestrator opens a new pull request through the forge. The harness writes
   the body with an introductory section naming the goal, short base commit,
   branch, and run ID, alongside a link to the run page when configured. It
   appends the agent's requested body under the heading "What the agent says".
   The orchestrator records the pull request URL on the run. When the forge does
   not support change requests, the run succeeds with the event
   `pull_request_not_opened`.
6. **Terminal.** The orchestrator deletes the sandbox. The sandbox's
   `shutdownPolicy: Delete` serves as a backstop if the control plane terminates
   unexpectedly. The orchestrator updates the `HeroRun` status to the terminal
   phase synchronously (#194).

   Retaining workspaces and cleaning up portal secrets is planned (epic #33 for
   the portal and workspace). Kubernetes deletes the portal Secret with the
   sandbox through owner references. The workspace volume stays for the
   retention window so the session can resume with review feedback. The
   orchestrator deletes the volume when the pull request closes or merges, or
   when the window passes.

A run has one deterministic branch name, so re-running the OpeningPR step
reuses the existing open PR instead of opening another. The scheduler skips a goal
that already has an open auto-optimizer PR, unless the goal's `schedule` says
otherwise.

### Runs as custom resources: the HeroRun decision

The orchestrator stores runs as JSON and JSONL records in the state
directory. The reconciler keys work by `reconcile.Request` (the namespace and name
of the `HeroRun` object). The orchestrator watches HeroRuns and their child
Sandboxes with controller-runtime's standard `For` and `Owns` builder.
Representing runs as a Kubernetes custom resource offers clear benefits, but it
also carries significant costs.

A `HeroRun` custom resource buys standard Kubernetes integration:
- **Owner references.** Sandboxes declare an owner reference to their
  `HeroRun`, allowing controller-runtime's standard `For` and `Owns` watches to
  reconcile the run whenever a child Sandbox changes.
- **Garbage collection.** Kubernetes deletes child Sandboxes automatically when
  someone deletes a `HeroRun`.
- **Cluster visibility.** Operators can inspect runs directly through
  `kubectl get heroruns`, and controllers can observe progress through status
  conditions.
- **Standard reconcile requests.** The reconciler operates on standard
  `reconcile.Request` values containing a namespace and a name.

A `HeroRun` custom resource would cost simplicity and storage efficiency:
- **Violates "State is files".** The project design relies on a single state
  directory with one writer per file, append-only JSONL events, and no database.
  A custom resource moves run state into etcd.
- **Exceeds etcd limits.** A run accumulates tool outputs, model messages, and
  streamed turn events. These records quickly exceed etcd's 1.5 MB object size
  limit, so transcripts cannot live in the custom resource.
- **Requires an API server.** Reading past runs requires an active Kubernetes API
  server. With file records, developers can inspect runs offline with standard
  tools.
- **Breaks the web UI.** The web server reads repository pages and transcripts
  directly from the state directory. Storing runs in custom resources requires
  rewriting the web server or adding a synchronization loop.

Two shapes for `HeroRun` exist:
- **Thin `HeroRun` custom resource as a rebuildable cache.** The state directory
  on object storage stays the source of truth. The orchestrator publishes a
  minimal `HeroRun` object into the cluster to establish ownership and visibility.
  The orchestrator can reconstruct deleted `HeroRun` objects from disk at any
  time.
- **`HeroRun` as the primary source of truth.** The custom resource stores the
  authoritative run state, while transcripts and events remain on disk. Moving the
  source of truth into `HeroRun` remains an open question: it would require
  updating the web UI to read from both the API server and the state directory,
  and reconciling status updates across two stores.

We decide to adopt `HeroRun` as a thin, rebuildable custom resource. The state
directory on object storage (GCS in production, via the workspace storage in this
design) remains the source of truth for runs, sessions, and events. We do not use
the Kubernetes API server as a database. `HeroRun` places the run into the
Kubernetes object hierarchy, owns the run's Sandboxes, and enables standard
controller-runtime reconciliation. To prevent frequent session events from
generating excessive API server traffic, the orchestrator writes `HeroRun` status
at a bounded rate (at most once every 3 seconds per object). The orchestrator
deletes the sandbox when a run reaches a terminal phase, and
`spec.shutdownPolicy: Delete` (set in `pkg/session/agent.go`) serves as the
backstop. A missing or deleted `HeroRun` is not data loss, because the
orchestrator can rebuild every `HeroRun` from the state directory.

Status writes distinguish between two tiers based on who consumes them:
progress ("watched by a person") versus critical ("acted on by another actor").
Happy-path phase transitions (`Pending` to `Preparing` to `Working` to `Selecting`
to `OpeningPR`), `sandboxName`, `branch`, and `pullRequestURL` updates belong to
the progress tier. They are coalesced and written at a bounded interval because
human observers tolerate modest delays. Transitions into terminal phases or
unhappy states (where `Ready` becomes `False` because the phase became `Failed`)
belong to the critical tier. Another actor must act promptly on these
transitions for sandbox deletion, alerting, or garbage collection. Critical
transitions are therefore written synchronously before the orchestrator
continues. If synchronous writing fails after one retry, the run falls back to
the dirty set so the orchestrator never blocks. Orchestrator shutdown flushes
are also critical. If this policy proves out here, a reusable version could
move to a shared library later.

To implement `HeroRun`, we will:
1. Define the `HeroRun` custom resource: the type lives in
   `api/herorun/v1alpha1` and the CRD under `k8s/crds/`.
2. Publish thin `HeroRun` objects and report status: the orchestrator creates
   each object when a run is created, rebuilds missing ones in `Resume`,
   and keeps status updated at a bounded rate (every 3 seconds per dirty object).
3. Add owner references from each created `Sandbox` to its `HeroRun` object:
   every Sandbox created for a run carries an owner reference naming its
   `HeroRun` with `controller: true`.
4. Replace `sandboxSource` with standard `For` and `Owns` watches (done).
5. Keep session transcripts, tool outputs, and events in the state directory.

## State

Every record is a file under the state directory. The `--state-dir` flag
names it. In production it is an ObjectFS volume. In development and tests it
is a local directory. JSON objects are written atomically. Logs are
append-only JSONL. The forge cache, a bbolt file, lives there too and can be
deleted at any time.

```
repos/<host>/<owner>/<name>/repo.json
repos/<host>/<owner>/<name>/goals/<goal>/goal.json           parsed goal, hash, best so far
repos/<host>/<owner>/<name>/runs/<run-id>/run.json           phase, timings, baseline, candidate, PR, cost
repos/<host>/<owner>/<name>/runs/<run-id>/events.jsonl       phase changes, measurements, errors
repos/<host>/<owner>/<name>/runs/<run-id>/sessions/<session-id>/metadata.json
repos/<host>/<owner>/<name>/runs/<run-id>/sessions/<session-id>/messages.jsonl
repos/<host>/<owner>/<name>/runs/<run-id>/sessions/<session-id>/tools/<n>.json   full tool output
repos/<host>/<owner>/<name>/usage/<YYYY-MM>.json              metering roll-up
forge/forge.db                                                 rebuildable cache
```

Run IDs sort by time, for example `20261023T120000Z-ab12`. Each file has
exactly one writer. The session manager owns its run directory. The syncer
owns `repo.json` and `goal.json`.

## Security model

- The sandbox holds no credentials for GitHub, for models, or for the cluster.
  Its outbound identity is whatever the portal injects. That identity is
  scoped to one repository and to the platform's LLM projects.
- The sandbox cannot reach the control plane, other sandboxes, the metadata
  server, or arbitrary hosts. Egress is an iptables allowlist owned by a UID
  the workload cannot become, plus a NetworkPolicy.
- The control plane reaches sandboxd over the pod network using asymmetric
  ed25519 authentication. There is no per-sandbox NetworkPolicy (removed as too
  expensive at sandbox scale); the signed token is what gates access. Transport
  security (TLS between the control plane and sandboxd) is tracked in #102:
  - The control plane holds an ed25519 signing key in memory and on its
    persistent state volume (`<state-dir>/sandbox-signing-key` in mode `0600`).
    The private key never leaves the process or volume, and no Kubernetes
    Secret is created for sandbox authentication; the control plane's
    ServiceAccount holds no RBAC permissions on Secrets.
  - When creating a sandbox, only the base64-encoded public key
    (`--auth-public-key`) and expected audience
    (`--auth-audience=<namespace>/<sandbox-name>`) are passed to sandboxd in
    container arguments.
  - For every gRPC call, the client signs a fresh token in
    `v1.<base64url(payload)>.<base64url(signature)>` format with a 5-minute
    lifetime (`exp`) and audience claim matching the target sandbox.
  - **What a captured token can and cannot do**: Within its 5-minute validity
    window, a captured token can only invoke gRPC operations on the specific
    sandbox instance designated by its audience claim (`<namespace>/<sandbox-name>`).
    It cannot authenticate to any other sandbox (the audience check will reject
    it), cannot be used after the 5-minute window expires, cannot be used to
    forge or mint new tokens without the ed25519 private key, and cannot
    access the Kubernetes API or any control plane Secrets.
- Prompt-injected code in a repository can waste budget and produce a bad PR.
  It cannot exfiltrate tokens or touch other repositories. The orchestrator
  enforces the budgets, not the prompt.
- The webhook endpoint holds no credentials. It only enqueues events.

## Metering and budgets

Native sessions report token usage from the API responses. The portal meters
inner CLI agents. The Anthropic and Gemini portals parse request and response
bodies and log usage with the pod identity, and the control plane attributes
that usage to the run. Each run records its cost. Each repository rolls cost
up per month. The scheduler refuses to start a run over the repository's cap,
and the session manager stops a session when the run reaches `maxCostUSD`.

## Web UI

The UI is server-rendered and uses no JavaScript framework. Live pages refresh
with a meta refresh tag.

| Path | Content |
|---|---|
| `/` | Repositories with their goals, last run, and best metric. |
| `/repos/{host}/{owner}/{name}` | Goals with schedule and best value, run history, an SVG trend per metric goal, and a Run now button. |
| `/repos/.../runs/{id}` | Phase, budget used, baseline and candidate, PR link, and the list of sessions. |
| `/repos/.../runs/{id}/sessions/{sid}` | The transcript with tool calls, truncated results, and links to full outputs. |
| `/healthz` | Liveness. |

Users sign in with GitHub through the OAuth authorization-code flow (`/login`,
`/auth/github/callback`, `/logout`). The middleware constructor is
`auth.NewGitHubHandler`. `ao` keeps a signed session cookie named `session_id`
(which always sets `Secure`) and stores no GitHub token; the token is used only
to verify identity via the GitHub API during callback. An allowlist of GitHub
logins (`AO_GITHUB_ALLOWED_USERS` or `--github-allowed-user`) decides who may sign in.
The gRPC session API on the `--listen` port is not covered and must stay on a
trusted network.

## Development without a GitHub App

- `ao serve --forge gitremote --repo <url>` watches any public repository or a
  local git daemon and runs sessions. It skips PRs, or pushes to a local bare
  remote.
- `GITHUB_TOKEN=... ao serve --repo <url>` uses the `github` forge with a
  personal access token and opens real PRs from your account. Use a fork,
  because merging your own bot's PR is a self-merge.
- `ao run --repo . --goal sort-benchmark` runs once from a laptop against the
  current kubeconfig. `dev/tasks/kind-up` installs agent-sandbox, the sandboxd
  image, and ObjectFS on kind.
- Unit tests use the `fake` forge, a fake LLM with scripted tool calls, and a
  fake sandboxd client. One end-to-end test runs on kind in CI.

## Ideas from the spike

A one-week spike on 2026-10-15 built a working vertical slice with a different
shape. A worker process inside the sandbox ran a supervisor coding agent, and
that agent delegated to subagents through a CLI tool belt. We are not
following the spike closely, but these ideas held up and inform this design.
Treat them as guidance, not as a specification.

- **The harness decides, the agent proposes.** The spike never trusted the
  agent's own numbers. Go code re-measured the final tree, and the PR body
  came from that measurement. An agent-reported value is at most context for
  the reviewer.
- **A tiny metric protocol beats a plugin API.** Any command that prints a
  number on its last line is a metric. A non-zero exit is a failure, not a
  value. Repeat the command and take the median. A repository can adopt this
  with a shell script. The Goals section describes the protocol in full.
- **Small outputs for the model, full outputs on disk.** Tool calls returned
  about 40 lines and wrote everything else to the run directory. The model's
  context stayed small, and the UI still had the full story. Our limited
  writer and per-tool output files keep this.
- **Context discipline for the planner.** The supervisor prompt said: spend a
  few turns on the layout, never run the benchmark yourself, give subagents
  complete self-contained tasks, prefer several small independent attempts in
  separate worktrees over one long attempt, ask each subagent for a five-line
  summary, and stop before the deadline. This carries straight into the outer
  session's system prompt in phase 2.
- **Agents as data, not code.** Each CLI agent was a definition with a
  command template, a flag for whether the prompt goes on stdin, and an output
  mode that extracts the final message and the token usage from the CLI's
  JSON stream. Adding an agent was a YAML entry. These headless flags worked
  in October 2026: `claude -p --output-format stream-json
  --dangerously-skip-permissions`, `gemini --approval-mode yolo --skip-trust
  --output-format json`, `codex exec --dangerously-bypass-approvals-and-sandbox
  --json`, `opencode run --auto --format json`, and `pi --mode json`. Model
  names differ by CLI. opencode and pi take `provider/model`, and the others
  take bare model ids. The auto-approve flags are acceptable only because the
  sandbox is the permission boundary.
- **Freeze the goal at the base commit.** The spike copied the goal file out
  of the tree before the agent started, so the agent could not change what
  was being measured. An attempt that edits the goal or the metric script is
  rejected.
- **Exactly one writer per state object.** State was JSON files keyed by a
  time-sortable run ID such as `20261015T120000Z-ab12`. Each object had one
  writer and no partial updates. Concurrency problems never appeared. We keep
  the same rule.
- **Let the sandbox controller be the safety net.** Each run's `Sandbox` had
  `shutdownPolicy: Delete` and a `shutdownTime` of the deadline plus a grace
  period, so a dead control plane could not leak pods. Everything of value
  was persisted outside the pod before exit.
- **Sandbox image recipe.** The image was Debian slim with Go, Node, Python,
  build essentials, the agent CLIs installed with npm, and a non-root uid 1000
  user with git configured and `GIT_TERMINAL_PROMPT=0`. Our worker image
  starts from this and adds the sandboxd injection, `tini`, and
  `geminicli-toolbox`.
- **Dogfooding through a checklist.** The spike's own goal was the percentage
  of acceptance checks that passed in a spec file. Each check was a fast
  command such as `go test -run TestX`, and the rule was that weakening a
  check is cheating. This returns as a checklist goal type and as this
  repository's own `.goals/`.
- **A fake agent and a local bare remote** made the whole lifecycle testable
  without network access or credentials. Our fake LLM and fake forge serve
  the same purpose.

We dropped four things on purpose: the in-sandbox worker entrypoint, the
supervisor CLI tool belt, the blob-store SDKs for state, and the token-first
sequencing.

## Expected upstream work

This design leans on a few things that upstream does not fully provide yet.
Each one becomes an issue in the respective project, not a workaround here.

- agent-sandbox. `sandbox-router` cannot proxy gRPC, so sandboxd needs direct
  Pod IP or Service connectivity and a custom NetworkPolicy, because the
  managed policy admits only the router. A per-sandbox `env` or Secret volume
  forces a cold start from a warm pool, which conflicts with per-run portal
  rules and sandboxd tokens. sandboxd as PID 1 does not reap orphans. `Start`
  needs a way to bound output, or the client has to. Specific gaps in the Go SDK
  (`sigs.k8s.io/agent-sandbox/clients/go/sandbox`):
  - Attaching to a plain `Sandbox` without a `WarmPoolName`/`ClaimName`: the SDK
    strictly requires `WarmPoolName` and binds to `SandboxClaim` resources. When
    an orchestrator manages raw `Sandbox` CRDs directly, the SDK provides no
    constructor (e.g. `NewForSandbox(ctx, name, namespace)`) to attach to an
    existing sandbox.
  - Bearer token authentication: `sandbox.Options` provides no option for passing
    bearer credentials, preventing client requests to `sandboxd` when running
    with `--auth-token-sha256` or `--auth-token-file` (which requires passing
    `authorization: Bearer <token>` in gRPC metadata and HTTP headers).
  - Stdin streaming: while `Commands.Start` returns a `ProcessHandle` with
    `Recv` and `SendSignal`, it does not support streaming standard input via
    `ProcessService.WriteStdin`.
  - Asymmetric authentication in sandboxd: sandboxd upstream only verified
    symmetric bearer tokens against a SHA-256 digest (`--auth-token-sha256`).
    Supporting ed25519 public key verification allows passing the public key in
    container args (`--auth-public-key`) along with `--auth-audience` while
    keeping the private key solely in control plane memory, eliminating the need
    for Kubernetes Secret objects and symmetric token management. This change
    lives on the fork branch `justinsb/agent-sandbox:sandboxd-asymmetric-auth`
    (at commit `76156ce`) until it is merged and released upstream in
    `kubernetes-sigs/agent-sandbox`.
  - Multi-platform image import: staging images built with multi-platform
    manifests can fail `kind load docker-image` (`ctr: content digest ... not found`),
    requiring in-node pulls (`crictl pull`) as a workaround.
- service-portals. The sidecar reads its rules from a mounted Secret at start
  and on `SIGHUP`. A configuration API would allow warm pools and token
  rotation without a Secret per pod. We must verify the git smart-HTTP
  authorization header that GitHub accepts for installation tokens through
  `all-in-one-portal`. The usage logs need a structured format that the
  control plane can attribute to a run.
- in-cluster-storage. We need `volumeClaimTemplates` on `Sandbox` to work
  with the `objectfs-exclusive-local` StorageClass and `ReadWriteOncePod`.
  Hand-off between a deleted and a recreated sandbox pod relies on the
  controller's hold lease. The checkpoint interval has to suit the cadence of
  session tool calls.

## Roadmap

Each milestone is a handful of issues assigned to the coding bot.

1. **Skeleton.** `cmd/ao`, `pkg/state`, and `pkg/goals` with front matter
   parsing and validation. `pkg/forge` adapted from eval-runner with
   `gitremote`, `github` with a token, `fake`, and the syncer. `ao serve`
   lists repositories and goals in the UI. No sandboxes yet.
2. **Sessions on kind.** Fork `sandboxed-tools` into `pkg/session`, `pkg/llm`,
   `pkg/tools`, and `pkg/toolsets`. Write `pkg/sandbox` for sandboxd with a
   bearer token, Pod IP connectivity, and a NetworkPolicy. Build the worker
   image with injected sandboxd. `ao run` executes a session against a
   fixture repository with the fake LLM.
3. **All-in-one PR.** The `ao` toolset with `measure`, `open_pull_request`,
   and `finish`. Baseline and select steps in clean sandboxes. PRs through
   the `github` forge. Run and session pages in the UI. The first real PR on
   a test repository.
4. **Portal and ObjectFS.** The portal sidecar with per-run rules and token
   minting. The workspace on ObjectFS. Resume a session after review
   comments.
5. **GitHub App.** The installation flow, the webhook accelerator,
   installation tokens, and OAuth for the UI. Deploy to GKE.
6. **Inner sessions.** `start_session`, native and CLI personalities, ACP for
   gemini-cli, and a comparison page across personalities.
7. **Operations.** Metering from portal logs, budgets, warm pools, gVisor,
   checklist and rubric goal types, and this repository's own `.goals/`.

## Development process for this repository

Design changes land here first. Maintainers file issues and assign them to
the coding bot, which sends PRs. Maintainers review, request changes, and
merge. Maintainers do not send code PRs themselves, so that no merge is a
self-merge. An issue names the milestone, the packages it touches, the tests
it expects, and the acceptance check a reviewer will run.
