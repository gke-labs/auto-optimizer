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

package run

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	agentsclientset "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned"
	informers "sigs.k8s.io/agent-sandbox/clients/k8s/informers/externalversions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets/ao"
)

const (
	// LabelRun is the Sandbox label key containing the run ID.
	LabelRun = "auto-optimizer.labs.gke.io/run"
	// AnnotationRepo is the Sandbox annotation key containing the repository key.
	AnnotationRepo = "auto-optimizer.labs.gke.io/repo"

	// DefaultReadyTimeout is the default duration to wait for a Sandbox to become ready.
	DefaultReadyTimeout = 3 * time.Minute

	// DefaultHeroRunStatusInterval is the default period between HeroRun status flushes.
	DefaultHeroRunStatusInterval = 3 * time.Second

	// DefaultVerifyTimeout is the default duration to wait for a goal's verify command to complete.
	DefaultVerifyTimeout = 10 * time.Minute

	// readySafetyRequeueInterval is the slow safety requeue interval when waiting for a Sandbox to become ready.
	readySafetyRequeueInterval = 1 * time.Minute
)

// Request identifies a run to be reconciled.
type Request struct {
	Repo  state.RepoKey
	RunID string
}

// String returns a human-readable identifier for the request.
func (r Request) String() string {
	return fmt.Sprintf("%s/%s", r.Repo, r.RunID)
}

// OrchestratorOptions configures the run Orchestrator.
type OrchestratorOptions struct {
	Store             state.Store
	SessionStore      session.Store
	Harness           *session.Harness
	Clock             func() time.Time
	MaxTurns          int
	MaxConcurrentRuns int

	// GitAuthor is the identity configured as git user.name and user.email in
	// each run's sandbox, as `Name <email>`.
	GitAuthor string

	// PublicURL is the public base URL of this server, used for links in pull request bodies.
	PublicURL string

	ReadyTimeout time.Duration

	// HeroRunStatusInterval is the interval at which dirty HeroRun status records are flushed.
	// When unset or <= 0, defaults to DefaultHeroRunStatusInterval (3 seconds).
	HeroRunStatusInterval time.Duration

	// KubeClient is the controller-runtime client used to publish HeroRun custom resources.
	// When nil, as in most unit tests, publishing is skipped and logged once at startup.
	KubeClient client.Client

	// VerifyTimeout is the maximum duration for running a goal's verify command.
	VerifyTimeout time.Duration

	// ForgeFor returns the Forge for a repository URL.
	// If nil or returns an error, push operations in Selecting fail.
	ForgeFor func(repoURL string) (forge.Forge, error)
}

// Orchestrator reconciles runs in phase Pending and Working using a controller-runtime controller.
type Orchestrator struct {
	store             state.Store
	sessionStore      session.Store
	harness           *session.Harness
	clock             func() time.Time
	maxTurns          int
	maxConcurrentRuns int
	gitAuthor         string
	publicURL         string
	readyTimeout      time.Duration
	kubeClient        client.Client
	logNoKubeClient   sync.Once
	statusPublisher   *heroRunStatusPublisher
	verifyTimeout     time.Duration
	forgeFor          func(repoURL string) (forge.Forge, error)

	notifyCh chan event.TypedGenericEvent[Request]

	activeRunsMu sync.Mutex
	activeRuns   map[string]bool
}

var _ reconcile.TypedReconciler[Request] = (*Orchestrator)(nil)
var _ manager.Runnable = (*Orchestrator)(nil)

// NewScheme builds a runtime.Scheme containing client-go types and the HeroRun types.
func NewScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("adding client-go to scheme: %w", err)
	}
	if err := herorunv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("adding herorun v1alpha1 to scheme: %w", err)
	}
	return scheme, nil
}

// NewManager builds a controller-runtime manager from rest.Config with metrics and health
// endpoints disabled and leader election disabled.
func NewManager(restConfig *rest.Config) (manager.Manager, error) {
	scheme, err := NewScheme()
	if err != nil {
		return nil, err
	}
	return ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Logger: klog.NewKlogr(),
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	})
}

// NewOrchestrator creates a new run Orchestrator.
func NewOrchestrator(opts OrchestratorOptions) (*Orchestrator, error) {
	if opts.Store == nil {
		return nil, errors.New("state store is required")
	}
	if opts.Harness == nil {
		return nil, errors.New("harness is required")
	}

	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}

	maxTurns := opts.MaxTurns
	if maxTurns <= 0 {
		maxTurns = 30
	}

	maxConcurrentRuns := opts.MaxConcurrentRuns
	if maxConcurrentRuns <= 0 {
		maxConcurrentRuns = 4
	}

	readyTimeout := opts.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = DefaultReadyTimeout
	}

	interval := opts.HeroRunStatusInterval
	if interval <= 0 {
		interval = DefaultHeroRunStatusInterval
	}
	namespace := ""
	if opts.Harness != nil {
		namespace = opts.Harness.Namespace()
	}

	verifyTimeout := opts.VerifyTimeout
	if verifyTimeout <= 0 {
		verifyTimeout = DefaultVerifyTimeout
	}

	orch := &Orchestrator{
		store:             opts.Store,
		sessionStore:      opts.SessionStore,
		harness:           opts.Harness,
		clock:             clock,
		maxTurns:          maxTurns,
		maxConcurrentRuns: maxConcurrentRuns,
		gitAuthor:         opts.GitAuthor,
		publicURL:         strings.TrimRight(opts.PublicURL, "/"),
		readyTimeout:      readyTimeout,
		kubeClient:        opts.KubeClient,
		verifyTimeout:     verifyTimeout,
		forgeFor:          opts.ForgeFor,
		notifyCh:          make(chan event.TypedGenericEvent[Request], 1024),
		activeRuns:        make(map[string]bool),
	}
	orch.statusPublisher = newHeroRunStatusPublisher(opts.KubeClient, opts.Store, clock, interval, namespace, orch.publishHeroRun)
	return orch, nil
}

// RunPageURL returns the public URL for a run's detail page, or empty when unset.
func (o *Orchestrator) RunPageURL(repo *state.Repo, run *state.Run) string {
	if o == nil || o.publicURL == "" || run == nil || run.ID == "" {
		return ""
	}
	return fmt.Sprintf("%s/runs/%s", o.publicURL, run.ID)
}

// Notify enqueues a run for reconciliation.
// If o is nil, Notify is a no-op.
func (o *Orchestrator) Notify(repo state.RepoKey, runID string) {
	if o == nil {
		return
	}
	req := Request{Repo: repo, RunID: runID}
	select {
	case o.notifyCh <- event.TypedGenericEvent[Request]{Object: req}:
	default:
		go func() {
			o.notifyCh <- event.TypedGenericEvent[Request]{Object: req}
		}()
	}
}

// SetupWithManager registers the orchestrator with the controller-runtime manager.
func (o *Orchestrator) SetupWithManager(mgr manager.Manager) error {
	// Skip name validation so unit tests can build multiple controllers named "run" in one process.
	skipNameValidation := true
	c, err := controller.NewTyped[Request]("run", mgr, controller.TypedOptions[Request]{
		Reconciler:              o,
		MaxConcurrentReconciles: o.maxConcurrentRuns,
		SkipNameValidation:      &skipNameValidation,
	})
	if err != nil {
		return fmt.Errorf("creating run controller: %w", err)
	}

	src := source.TypedChannel[Request, Request](
		o.notifyCh,
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, req Request) []Request {
			return []Request{req}
		}),
	)
	if err := c.Watch(src); err != nil {
		return fmt.Errorf("watching notification channel: %w", err)
	}

	if o.harness != nil && o.harness.SandboxClient() != nil && o.harness.SandboxClient().AgentsClient() != nil {
		sbSrc := &sandboxSource{
			orch:         o,
			agentsClient: o.harness.SandboxClient().AgentsClient(),
			namespace:    o.harness.Namespace(),
		}
		if err := c.Watch(sbSrc); err != nil {
			return fmt.Errorf("watching sandbox resources: %w", err)
		}
	}

	return mgr.Add(o)
}

// requestsForSandbox maps a Sandbox to reconciliation requests for the run it belongs to.
// It follows the controller owner reference to the HeroRun and reads the run ID and repository
// from the HeroRun. If the Sandbox has no HeroRun owner, or KubeClient is nil, it falls back
// to reading the run ID and repository from the Sandbox's own label and annotation.
func (o *Orchestrator) requestsForSandbox(ctx context.Context, sb *sandboxv1beta1.Sandbox) []Request {
	if sb == nil {
		return nil
	}

	owner := metav1.GetControllerOf(sb)
	if owner != nil && owner.Kind == "HeroRun" && o.kubeClient != nil {
		namespace := sb.Namespace
		if namespace == "" && o.harness != nil {
			namespace = o.harness.Namespace()
		}
		var hr herorunv1alpha1.HeroRun
		if err := o.kubeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: owner.Name}, &hr); err != nil {
			scoped.Log(ctx).V(2).Info("failed to get HeroRun for sandbox", "sandbox", sb.Name, "heroRun", owner.Name, "error", err)
			return nil
		}
		return requestFromLabelsAndAnnotations(ctx, hr.Labels, hr.Annotations, "HeroRun")
	}

	return requestFromLabelsAndAnnotations(ctx, sb.Labels, sb.Annotations, "sandbox")
}

func requestFromLabelsAndAnnotations(ctx context.Context, labels, annotations map[string]string, source string) []Request {
	runID := labels[LabelRun]
	if runID == "" {
		return nil
	}
	repoStr := annotations[AnnotationRepo]
	if repoStr == "" {
		return nil
	}
	key, err := state.ParseRepoKey(repoStr)
	if err != nil {
		scoped.Log(ctx).V(2).Info("failed to parse repo key from "+source+" annotation", "repo", repoStr, "error", err)
		return nil
	}
	return []Request{{Repo: key, RunID: runID}}
}

// sandboxSource is a hand-written source.TypedSource[Request] that watches
// Sandboxes in the control-plane namespace, maps the run label and repo
// annotation to a Request, and enqueues it for reconciliation.
//
// A conventional controller-runtime setup uses source.Kind with an
// EnqueueRequestForOwner or EnqueueRequestsFromMapFunc handler, but that
// does not fit here for two reasons:
//
//  1. The reconciler's request type is run.Request{Repo, RunID}, not
//     reconcile.Request. A run is a JSON record in the state directory, not a
//     Kubernetes object, so a Sandbox cannot carry an owner reference to it
//     and EnqueueRequestForOwner has nothing to enqueue.
//  2. The sandbox watch uses the agent-sandbox typed clientset and a
//     namespace-scoped shared informer rather than the manager's cache,
//     because the manager's scheme does not include the Sandbox CRD types
//     and unit tests supply an in-memory fake clientset.
type sandboxSource struct {
	orch         *Orchestrator
	agentsClient agentsclientset.Interface
	namespace    string
}

var _ source.TypedSource[Request] = (*sandboxSource)(nil)

func (s *sandboxSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[Request]) error {
	factory := informers.NewSharedInformerFactoryWithOptions(s.agentsClient, 0, informers.WithNamespace(s.namespace))
	informer := factory.Agents().V1beta1().Sandboxes().Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			s.enqueueSandbox(ctx, obj, queue)
		},
		UpdateFunc: func(oldObj, newObj any) {
			s.enqueueSandbox(ctx, newObj, queue)
		},
		DeleteFunc: func(obj any) {
			s.enqueueSandbox(ctx, obj, queue)
		},
	})
	if err != nil {
		return fmt.Errorf("adding event handler to sandbox informer: %w", err)
	}

	factory.StartWithContext(ctx)

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("timed out waiting for sandbox informer caches to sync")
	}

	return nil
}

func (s *sandboxSource) enqueueSandbox(ctx context.Context, obj any, queue workqueue.TypedRateLimitingInterface[Request]) {
	sb, ok := obj.(*sandboxv1beta1.Sandbox)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		sb, ok = tombstone.Obj.(*sandboxv1beta1.Sandbox)
		if !ok || sb == nil {
			return
		}
	}

	if s.orch == nil {
		return
	}
	for _, req := range s.orch.requestsForSandbox(ctx, sb) {
		queue.Add(req)
	}
}

// Start implements manager.Runnable. On manager startup it calls Resume,
// which enqueues all non-terminal runs. It then blocks until ctx is cancelled.
func (o *Orchestrator) Start(ctx context.Context) error {
	if o.kubeClient == nil {
		o.logNoKubeClient.Do(func() {
			scoped.Log(ctx).Info("HeroRun publishing skipped: no Kubernetes client configured")
		})
	}
	if err := o.Resume(ctx); err != nil {
		scoped.Log(ctx).Error(err, "failed to resume interrupted runs on startup")
	}
	if o.statusPublisher != nil {
		go func() {
			if err := o.statusPublisher.Run(ctx); err != nil {
				scoped.Log(ctx).Error(err, "heroRunStatusPublisher stopped with error")
			}
		}()
	}
	<-ctx.Done()
	return nil
}

// Resume inspects existing runs across all repositories on startup.
// Every non-terminal run is enqueued for reconciliation.
func (o *Orchestrator) Resume(ctx context.Context) error {
	log := scoped.Log(ctx)
	repos, err := o.store.ListRepos(ctx)
	if err != nil {
		return fmt.Errorf("listing repositories for resume: %w", err)
	}

	for _, repo := range repos {
		runs, err := o.store.ListRuns(ctx, repo.Key())
		if err != nil {
			log.Error(err, "failed to list runs during resume", "repo", repo.URL)
			continue
		}

		for _, r := range runs {
			if o.statusPublisher != nil {
				o.statusPublisher.MarkDirty(repo.Key(), r.ID)
			}
			if !r.Phase.IsTerminal() {
				if r.Phase != state.PhasePending {
					o.publishHeroRun(ctx, repo, r)
				}
				o.Notify(repo.Key(), r.ID)
			}
		}
	}
	return nil
}

// Reconcile loads the run and the repository, dispatches on the phase to
// prepareRun, reconcilePreparing, or workRun, and returns.
func (o *Orchestrator) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
	log := scoped.Log(ctx)

	run, err := o.store.GetRun(ctx, req.Repo, req.RunID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting run %s: %w", req.RunID, err)
	}

	repo, err := o.store.GetRepo(ctx, req.Repo)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting repo %s: %w", req.Repo, err)
	}

	switch run.Phase {
	case state.PhasePending:
		if err := o.prepareRun(ctx, repo, run); err != nil {
			log.Error(err, "preparing run failed", "repo", repo.URL, "run", run.ID)
			// prepareRun transitions to PhaseFailed on error; do not requeue terminal runs.
			return reconcile.Result{}, nil
		}
		requeueAfter := readySafetyRequeueInterval
		if run.ReadyDeadline != nil {
			rem := run.ReadyDeadline.Sub(o.clock().UTC())
			if rem < requeueAfter {
				requeueAfter = rem
			}
		}
		if requeueAfter <= 0 {
			requeueAfter = time.Millisecond
		}
		return reconcile.Result{RequeueAfter: requeueAfter}, nil

	case state.PhasePreparing:
		res, err := o.reconcilePreparing(ctx, repo, run)
		if err != nil {
			log.Error(err, "reconciling preparing run failed", "repo", repo.URL, "run", run.ID)
			return reconcile.Result{}, nil
		}
		return res, nil

	case state.PhaseWorking:
		if o.sessionStore != nil && o.harness != nil && o.harness.LLMClient() != nil {
			if err := o.workRun(ctx, repo, run); err != nil {
				log.Error(err, "working run failed", "repo", repo.URL, "run", run.ID)
				// workRun transitions to PhaseFailed on error; do not requeue terminal runs.
				return reconcile.Result{}, nil
			}
		}

	case state.PhaseSelecting:
		res, err := o.reconcileSelecting(ctx, repo, run)
		if err != nil {
			log.Error(err, "reconciling selecting run failed", "repo", repo.URL, "run", run.ID)
			return reconcile.Result{}, nil
		}
		return res, nil

	case state.PhaseOpeningPR:
		if err := o.openPullRequest(ctx, repo, run); err != nil {
			log.Error(err, "opening pull request failed", "repo", repo.URL, "run", run.ID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, nil
	}

	return reconcile.Result{}, nil
}

// prepareRun executes the first step of Preparing: publishes the HeroRun,
// creates the Sandbox with an owner reference to the HeroRun (or unowned if
// HeroRun is unavailable), records SandboxName and ReadyDeadline, moves the
// run to Preparing, appends a phase event, and returns.
func (o *Orchestrator) prepareRun(ctx context.Context, repo *state.Repo, run *state.Run) error {
	log := scoped.Log(ctx)
	now := o.clock().UTC()

	// 1. Assign sandbox name if not already assigned
	sandboxName := run.SandboxName
	if sandboxName == "" {
		sandboxName = sandboxNameForRun(run.ID)
		run.SandboxName = sandboxName
	}

	// 2. Set deadline on the run
	deadline := now.Add(o.readyTimeout)
	run.ReadyDeadline = &deadline

	log.Info("preparing run: creating sandbox", "repo", repo.URL, "run", run.ID, "sandbox", sandboxName)

	// 3. Publish HeroRun before creating the sandbox so the sandbox can reference it as owner
	heroRun := o.publishHeroRun(ctx, repo, run)

	var ownerRefs []metav1.OwnerReference
	if heroRun != nil {
		isController := true
		ownerRefs = []metav1.OwnerReference{
			{
				APIVersion: herorunv1alpha1.GroupVersion.String(),
				Kind:       "HeroRun",
				Name:       heroRun.Name,
				UID:        heroRun.UID,
				Controller: &isController,
			},
		}
	} else {
		log.Info("creating unowned sandbox: no HeroRun available", "run", run.ID, "sandbox", sandboxName)
	}

	// 4. Create sandbox with a label naming the run, annotation naming the repository, and owner reference
	labels := map[string]string{
		LabelRun: run.ID,
	}
	annotations := map[string]string{
		AnnotationRepo: repo.Key().String(),
	}
	if _, err := o.harness.CreateSandboxNamedWithOwnerReferences(ctx, sandboxName, labels, annotations, ownerRefs); err != nil {
		prev := run.Phase
		failTime := o.clock().UTC()
		run.SetPhase(state.PhaseFailed, failTime)
		run.Error = fmt.Sprintf("creating sandbox: %v", err)
		run.ReadyDeadline = nil
		if putErr := o.putRun(ctx, repo, run, prev); putErr != nil {
			log.Error(putErr, "failed to update run to Failed", "run", run.ID)
		}

		if appendErr := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    failTime,
			Phase:   state.PhaseFailed,
			Kind:    "phase",
			Message: fmt.Sprintf("Run %s failed: creating sandbox: %v", run.ID, err),
			Error:   run.Error,
		}); appendErr != nil {
			log.Error(appendErr, "failed to append Failed phase event", "run", run.ID)
		}

		return fmt.Errorf("creating sandbox: %w", err)
	}

	// 5. Transition to Preparing
	prev := run.Phase
	run.SetPhase(state.PhasePreparing, now)
	if err := o.putRun(ctx, repo, run, prev); err != nil {
		return fmt.Errorf("updating run to Preparing: %w", err)
	}

	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    now,
		Phase:   state.PhasePreparing,
		Kind:    "phase",
		Message: fmt.Sprintf("Run %s entered Preparing phase", run.ID),
	}); err != nil {
		log.Error(err, "failed to append Preparing phase event", "run", run.ID)
	}

	return nil
}

// sandboxNameForRun returns the canonical name for a run's Sandbox and HeroRun objects.
func sandboxNameForRun(runID string) string {
	return fmt.Sprintf("run-%s", strings.ToLower(runID))
}

// publishHeroRun publishes a thin HeroRun custom resource for run in the sandbox namespace.
// It creates the object if it does not already exist; if it exists, it leaves it alone.
// It returns the published or existing object, or nil if there is no KubeClient or publishing failed.
// A publish failure never fails the run; it is logged and the next reconcile or restart retries.
func (o *Orchestrator) publishHeroRun(ctx context.Context, repo *state.Repo, run *state.Run) *herorunv1alpha1.HeroRun {
	if o.kubeClient == nil {
		o.logNoKubeClient.Do(func() {
			scoped.Log(ctx).Info("HeroRun publishing skipped: no Kubernetes client configured")
		})
		return nil
	}
	if repo == nil || run == nil {
		return nil
	}

	name := sandboxNameForRun(run.ID)
	namespace := ""
	if o.harness != nil {
		namespace = o.harness.Namespace()
	}

	log := scoped.Log(ctx)

	var existing herorunv1alpha1.HeroRun
	err := o.kubeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &existing)
	if err == nil {
		// Already exists; leave it alone (status handling is part 4).
		return &existing
	}
	if !k8serrors.IsNotFound(err) {
		log.Error(err, "failed to get HeroRun", "run", run.ID, "heroRun", name)
		return nil
	}

	goal := run.GoalName
	if goal == "" && run.Goal != nil {
		goal = run.Goal.Name
	}

	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				LabelRun: run.ID,
			},
			Annotations: map[string]string{
				AnnotationRepo: repo.Key().String(),
			},
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: repo.URL,
			Goal:       goal,
			RunID:      run.ID,
			BaseCommit: run.BaseCommit,
		},
	}

	if err := o.kubeClient.Create(ctx, hr); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			log.Error(err, "failed to create HeroRun", "run", run.ID, "heroRun", name)
			return nil
		}
		if err := o.kubeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &existing); err != nil {
			log.Error(err, "failed to get existing HeroRun after already-exists", "run", run.ID, "heroRun", name)
			return nil
		}
		return &existing
	}
	return hr
}

// isSandboxFailed inspects sandbox status conditions to detect failure before readiness.
func isSandboxFailed(sb *sandboxv1beta1.Sandbox) (string, bool) {
	for _, cond := range sb.Status.Conditions {
		if cond.Type == string(sandboxv1beta1.SandboxConditionFinished) && cond.Status == metav1.ConditionTrue {
			if cond.Reason == sandboxv1beta1.SandboxReasonPodFailed {
				msg := cond.Message
				if msg == "" {
					msg = fmt.Sprintf("pod finished (%s)", cond.Reason)
				}
				return msg, true
			}
		}
		if cond.Type == string(sandboxv1beta1.SandboxConditionReady) && cond.Status == metav1.ConditionFalse {
			switch cond.Reason {
			case sandboxv1beta1.SandboxReasonInvalidConfiguration,
				sandboxv1beta1.SandboxReasonMultiplePods,
				sandboxv1beta1.SandboxReasonExpired,
				sandboxv1beta1.SandboxReasonPodFailed:
				msg := cond.Message
				if msg == "" {
					msg = fmt.Sprintf("sandbox readiness failed (%s)", cond.Reason)
				}
				return msg, true
			}
		}
	}
	return "", false
}

// reconcilePreparing executes the second step of Preparing: reads the Sandbox,
// checks failure conditions / deletion / deadline expiration, clones the repository,
// moves the run to Working, and enqueues it.
func (o *Orchestrator) reconcilePreparing(ctx context.Context, repo *state.Repo, run *state.Run) (reconcile.Result, error) {
	log := scoped.Log(ctx)
	now := o.clock().UTC()

	// 1. Fetch Sandbox resource
	if o.harness == nil || o.harness.SandboxClient() == nil {
		return reconcile.Result{}, errors.New("harness sandbox client is nil")
	}

	sbObj, err := o.harness.SandboxClient().GetSandboxResource(ctx, o.harness.Namespace(), run.SandboxName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			run.ReadyDeadline = nil
			return reconcile.Result{}, o.failRunWithEvent(ctx, repo, run, fmt.Sprintf("sandbox %s was deleted before becoming ready", run.SandboxName))
		}
		return reconcile.Result{}, fmt.Errorf("getting sandbox %s: %w", run.SandboxName, err)
	}

	// 2. Check for reported failure on Sandbox
	if failMsg, failed := isSandboxFailed(sbObj); failed {
		if delErr := o.harness.SandboxClient().DeleteSandboxByName(ctx, o.harness.Namespace(), run.SandboxName); delErr != nil {
			log.V(2).Info("error deleting failed sandbox", "sandbox", run.SandboxName, "error", delErr)
		}
		run.ReadyDeadline = nil
		return reconcile.Result{}, o.failRunWithEvent(ctx, repo, run, fmt.Sprintf("sandbox %s failed: %s", run.SandboxName, failMsg))
	}

	sb := o.harness.GetOrAttachSandbox(run.SandboxName)
	if sb != nil {
		if pullErr := sb.CheckPodImagePullStatus(ctx); pullErr != nil {
			if delErr := o.harness.SandboxClient().DeleteSandboxByName(ctx, o.harness.Namespace(), run.SandboxName); delErr != nil {
				log.V(2).Info("error deleting failed sandbox", "sandbox", run.SandboxName, "error", delErr)
			}
			run.ReadyDeadline = nil
			return reconcile.Result{}, o.failRunWithEvent(ctx, repo, run, fmt.Sprintf("sandbox %s pod image pull failed: %v", run.SandboxName, pullErr))
		}
	}

	// 3. Check if Sandbox is Ready
	var ready bool
	for _, cond := range sbObj.Status.Conditions {
		if cond.Type == string(sandboxv1beta1.SandboxConditionReady) && cond.Status == metav1.ConditionTrue {
			ready = true
			break
		}
	}

	if !ready {
		// If deadline has passed and it is not Ready, fail the run.
		if run.ReadyDeadline != nil && !now.Before(*run.ReadyDeadline) {
			if o.harness != nil && o.harness.SandboxClient() != nil && run.SandboxName != "" {
				if delErr := o.harness.SandboxClient().DeleteSandboxByName(ctx, o.harness.Namespace(), run.SandboxName); delErr != nil {
					log.V(2).Info("error deleting unready sandbox after timeout", "sandbox", run.SandboxName, "error", delErr)
				}
			}
			run.ReadyDeadline = nil
			return reconcile.Result{}, o.failRunWithEvent(ctx, repo, run, "sandbox not ready before deadline")
		}

		// Otherwise return RequeueAfter with a slow safety interval capped at the deadline.
		requeueAfter := readySafetyRequeueInterval
		if run.ReadyDeadline != nil {
			rem := run.ReadyDeadline.Sub(now)
			if rem < requeueAfter {
				requeueAfter = rem
			}
		}
		if requeueAfter <= 0 {
			requeueAfter = time.Millisecond
		}
		return reconcile.Result{RequeueAfter: requeueAfter}, nil
	}

	// 4. Sandbox is Ready! Attach, run clone and checkout, move to Working, and return.
	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    o.clock().UTC(),
		Phase:   state.PhasePreparing,
		Kind:    "sandbox_ready",
		Message: fmt.Sprintf("Sandbox %s is ready", run.SandboxName),
		Data:    map[string]any{"sandbox": run.SandboxName},
	}); err != nil {
		log.Error(err, "failed to append sandbox_ready event", "run", run.ID)
	}

	if sb == nil {
		sb = o.harness.GetOrAttachSandbox(run.SandboxName)
	}
	if sb == nil {
		run.ReadyDeadline = nil
		return reconcile.Result{}, o.failRunWithEvent(ctx, repo, run, "failed to get sandbox handle")
	}

	podIP := ""
	if len(sbObj.Status.PodIPs) > 0 {
		podIP = sbObj.Status.PodIPs[0]
	}
	sb.SetPodInfo(sbObj.Name, podIP)

	commands := []struct {
		desc string
		args []string
	}{
		{
			desc: fmt.Sprintf("git clone -- %s /workspace/repo", repo.URL),
			args: []string{"git", "clone", "--", repo.URL, "/workspace/repo"},
		},
		{
			desc: fmt.Sprintf("git -C /workspace/repo checkout %s", run.BaseCommit),
			args: []string{"git", "-C", "/workspace/repo", "checkout", run.BaseCommit},
		},
	}

	for _, cmd := range commands {
		log.Info("executing command inside sandbox", "run", run.ID, "cmd", cmd.desc)

		res, execErr := sb.ExecCommand(ctx, tools.ExecCommandOptions{
			Command: cmd.args,
		})

		cmdTime := o.clock().UTC()
		var cmdFailed bool
		var cmdErrMsg string

		if execErr != nil {
			cmdFailed = true
			cmdErrMsg = fmt.Sprintf("command error: %v", execErr)
		} else if res != nil && res.ExitCode != 0 {
			cmdFailed = true
			stderr := strings.TrimSpace(res.Stderr)
			if stderr != "" {
				cmdErrMsg = fmt.Sprintf("command failed with exit code %d: %s", res.ExitCode, stderr)
			} else {
				cmdErrMsg = fmt.Sprintf("command failed with exit code %d", res.ExitCode)
			}
		} else if res == nil {
			cmdFailed = true
			cmdErrMsg = "command returned nil result"
		}

		if cmdFailed {
			log.Error(errors.New(cmdErrMsg), "command failed; deleting sandbox", "run", run.ID, "sandbox", run.SandboxName)

			var exitCode int
			var stdout, stderr string
			if res != nil {
				exitCode = res.ExitCode
				stdout = res.Stdout
				stderr = res.Stderr
			}

			// Record command failure event
			if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
				Time:    cmdTime,
				Phase:   state.PhasePreparing,
				Kind:    "command",
				Message: cmdErrMsg,
				Error:   cmdErrMsg,
				Data: map[string]any{
					"command":   cmd.desc,
					"args":      cmd.args,
					"exit_code": exitCode,
					"stdout":    stdout,
					"stderr":    stderr,
				},
			}); err != nil {
				log.Error(err, "failed to append command failure event", "run", run.ID)
			}

			// Delete sandbox
			if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
				log.Error(delErr, "failed to delete sandbox after command failure", "sandbox", run.SandboxName)
			}

			run.ReadyDeadline = nil
			return reconcile.Result{}, o.failRunWithEvent(ctx, repo, run, cmdErrMsg)
		}

		// Command succeeded! Record command result event
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    cmdTime,
			Phase:   state.PhasePreparing,
			Kind:    "command",
			Message: fmt.Sprintf("Executed %s", cmd.desc),
			Data: map[string]any{
				"command":   cmd.desc,
				"args":      cmd.args,
				"exit_code": 0,
				"stdout":    res.Stdout,
				"stderr":    res.Stderr,
			},
		}); err != nil {
			log.Error(err, "failed to append command success event", "run", run.ID)
		}
	}

	if o.gitAuthor != "" {
		if err := sb.ConfigureGitIdentity(ctx, "/workspace/repo", o.gitAuthor); err != nil {
			log.Error(err, "failed to configure git identity; deleting sandbox", "run", run.ID, "sandbox", run.SandboxName)

			// Delete sandbox
			if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
				log.Error(delErr, "failed to delete sandbox after command failure", "sandbox", run.SandboxName)
			}

			run.ReadyDeadline = nil
			return reconcile.Result{}, o.failRunWithEvent(ctx, repo, run, err.Error())
		}

		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    o.clock().UTC(),
			Phase:   state.PhasePreparing,
			Kind:    "git_identity_configured",
			Message: fmt.Sprintf("git identity configured: %s", o.gitAuthor),
			Data:    map[string]any{"author": o.gitAuthor},
		}); err != nil {
			log.Error(err, "failed to append git_identity_configured event", "run", run.ID)
		}
	}

	// Move to Working phase
	prev := run.Phase
	workTime := o.clock().UTC()
	run.SetPhase(state.PhaseWorking, workTime)
	run.ReadyDeadline = nil
	if err := o.putRun(ctx, repo, run, prev); err != nil {
		return reconcile.Result{}, fmt.Errorf("updating run to Working: %w", err)
	}

	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    workTime,
		Phase:   state.PhaseWorking,
		Kind:    "phase",
		Message: fmt.Sprintf("Run %s entered Working phase", run.ID),
	}); err != nil {
		log.Error(err, "failed to append Working phase event", "run", run.ID)
	}

	log.Info("run successfully prepared and entered Working phase", "run", run.ID, "sandbox", run.SandboxName)

	o.Notify(repo.Key(), run.ID)
	return reconcile.Result{}, nil
}

func (o *Orchestrator) setRunActive(runID string, active bool) bool {
	o.activeRunsMu.Lock()
	defer o.activeRunsMu.Unlock()
	if active {
		if o.activeRuns[runID] {
			return false
		}
		o.activeRuns[runID] = true
		return true
	}
	delete(o.activeRuns, runID)
	return true
}

// putRun writes a run record to the state store and updates HeroRun status:
// critical transitions are published synchronously via PublishNow, while
// progress updates are marked dirty and coalesced by the publisher loop.
func (o *Orchestrator) putRun(ctx context.Context, repo *state.Repo, run *state.Run, prev state.RunPhase) error {
	if err := o.store.PutRun(ctx, run); err != nil {
		return err
	}
	if o.statusPublisher != nil {
		var key state.RepoKey
		if repo != nil {
			key = repo.Key()
		} else if run != nil {
			key = run.Repo
		}
		if statusTier(prev, run.Phase) == tierCritical {
			if err := o.statusPublisher.PublishNow(ctx, repo, run); err != nil {
				scoped.Log(ctx).V(2).Info("synchronous HeroRun status publish failed, deferred to next tick", "run", run.ID, "error", err)
			}
		} else {
			o.statusPublisher.MarkDirty(key, run.ID)
		}
	}
	return nil
}

func (o *Orchestrator) finishRun(ctx context.Context, repo *state.Repo, run *state.Run, sb *session.Sandbox, phase state.RunPhase, kind, message string, data map[string]any) {
	if ctx.Err() != nil {
		return
	}

	prev := run.Phase
	endTime := o.clock().UTC()
	run.SetPhase(phase, endTime)
	if phase == state.PhaseFailed {
		run.Error = message
	}
	if err := o.putRun(ctx, repo, run, prev); err != nil {
		scoped.Log(ctx).Error(err, "failed to update run phase", "phase", phase, "run", run.ID)
	}

	msg := message
	if phase == state.PhaseFailed && kind == "failed" {
		msg = fmt.Sprintf("Run failed: %s", message)
	}
	event := state.Event{
		Time:    endTime,
		Phase:   phase,
		Kind:    kind,
		Message: msg,
		Data:    data,
	}
	if phase == state.PhaseFailed {
		event.Error = message
	}
	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, event); err != nil {
		scoped.Log(ctx).Error(err, "failed to append event", "kind", kind, "run", run.ID)
	}

	if sb != nil {
		if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
			scoped.Log(ctx).V(2).Info("error deleting sandbox after run ended", "sandbox", run.SandboxName, "error", delErr)
		}
	}
}

func (o *Orchestrator) failRunWithEvent(ctx context.Context, repo *state.Repo, run *state.Run, errMsg string) error {
	o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "failed", errMsg, nil)
	return errors.New(errMsg)
}

func (o *Orchestrator) workRun(ctx context.Context, repo *state.Repo, run *state.Run) error {
	log := scoped.Log(ctx)

	if ctx.Err() != nil {
		return nil
	}

	if !o.setRunActive(run.ID, true) {
		log.V(2).Info("run is already actively executing a session", "run", run.ID)
		return nil
	}
	defer o.setRunActive(run.ID, false)

	sandboxName := run.SandboxName
	if sandboxName == "" {
		return o.failRunWithEvent(ctx, repo, run, "sandbox name is empty for working run")
	}

	sb := o.harness.GetOrAttachSandbox(sandboxName)
	if sb == nil {
		return o.failRunWithEvent(ctx, repo, run, "failed to get sandbox handle")
	}

	// Verify sandbox exists in Kubernetes and has not expired.
	// If expired or deleted, fail the run with a clear event.
	if err := sb.CheckAliveAndExtendLifecycle(ctx, o.clock().UTC(), session.SandboxInactivityTimeout); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return o.failRunWithEvent(ctx, repo, run, fmt.Sprintf("sandbox %s expired or deleted: %v", sandboxName, err))
	}

	// Assign session ID if not already assigned and persist in run.json
	sessionID := run.SessionID
	if sessionID == "" {
		sessionID = generateSessionID(run.ID)
		run.SessionID = sessionID
		if err := o.putRun(ctx, repo, run, run.Phase); err != nil {
			return fmt.Errorf("updating run with session ID: %w", err)
		}
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    o.clock().UTC(),
			Phase:   state.PhaseWorking,
			Kind:    "session_started",
			Message: fmt.Sprintf("Started session %s", sessionID),
			Data:    map[string]any{"session_id": sessionID},
		}); err != nil {
			log.Error(err, "failed to append session_started event", "run", run.ID)
		}
	}

	goalBody := ""
	if run.Goal != nil {
		goalBody = run.Goal.Body
	} else if run.GoalName != "" {
		goalRec, err := o.store.GetGoal(ctx, repo.Key(), run.GoalName)
		if err == nil && goalRec != nil && goalRec.Goal != nil {
			goalBody = goalRec.Goal.Body
		}
	}

	sess, err := o.harness.LoadSession(ctx, o.sessionStore, sessionID)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			sess, err = o.harness.NewSession(ctx, o.sessionStore, sessionID, session.Metadata{
				CreatedAt: o.clock().UTC(),
			})
			if err != nil {
				return fmt.Errorf("creating session %s: %w", sessionID, err)
			}
		} else {
			return fmt.Errorf("loading session %s: %w", sessionID, err)
		}
	}

	// Bind sandbox to session
	sess.SetActiveSandbox(sb)
	sess.FailOnSandboxExpiry = true

	// Seed system prompt if new session
	if len(sess.Messages()) == 0 {
		sysPrompt := buildSystemPrompt(o.harness.RunOptions().SystemPrompt, repo, run, goalBody)
		if err := sess.AddMessages(ctx, llm.Message{
			Role:    "system",
			Content: &sysPrompt,
		}); err != nil {
			return fmt.Errorf("seeding system prompt: %w", err)
		}
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var heartbeatErr error
	var heartbeatMu sync.Mutex

	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)

	go func() {
		ticker := time.NewTicker(session.SandboxInactivityTimeout / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-sessionCtx.Done():
				return
			case <-ticker.C:
				if err := sb.CheckAliveAndExtendLifecycle(sessionCtx, o.clock().UTC(), session.SandboxInactivityTimeout); err != nil {
					heartbeatMu.Lock()
					heartbeatErr = err
					heartbeatMu.Unlock()
					cancel()
					return
				}
			}
		}
	}()

	events := &orchestratorTurnEvents{}
	finishTool := ao.NewFinishTool()
	prTool := ao.NewOpenPullRequestTool(ao.OpenPullRequestExpectation{
		Repository: repo.URL,
		Branch:     run.BranchName(),
	})
	harness := o.harness.WithMaxTurns(o.maxTurns).WithExtraTools(finishTool, prTool)
	turnErr := harness.RunTurn(sessionCtx, sess, "Please work on the goal.", events)

	if errors.Is(turnErr, session.ErrPullRequestRequested) {
		run.PRTitle = prTool.Title
		run.PRBody = prTool.Body
		prev := run.Phase
		now := o.clock().UTC()
		run.SetPhase(state.PhaseSelecting, now)
		if err := o.putRun(ctx, repo, run, prev); err != nil {
			return fmt.Errorf("updating run to Selecting: %w", err)
		}
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    now,
			Phase:   state.PhaseSelecting,
			Kind:    "pull_request_requested",
			Message: "Pull request requested by agent",
		}); err != nil {
			log.Error(err, "failed to append pull_request_requested event", "run", run.ID)
		}
		o.Notify(repo.Key(), run.ID)
		return nil
	}

	if errors.Is(turnErr, session.ErrSessionFinished) {
		run.Summary = finishTool.Summary
		o.finishRun(ctx, repo, run, sb, state.PhaseNoImprovement, "finished", finishTool.Summary, nil)
		return nil
	}

	if turnErr == nil || errors.Is(turnErr, session.ErrMaxTurns) {
		o.finishRun(ctx, repo, run, sb, state.PhaseNoImprovement, "no_improvement", "session ended without a pull request", nil)
		return nil
	}

	if ctx.Err() != nil {
		return nil
	}

	heartbeatMu.Lock()
	hbErr := heartbeatErr
	heartbeatMu.Unlock()
	if errors.Is(turnErr, session.ErrSandboxExpired) || errors.Is(hbErr, session.ErrSandboxExpired) {
		return o.failRunWithEvent(ctx, repo, run, "sandbox expired or was deleted during session")
	}

	return o.failRunWithEvent(ctx, repo, run, fmt.Sprintf("session error: %v", turnErr))
}

func (o *Orchestrator) reconcileSelecting(ctx context.Context, repo *state.Repo, run *state.Run) (reconcile.Result, error) {
	log := scoped.Log(ctx)

	if o.harness == nil {
		return reconcile.Result{}, errors.New("harness is nil")
	}

	sandboxName := run.SandboxName
	if sandboxName == "" {
		o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "failed", "sandbox name is empty for selecting run", nil)
		return reconcile.Result{}, nil
	}

	sb := o.harness.GetOrAttachSandbox(sandboxName)
	if sb == nil {
		o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "failed", "failed to get sandbox handle", nil)
		return reconcile.Result{}, nil
	}

	if run.Goal != nil && run.Goal.Verify != "" {
		now := o.clock().UTC()
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    now,
			Phase:   state.PhaseSelecting,
			Kind:    "verify_started",
			Message: fmt.Sprintf("Running verify command: %s", run.Goal.Verify),
			Data:    map[string]any{"command": run.Goal.Verify},
		}); err != nil {
			log.Error(err, "failed to append verify_started event", "run", run.ID)
		}

		verifyCtx, verifyCancel := context.WithTimeout(ctx, o.verifyTimeout)
		res, err := sb.RunVerify(verifyCtx, "/workspace/repo", run.Goal.Verify, 50)
		timedOut := errors.Is(err, context.DeadlineExceeded) || verifyCtx.Err() == context.DeadlineExceeded || status.Code(err) == codes.DeadlineExceeded
		verifyCancel()

		if ctx.Err() != nil {
			return reconcile.Result{}, nil
		}

		if err != nil {
			if timedOut {
				o.finishRun(ctx, repo, run, sb, state.PhaseNoImprovement, "verify_failed", fmt.Sprintf("verify command timed out after %s", o.verifyTimeout), nil)
				return reconcile.Result{}, nil
			}
			o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "verify_error", fmt.Sprintf("running verify command: %v", err), nil)
			return reconcile.Result{}, nil
		}

		if res.ExitCode != 0 {
			msg := fmt.Sprintf("verify command failed with exit code %d", res.ExitCode)
			o.finishRun(ctx, repo, run, sb, state.PhaseNoImprovement, "verify_failed", msg, map[string]any{
				"exitCode": res.ExitCode,
				"tail":     res.Tail,
			})
			return reconcile.Result{}, nil
		}

		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    o.clock().UTC(),
			Phase:   state.PhaseSelecting,
			Kind:    "verify_passed",
			Message: "Verify command passed",
		}); err != nil {
			log.Error(err, "failed to append verify_passed event", "run", run.ID)
		}
	}

	destDir, err := os.MkdirTemp("", "ao-export-*")
	if err != nil {
		o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "failed", fmt.Sprintf("creating temp dir for export: %v", err), nil)
		return reconcile.Result{}, nil
	}
	defer func() {
		if rmErr := os.RemoveAll(destDir); rmErr != nil {
			log.V(2).Info("failed to remove temp export dir", "dir", destDir, "error", rmErr)
		}
	}()

	res, err := sb.ExportCommits(ctx, session.ExportOptions{
		RepoDir: "/workspace/repo",
		Base:    run.BaseCommit,
		DestDir: destDir,
	})
	if err != nil {
		if errors.Is(err, session.ErrNoChanges) {
			o.finishRun(ctx, repo, run, sb, state.PhaseNoImprovement, "no_changes", "no changes against base commit", nil)
			return reconcile.Result{}, nil
		}
		if errors.Is(err, session.ErrGoalsChanged) {
			o.finishRun(ctx, repo, run, sb, state.PhaseNoImprovement, "refused", "changes under .goals/", nil)
			return reconcile.Result{}, nil
		}
		if errors.Is(err, session.ErrUncommittedChanges) {
			o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "failed", err.Error(), nil)
			return reconcile.Result{}, nil
		}
		o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "failed", fmt.Sprintf("export commits: %v", err), nil)
		return reconcile.Result{}, nil
	}

	log.V(2).Info("commits exported successfully", "run", run.ID, "tip", res.Tip)
	return reconcile.Result{}, o.exportSuccess(ctx, repo, run, sb, res)
}

func (o *Orchestrator) exportSuccess(ctx context.Context, repo *state.Repo, run *state.Run, sb *session.Sandbox, res *session.ExportResult) error {
	if res.PackfilePath != "" {
		defer func() {
			if err := os.Remove(res.PackfilePath); err != nil && !os.IsNotExist(err) {
				scoped.Log(ctx).V(2).Info("failed to remove packfile", "path", res.PackfilePath, "error", err)
			}
		}()
	}

	now := o.clock().UTC()
	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    now,
		Phase:   state.PhaseSelecting,
		Kind:    "commits_exported",
		Message: fmt.Sprintf("Exported commits with tip %s", res.Tip),
		Data:    map[string]any{"tip": res.Tip},
	}); err != nil {
		scoped.Log(ctx).Error(err, "failed to append commits_exported event", "run", run.ID)
	}

	if o.forgeFor == nil {
		o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "push_failed", fmt.Sprintf("no forge configured for repository %s", repo.URL), nil)
		return nil
	}

	f, err := o.forgeFor(repo.URL)
	if err != nil {
		o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "push_failed", fmt.Sprintf("selecting forge for %s: %v", repo.URL, err), nil)
		return nil
	}
	if f == nil {
		o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "push_failed", fmt.Sprintf("no forge configured for repository %s", repo.URL), nil)
		return nil
	}

	packFile, err := os.Open(res.PackfilePath)
	if err != nil {
		o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "push_failed", fmt.Sprintf("opening packfile: %v", err), nil)
		return nil
	}
	defer func() {
		if closeErr := packFile.Close(); closeErr != nil {
			scoped.Log(ctx).V(2).Info("failed to close packfile", "error", closeErr)
		}
	}()

	var creds forge.PushCredentialer
	if c, ok := f.(forge.PushCredentialer); ok {
		creds = c
	}

	branchName := run.BranchName()
	pushErr := forge.PushPackfile(ctx, forge.PushPackfileOptions{
		RemoteURL:   repo.URL,
		Branch:      branchName,
		Tip:         plumbing.NewHash(res.Tip),
		Base:        plumbing.NewHash(run.BaseCommit),
		Packfile:    packFile,
		Credentials: creds,
	})
	if pushErr != nil {
		if errors.Is(pushErr, forge.ErrBranchMoved) {
			o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "branch_moved", "remote branch is not at base commit", nil)
			return nil
		}
		o.finishRun(ctx, repo, run, sb, state.PhaseFailed, "push_failed", fmt.Sprintf("push packfile: %v", pushErr), nil)
		return nil
	}

	run.Branch = branchName

	pushTime := o.clock().UTC()
	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    pushTime,
		Phase:   state.PhaseSelecting,
		Kind:    "branch_pushed",
		Message: fmt.Sprintf("Pushed branch %s with tip %s", branchName, res.Tip),
		Data: map[string]any{
			"branch": branchName,
			"tip":    res.Tip,
		},
	}); err != nil {
		scoped.Log(ctx).Error(err, "failed to append branch_pushed event", "run", run.ID)
	}

	if sb != nil {
		if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
			scoped.Log(ctx).V(2).Info("error deleting sandbox after run left selecting", "sandbox", run.SandboxName, "error", delErr)
		}
	}

	prev := run.Phase
	run.SetPhase(state.PhaseOpeningPR, pushTime)
	if err := o.putRun(ctx, repo, run, prev); err != nil {
		scoped.Log(ctx).Error(err, "failed to update run to OpeningPR", "run", run.ID)
	}

	return o.openPullRequest(ctx, repo, run)
}

func (o *Orchestrator) openPullRequest(ctx context.Context, repo *state.Repo, run *state.Run) error {
	if ctx.Err() != nil {
		return nil
	}

	if o.forgeFor == nil {
		o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "pull_request_failed", fmt.Sprintf("no forge configured for repository %s", repo.URL), nil)
		return nil
	}

	f, err := o.forgeFor(repo.URL)
	if err != nil {
		o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "pull_request_failed", fmt.Sprintf("selecting forge for %s: %v", repo.URL, err), nil)
		return nil
	}
	if f == nil {
		o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "pull_request_failed", fmt.Sprintf("no forge configured for repository %s", repo.URL), nil)
		return nil
	}

	cr, ok := f.(forge.ChangeRequestCreator)
	if !ok {
		o.finishRun(ctx, repo, run, nil, state.PhaseSucceeded, "pull_request_not_opened", "forge cannot open pull requests", nil)
		return nil
	}

	branch := run.BranchName()
	if run.Branch == "" {
		run.Branch = branch
	}

	prs, err := cr.ListPullRequests(ctx, repo.URL, branch)
	if err != nil {
		o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "pull_request_failed", fmt.Sprintf("listing pull requests: %v", err), nil)
		return nil
	}

	for _, pr := range prs {
		if pr.State == "open" && pr.Head == branch {
			run.PRURL = pr.URL
			o.finishRun(ctx, repo, run, nil, state.PhaseSucceeded, "pull_request_reused", pr.URL, map[string]any{"url": pr.URL})
			return nil
		}
	}

	base := repo.DefaultBranch
	if base == "" {
		base = "main"
	}
	title := run.PRTitle
	if title == "" {
		title = fmt.Sprintf("Optimize %s", run.GoalName)
	}
	body := o.buildPRBody(repo, run)

	pr, err := cr.OpenPullRequest(ctx, repo.URL, branch, base, title, body)
	if err != nil {
		o.finishRun(ctx, repo, run, nil, state.PhaseFailed, "pull_request_failed", err.Error(), nil)
		return nil
	}

	run.PRURL = pr.URL
	o.finishRun(ctx, repo, run, nil, state.PhaseSucceeded, "pull_request_opened", pr.URL, map[string]any{"url": pr.URL})
	return nil
}

func (o *Orchestrator) buildPRBody(repo *state.Repo, run *state.Run) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Goal: %s\n", run.GoalName))
	b.WriteString(fmt.Sprintf("Base commit: %s\n", shortCommit(run.BaseCommit)))
	b.WriteString(fmt.Sprintf("Branch: %s\n", run.Branch))
	b.WriteString(fmt.Sprintf("Run ID: %s\n", run.ID))
	if runURL := o.RunPageURL(repo, run); runURL != "" {
		b.WriteString(fmt.Sprintf("Run page: [%s](%s)\n", runURL, runURL))
	}
	b.WriteString("\n## What the agent says\n\n")
	b.WriteString(run.PRBody)
	return b.String()
}

func shortCommit(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

func buildSystemPrompt(basePrompt string, repo *state.Repo, run *state.Run, goalBody string) string {
	var sb strings.Builder
	if basePrompt != "" {
		sb.WriteString(basePrompt)
		sb.WriteString("\n\n")
	}

	sb.WriteString("# Goal\n\n")
	sb.WriteString(strings.TrimSpace(goalBody))
	sb.WriteString("\n\n")

	sb.WriteString("# Repository Facts\n\n")
	if repo != nil {
		sb.WriteString(fmt.Sprintf("- URL: %s\n", repo.URL))
		if repo.DefaultBranch != "" {
			sb.WriteString(fmt.Sprintf("- Default Branch: %s\n", repo.DefaultBranch))
		}
	}
	if run != nil {
		if branch := run.BranchName(); branch != "" {
			sb.WriteString(fmt.Sprintf("- Branch: %s\n", branch))
		}
		if run.BaseCommit != "" {
			sb.WriteString(fmt.Sprintf("- Base Commit: %s\n", run.BaseCommit))
		}
	}
	sb.WriteString("- Working Directory: /workspace/repo\n\n")
	sb.WriteString("Commit your changes with git before calling open_pull_request; only commits on the branch are pushed, nothing uncommitted is included.\n\n")

	sb.WriteString("# Rules\n\n")
	sb.WriteString("- Never edit .goals/ or the metric script.\n")
	sb.WriteString("- Keep changes focused.\n")
	sb.WriteString("- Call the finish tool with a summary written for maintainers when you have nothing more to do.\n")

	return sb.String()
}

func generateSessionID(runID string) string {
	var sb strings.Builder
	sb.WriteString("run")
	for _, r := range strings.ToLower(runID) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
		}
	}
	id := sb.String()
	if len(id) > 40 {
		id = id[:40]
	}
	return id
}

type orchestratorTurnEvents struct{}

func (e *orchestratorTurnEvents) AssistantMessage(_ context.Context, _ string) {}

func (e *orchestratorTurnEvents) ApproveToolCall(_ context.Context, _ llm.ToolCall) (bool, error) {
	return true, nil
}

func (e *orchestratorTurnEvents) ToolCallStarted(ctx context.Context, call llm.ToolCall) {
	scoped.Log(ctx).V(1).Info("tool call started", "tool", call.Function.Name)
}

func (e *orchestratorTurnEvents) ToolCallFinished(ctx context.Context, call llm.ToolCall, err error) {
	scoped.Log(ctx).V(1).Info("tool call finished", "tool", call.Function.Name, "error", err)
}

func (e *orchestratorTurnEvents) MessageAppended(_ context.Context, _ llm.Message) {}
