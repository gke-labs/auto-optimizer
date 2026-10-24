<!--
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Development Guide

This guide covers setting up a development environment, running tests, deploying dependencies to Kubernetes, and managing upstream versions for `auto-optimizer`.

## Prerequisites

- **Go**: Version 1.27.3 or later (or run commands with `GOTOOLCHAIN=auto`).
- **Docker**: A running Docker daemon accessible locally or via `DOCKER_HOST`.
- **Git**: For version control and repository metadata.

Pinned versions of `kind` and `kubectl` are installed automatically into `.build/bin/` by the development tasks and do not need to be installed on your system `PATH`.

## Core Development Tasks

The project uses two primary development scripts located in `dev/tasks/`:

### 1. `dev/tasks/deploy-to-kube`

Installs all cluster infrastructure and prerequisites needed by `auto-optimizer` into the **current kubeconfig context**.

```bash
./dev/tasks/deploy-to-kube
```

#### What It Installs

`deploy-to-kube` installs, in order, and waits for each to be ready:

1. **Namespaces**:
   - `ao-system`: Namespace for auto-optimizer control plane components.
   - `ao-sandboxes`: Namespace where agent sandboxes run.

2. **agent-sandbox**:
   - Upstream manifests (`sandbox.yaml` and `extensions.yaml`) from `kubernetes-sigs/agent-sandbox` pinned to release `v1.0.8`.
   - Applied via `kubectl apply --server-side`.
   - Waits for the `sandboxes.agents.x-k8s.io` CRD to be established and the `agent-sandbox-controller` deployment in `agent-sandbox-system` to roll out.

3. **service-portals**:
   - Sandboxes use service-portals exclusively in transparent sidecar mode (see `docs/design.md`, Sandboxes). Auto-optimizer does not need the central explicit service proxy (`Deployment/service-portal`) or `cert-manager`.
   - Clones `gke-labs/service-portals` outside the source tree under `${XDG_CACHE_HOME:-$HOME/.cache}/auto-optimizer/service-portals/<commit>` at pinned commit `686b34d8aa74546af1039119bbfa6e05ead15f53`.
   - Builds container images (`init-service-portals`, `all-in-one-portal`, `gitproxy-server`, `service-portal`) using `ap build`.
   - On `kind`, loads the built images into kind nodes. On a real cluster, builds and pushes images to `IMAGE_PREFIX`.

4. **in-cluster-storage (ObjectFS)**:
   - Clones `gke-labs/in-cluster-storage` outside the source tree under `${XDG_CACHE_HOME:-$HOME/.cache}/auto-optimizer/in-cluster-storage/<commit>` at pinned commit `923578df519c02cd0da1b7455d8a1e0278a103c4`.
   - Deploys upstream manifests without patching using `ap deploy`. `ap deploy` builds the images and applies `k8s/wal.yaml` and `k8s/objectfs.yaml`, prefixing image references with `IMAGE_PREFIX`.
   - On local `kind` clusters:
     - Builds in-cluster `fakes3` (`fakes3/`) with `ap build` and loads it into kind.
     - Builds in-cluster-storage images (`wal-buffer`, `objectfs-controller`, `objectfs-node-daemon`) and loads them into kind.
     - Runs `ap deploy --skip-push`.
     - Applies the `objectfs-config` ConfigMap pointing `backend` to `fakes3`.
     - Injects S3 credentials into `wal-buffer` and `objectfs-controller` StatefulSets via a Kubernetes Secret and `kubectl set env` (no manifest patching).
   - Waits for the `wal-buffer` StatefulSet, `objectfs-controller` StatefulSet, and `objectfs-node-daemon` DaemonSet to roll out successfully.
   - Waits for the `objectfs-exclusive-local` StorageClass to be established.

5. **auto-optimizer (`ao`)**:
   - Deploys control plane manifests in `k8s/ao.yaml`:
     - `ServiceAccount` `ao` in `ao-system`.
     - `Role` and `RoleBinding` in `ao-sandboxes` granting least-privilege access to manage `sandboxes` and sandbox execution.
     - `ConfigMap` `ao-config` in `ao-system` setting server and sandbox configuration.
     - `Deployment` `ao` in `ao-system` running `ao serve` with an `emptyDir` state volume and read-only root filesystem.
     - `Service` `ao` ClusterIP on port 8081 named `grpc`.
   - Builds container images (`ao`, `geminicli-toolbox`) and loads them into kind (or pushes to `IMAGE_PREFIX`).
   - Runs `ap deploy` to apply manifests with image tags replaced.
   - On kind, configures `OPENAI_MODEL=fake-eliza` and `SANDBOX_IMAGE=${IMAGE_PREFIX}/geminicli-toolbox:${IMAGE_TAG}` in `ao-config` by default.
   - Waits for `deployment/ao` in `ao-system` to roll out.

#### Interacting with the In-Cluster Control Plane

##### Connecting to the In-Cluster Server

After `deploy-to-kube` completes, port-forward the `ao` service to your machine:

```bash
kubectl -n ao-system port-forward svc/ao 8081:8081
```

In another terminal, use `ao session send` to send a prompt to the in-cluster server:

```bash
ao session send demo "run: uname -a"
```

The command streams events from the server and outputs the assistant's reply. You can verify the sandbox created by the server in the sandbox namespace:

```bash
kubectl get sandboxes -n ao-sandboxes
```

Client commands default to `--server 127.0.0.1:8081` (configurable via `--server` or the `AO_SERVER` environment variable).

##### Configuring a Real Model (e.g. Gemini)

By default on kind, `deploy-to-kube` sets `OPENAI_MODEL=fake-eliza` so no external API keys are needed.

To use a real model such as Gemini:

1. Create the `ao-model-keys` secret in `ao-system` with your API key:
   ```bash
   kubectl create secret generic ao-model-keys \
     -n ao-system \
     --from-literal=GEMINI_API_KEY="your-api-key"
   ```

2. Pass `OPENAI_MODEL` when running `deploy-to-kube`:
   ```bash
   OPENAI_MODEL=gemini-2.5-flash ./dev/tasks/deploy-to-kube
   ```
   Or update the `ao-config` ConfigMap directly and restart the deployment:
   ```bash
   kubectl -n ao-system patch configmap ao-config --type=merge -p '{"data":{"OPENAI_MODEL":"gemini-2.5-flash"}}'
   kubectl -n ao-system rollout restart deploy/ao
   ```

#### Idempotence

`deploy-to-kube` is idempotent. Running it a second time against an already-configured cluster will make no changes, detect existing resources and rollouts, and exit `0` within seconds. Sibling checkouts are cached and reused at their pinned commits.

#### Pointing at a Real Cluster (e.g. GKE)

To point `deploy-to-kube` at a real cluster rather than `kind`:

1. Set your `kubectl` context to your target cluster:
   ```bash
   kubectl config use-context <my-cluster-context>
   ```

2. Set `IMAGE_PREFIX` to a container registry accessible to the cluster (such as Google Artifact Registry):
   ```bash
   export IMAGE_PREFIX="us-central1-docker.pkg.dev/my-project/my-repo"
   ```

3. Specify an external object storage backend URL using the `OBJECTFS_BACKEND` environment variable. For example, for Google Cloud Storage:
   ```bash
   export OBJECTFS_BACKEND="gs://my-bucket/prefix"
   ```
   Or for Amazon S3:
   ```bash
   export OBJECTFS_BACKEND="s3://my-bucket/prefix"
   ```

4. Run the script:
   ```bash
   ./dev/tasks/deploy-to-kube
   ```

When `OBJECTFS_BACKEND` is set, `deploy-to-kube` skips deploying in-cluster `fakes3` and configures ObjectFS to use the specified cloud storage bucket.

#### Bumping Pinned Versions

All upstream dependencies are pinned in variables at the top of `dev/tasks/deploy-to-kube`:

```bash
AGENT_SANDBOX_VERSION="v1.0.8"
SERVICE_PORTALS_COMMIT="686b34d8aa74546af1039119bbfa6e05ead15f53"
IN_CLUSTER_STORAGE_COMMIT="923578df519c02cd0da1b7455d8a1e0278a103c4"
```

To bump a version:
- **`AGENT_SANDBOX_VERSION`**: Check releases at [kubernetes-sigs/agent-sandbox/releases](https://github.com/kubernetes-sigs/agent-sandbox/releases) and update the version tag.
- **`SERVICE_PORTALS_COMMIT`**: Check latest commits at [gke-labs/service-portals/commits/main](https://github.com/gke-labs/service-portals/commits/main) and update the commit SHA.
- **`IN_CLUSTER_STORAGE_COMMIT`**: Check latest commits at [gke-labs/in-cluster-storage/commits/main](https://github.com/gke-labs/in-cluster-storage/commits/main) and update the commit SHA.

After updating versions, run `./dev/tasks/test-e2e` to verify compatibility.

---

### 2. `dev/tasks/test-e2e`

Prepares a local `kind` cluster and deploys dependencies for end-to-end testing. This script is discovered and run automatically by `ap e2e` before executing the Go E2E tests, but can also be run standalone to prepare or reset the local cluster.

```bash
./dev/tasks/test-e2e
```

#### What It Does

1. **Tool Setup**: Ensures pinned `kind` (v0.31.0), `kubectl` (v1.35.0), and (if missing) `docker` CLI and `docker-buildx` plugin are installed under `.build/` (with `docker-buildx` linked into `~/.docker/cli-plugins/`) and placed on `PATH`.
2. **Cluster Creation**: Creates a `kind` cluster named `auto-optimizer` (or reuses an existing one if already present). The cluster is kept alive by default after preparation and test runs so you can inspect resources or rerun tests.
3. **Deployment**: Executes `dev/tasks/deploy-to-kube` against the cluster to deploy all prerequisites.

#### Options and Flags

- `--cleanup` or `-c`: Deletes the `auto-optimizer` kind cluster and exits without creating a new cluster, deploying dependencies, or running tests.
  ```bash
  ./dev/tasks/test-e2e --cleanup
  ```
- `--delete` or `-d`: Deletes any existing `auto-optimizer` cluster before creating a fresh one and deploying dependencies to start fresh.
  ```bash
  ./dev/tasks/test-e2e --delete
  ```

---

## End-to-End Testing

End-to-end tests are implemented in `tests/e2e/e2e_test.go` using the test harness in `tests/e2e/harness.go`. Tests skip automatically unless `RUN_E2E=1` is set, ensuring unit test runs (`ap test`, `go test ./...`) pass without a Kubernetes cluster.

### Running E2E Tests via `ap` (Recommended)

The single unified path to run the full end-to-end test suite is:

```bash
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest e2e
```

When `ap e2e` runs:
1. `dev/tasks/test-e2e` runs first to ensure tools are installed, create the local `auto-optimizer` kind cluster, and deploy dependencies via `deploy-to-kube`.
2. `ap`'s Go e2e task runs second, executing `RUN_E2E=1 go test -v -count=1 -timeout 30m ./tests/e2e/...` against the prepared cluster.

### Running E2E Tests Directly Against an Existing Cluster

If you already have a cluster prepared (via `./dev/tasks/test-e2e`, `./dev/tasks/deploy-to-kube`, or a remote Kubernetes cluster), you can run the Go E2E tests directly:

```bash
RUN_E2E=1 go test -v -count=1 ./tests/e2e/...
```

To run a specific subtest:

```bash
RUN_E2E=1 go test -v -count=1 -run TestSmoke/SandboxHealth ./tests/e2e/...
```

#### Test Coverage

Subtests in `TestSmoke` verify:
- `SandboxHealth`: Creates a minimal `Sandbox` (`agents.x-k8s.io/v1beta1`) in `ao-sandboxes` running upstream `sandboxd`, waits for `Ready`, port-forwards to port 8080, queries `/v1/health` for `{"status":"ok"}`, and deletes the sandbox.
- `ObjectFSPVCWrite`: Creates a `PersistentVolumeClaim` with `storageClassName: objectfs-exclusive-local` and `ReadWriteOncePod` access mode. Launches a test Pod writing to the mounted volume, verifies successful exit and output, and deletes test resources.
- `ServicePortalsImages`: Launches a test Pod in namespace `default` running `init-service-portals --help` from the built image (`${IMAGE_PREFIX}/init-service-portals:${IMAGE_TAG}`) and verifies it exits `0`.

---

## Presubmits and CI

The repository uses `ap` from `gke-labs-infra` for code generation, formatting, and CI presubmits.

### Running with `ap`

```bash
# Regenerate presubmits, workflows, and format code
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest generate //...

# Run E2E tests
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest e2e

# Run linter
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest lint

# Run unit tests
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest test
```

### GitHub Actions Presubmits

Pull requests run the presubmits defined in `.github/workflows/ci-presubmits.yaml`, which are generated by `ap generate`:
- `ap-verify-generate`: Verifies that generated files are in sync with `ap generate`.
- `ap-lint`: Static analysis and formatting check.
- `ap-test`: Unit tests.
- `ap-e2e`: Runs `ap e2e` against a kind cluster in CI (preparing the cluster via `dev/tasks/test-e2e` and executing the Go test suite).
