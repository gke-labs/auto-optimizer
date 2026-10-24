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

package e2e

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Harness provides test helpers for end-to-end tests against a Kubernetes/kind cluster.
type Harness struct {
	t           *testing.T
	clusterName string
}

// NewHarness creates a new Harness instance. If clusterName is empty, it checks
// KIND_CLUSTER_NAME or defaults to "auto-optimizer".
func NewHarness(t *testing.T, clusterName string) *Harness {
	if clusterName == "" {
		clusterName = os.Getenv("KIND_CLUSTER_NAME")
		if clusterName == "" {
			clusterName = "auto-optimizer"
		}
	}
	return &Harness{
		t:           t,
		clusterName: clusterName,
	}
}

// WithT returns a copy of the Harness associated with a subtest's *testing.T.
func (h *Harness) WithT(t *testing.T) *Harness {
	return &Harness{
		t:           t,
		clusterName: h.clusterName,
	}
}

// ClusterName returns the name of the kind cluster under test.
func (h *Harness) ClusterName() string {
	return h.clusterName
}

// ContextName returns the kubeconfig context name for the kind cluster.
func (h *Harness) ContextName() string {
	if strings.HasPrefix(h.clusterName, "kind-") {
		return h.clusterName
	}
	return "kind-" + h.clusterName
}

// Setup verifies prerequisites and ensures the correct kubectl context is active,
// creating the kind cluster and deploying dependencies if it does not yet exist.
func (h *Harness) Setup() {
	h.t.Helper()
	h.ensurePath()

	if _, err := exec.LookPath("kubectl"); err != nil {
		h.t.Fatalf("kubectl not found on PATH: %v", err)
	}

	// Check if kind cluster exists
	if _, err := exec.LookPath("kind"); err != nil {
		h.t.Logf("kind binary not found on PATH: %v", err)
	} else {
		cmd := exec.Command("kind", "get", "clusters")
		out, err := cmd.Output()
		exists := false
		if err != nil {
			h.t.Logf("failed to get kind clusters: %v", err)
		} else {
			for _, c := range strings.Split(string(out), "\n") {
				if strings.TrimSpace(c) == h.clusterName {
					exists = true
					break
				}
			}
		}

		if !exists {
			h.t.Logf("Cluster %s does not exist, creating with kind...", h.clusterName)
			createCmd := exec.Command("kind", "create", "cluster", "--name", h.clusterName)
			if createOut, createErr := createCmd.CombinedOutput(); createErr != nil {
				h.t.Fatalf("failed to create kind cluster %s: %v\nOutput: %s", h.clusterName, createErr, string(createOut))
			}

			// Deploy dependencies using dev/tasks/deploy-to-kube
			gitRoot, gitErr := h.getGitRoot()
			if gitErr != nil {
				h.t.Logf("failed to get git root: %v", gitErr)
			} else {
				deployScript := filepath.Join(gitRoot, "dev", "tasks", "deploy-to-kube")
				if _, statErr := os.Stat(deployScript); statErr != nil {
					h.t.Logf("deploy script not found: %v", statErr)
				} else {
					h.t.Logf("Deploying dependencies via %s...", deployScript)
					deployCmd := exec.Command(deployScript)
					deployCmd.Env = append(os.Environ(), "KIND_CLUSTER_NAME="+h.clusterName)
					if deployOut, deployErr := deployCmd.CombinedOutput(); deployErr != nil {
						h.t.Fatalf("failed to deploy dependencies: %v\nOutput: %s", deployErr, string(deployOut))
					}
				}
			}

			h.t.Cleanup(func() {
				if os.Getenv("KEEP_CLUSTER") != "true" && os.Getenv("CI") != "true" && os.Getenv("GITHUB_ACTIONS") != "true" {
					h.t.Logf("Deleting kind cluster %s...", h.clusterName)
					delCmd := exec.Command("kind", "delete", "cluster", "--name", h.clusterName)
					if delOut, delErr := delCmd.CombinedOutput(); delErr != nil {
						h.t.Logf("warning: failed to delete kind cluster: %v\nOutput: %s", delErr, string(delOut))
					}
				}
			})
		}
	}

	// Switch to target cluster context
	if _, err := h.Kubectl("config", "use-context", h.ContextName()); err != nil {
		h.t.Logf("warning: failed to switch kubectl context to %s: %v", h.ContextName(), err)
	}
}

func (h *Harness) getGitRoot() (string, error) {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel failed: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// ensurePath ensures .build/bin is in PATH if present.
func (h *Harness) ensurePath() {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, outErr := cmd.Output()
	if outErr != nil {
		return
	}
	root := strings.TrimSpace(string(out))
	buildBin := filepath.Join(root, ".build", "bin")
	if info, err := os.Stat(buildBin); err == nil && info.IsDir() {
		currentPath := os.Getenv("PATH")
		if !strings.Contains(currentPath, buildBin) {
			if setErr := os.Setenv("PATH", buildBin+string(os.PathListSeparator)+currentPath); setErr != nil {
				h.t.Logf("warning: failed to update PATH: %v", setErr)
			}
		}
	}
}

// Kubectl executes a kubectl command against the harness's cluster context.
func (h *Harness) Kubectl(args ...string) (string, error) {
	cmdArgs := append([]string{"--context", h.ContextName()}, args...)
	cmd := exec.Command("kubectl", cmdArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), fmt.Errorf("kubectl %s failed: %w (stderr: %s)", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// RunKubectl executes a kubectl command, logging the command and failing the test on error.
func (h *Harness) RunKubectl(args ...string) string {
	h.t.Helper()
	out, err := h.Kubectl(args...)
	if err != nil {
		h.t.Fatalf("%v", err)
	}
	return out
}

// KubectlWithStdin executes a kubectl command providing the given string on standard input.
func (h *Harness) KubectlWithStdin(stdin string, args ...string) (string, error) {
	cmdArgs := append([]string{"--context", h.ContextName()}, args...)
	cmd := exec.Command("kubectl", cmdArgs...)
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), fmt.Errorf("kubectl %s failed: %w (stderr: %s)", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// ApplyManifest applies a Kubernetes manifest string via kubectl apply -f -.
func (h *Harness) ApplyManifest(manifest string) {
	h.t.Helper()
	h.t.Logf("Applying manifest:\n%s", manifest)
	if _, err := h.KubectlWithStdin(manifest, "apply", "-f", "-"); err != nil {
		h.t.Fatalf("failed to apply manifest: %v", err)
	}
}

// ApplyManifestWithCleanup applies a manifest string and registers a t.Cleanup callback
// to delete the applied resources when the test completes.
func (h *Harness) ApplyManifestWithCleanup(manifest string) {
	h.t.Helper()
	h.ApplyManifest(manifest)
	h.t.Cleanup(func() {
		h.DeleteManifest(manifest)
	})
}

// DeleteManifest deletes resources described by a manifest string.
func (h *Harness) DeleteManifest(manifest string) {
	h.t.Helper()
	h.t.Logf("Deleting manifest resources...")
	if _, err := h.KubectlWithStdin(manifest, "delete", "-f", "-", "--ignore-not-found", "--wait=true", "--timeout=60s"); err != nil {
		h.t.Logf("warning: failed to delete manifest: %v", err)
	}
}

// Delete deletes a named resource of kind in namespace with --ignore-not-found.
func (h *Harness) Delete(namespace, kind, name string) {
	h.t.Helper()
	h.t.Logf("Deleting %s %s/%s", kind, namespace, name)
	args := []string{"delete", kind, name, "--ignore-not-found", "--wait=true", "--timeout=60s"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	if _, err := h.Kubectl(args...); err != nil {
		h.t.Logf("warning: failed to delete %s %s/%s: %v", kind, namespace, name, err)
	}
}

// DeleteWithCleanup registers a t.Cleanup callback to delete the specified resource.
func (h *Harness) DeleteWithCleanup(namespace, kind, name string) {
	h.t.Helper()
	h.t.Cleanup(func() {
		h.Delete(namespace, kind, name)
	})
}

// WaitForCondition waits for a resource condition to be met within the specified timeout.
// condition can be formatted as "condition=Ready" or simply "Ready".
func (h *Harness) WaitForCondition(namespace, resource, condition string, timeout time.Duration) {
	h.t.Helper()
	if !strings.Contains(condition, "=") {
		condition = "condition=" + condition
	}
	h.t.Logf("Waiting for %s condition %s (timeout: %s)...", resource, condition, timeout)
	args := []string{"wait", "--for=" + condition, resource, fmt.Sprintf("--timeout=%s", timeout)}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	if _, err := h.Kubectl(args...); err != nil {
		// Dump describe on failure
		descArgs := []string{"describe", resource}
		if namespace != "" {
			descArgs = append(descArgs, "-n", namespace)
		}
		desc, descErr := h.Kubectl(descArgs...)
		if descErr != nil {
			h.t.Logf("Failed to describe %s: %v", resource, descErr)
		} else {
			h.t.Logf("Resource %s describe on wait failure:\n%s", resource, desc)
		}
		h.t.Fatalf("timed out waiting for %s condition %s: %v", resource, condition, err)
	}
}

// WaitForPodSuccess waits for a Pod to reach the "Succeeded" phase, failing if the Pod reaches "Failed"
// or times out.
func (h *Harness) WaitForPodSuccess(namespace, podName string, timeout time.Duration) {
	h.t.Helper()
	h.t.Logf("Waiting for pod %s/%s to succeed (timeout: %s)...", namespace, podName, timeout)
	start := time.Now()
	for {
		if time.Since(start) > timeout {
			desc, descErr := h.Kubectl("describe", "pod", podName, "-n", namespace)
			if descErr != nil {
				h.t.Logf("Failed to describe pod on timeout: %v", descErr)
			} else {
				h.t.Logf("Pod %s/%s describe on timeout:\n%s", namespace, podName, desc)
			}
			h.t.Fatalf("timeout waiting for pod %s/%s to succeed", namespace, podName)
		}

		phase, err := h.Kubectl("get", "pod", podName, "-n", namespace, "-o", "jsonpath={.status.phase}")
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		switch strings.TrimSpace(phase) {
		case "Succeeded":
			return
		case "Failed":
			logs, logsErr := h.Kubectl("logs", podName, "-n", namespace)
			if logsErr != nil {
				h.t.Logf("failed to get pod logs: %v", logsErr)
			}
			desc, descErr := h.Kubectl("describe", "pod", podName, "-n", namespace)
			if descErr != nil {
				h.t.Logf("failed to describe pod: %v", descErr)
			}
			h.t.Logf("Pod %s/%s failed. Logs:\n%s\nDescription:\n%s", namespace, podName, logs, desc)
			h.t.Fatalf("pod %s/%s entered Failed phase", namespace, podName)
		}
		time.Sleep(2 * time.Second)
	}
}

// PortForward starts a port-forward from a free local port to the specified remote targetPort.
// It registers a t.Cleanup callback to stop the port-forward when the test finishes.
// It returns the assigned local port and an explicit stop function.
func (h *Harness) PortForward(namespace, target string, remotePort int) (int, func()) {
	h.t.Helper()

	// Find an available local port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatalf("failed to allocate local port for port-forward: %v", err)
	}
	localPort := listener.Addr().(*net.TCPAddr).Port
	if closeErr := listener.Close(); closeErr != nil {
		h.t.Fatalf("failed to close listener: %v", closeErr)
	}

	cmdArgs := []string{"--context", h.ContextName(), "port-forward"}
	if namespace != "" {
		cmdArgs = append(cmdArgs, "-n", namespace)
	}
	cmdArgs = append(cmdArgs, target, fmt.Sprintf("%d:%d", localPort, remotePort))

	cmd := exec.Command("kubectl", cmdArgs...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("failed to start port-forward to %s: %v", target, err)
	}

	var once sync.Once
	stop := func() {
		once.Do(func() {
			if cmd.Process != nil {
				if killErr := cmd.Process.Kill(); killErr != nil {
					h.t.Logf("warning: failed to kill port-forward: %v", killErr)
				}
				if waitErr := cmd.Wait(); waitErr != nil {
					h.t.Logf("port-forward process finished: %v", waitErr)
				}
			}
		})
	}
	h.t.Cleanup(stop)

	// Wait for port to become reachable
	addr := fmt.Sprintf("127.0.0.1:%d", localPort)
	ready := false
	for i := 0; i < 50; i++ {
		conn, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dialErr != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if closeErr := conn.Close(); closeErr != nil {
			h.t.Logf("warning: failed to close probe connection: %v", closeErr)
		}
		ready = true
		break
	}

	if !ready {
		stop()
		h.t.Fatalf("port-forward to %s (%s) did not become reachable within 5s: %s", target, addr, stderr.String())
	}

	return localPort, stop
}

// PortForwardService starts a port-forward from a free local port to the specified remote service and port.
// It returns the local address ("127.0.0.1:<port>") and an explicit stop function.
func (h *Harness) PortForwardService(namespace, service string, remotePort int) (string, func()) {
	h.t.Helper()
	target := service
	if !strings.HasPrefix(target, "svc/") && !strings.HasPrefix(target, "service/") {
		target = "svc/" + target
	}
	port, stop := h.PortForward(namespace, target, remotePort)
	return fmt.Sprintf("127.0.0.1:%d", port), stop
}

// GetPodLogs returns the logs for the specified pod and optional container.
func (h *Harness) GetPodLogs(namespace, pod string, container ...string) string {
	h.t.Helper()
	args := []string{"logs", pod}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	if len(container) > 0 && container[0] != "" {
		args = append(args, "-c", container[0])
	}
	logs, err := h.Kubectl(args...)
	if err != nil {
		h.t.Fatalf("failed to get logs for pod %s/%s: %v", namespace, pod, err)
	}
	return logs
}
