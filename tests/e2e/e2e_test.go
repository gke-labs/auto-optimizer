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
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSmoke(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e smoke tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	t.Run("SandboxHealth", func(t *testing.T) {
		subH := h.WithT(t)
		sandboxName := "smoke-test-sandbox"
		subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sandboxName)

		shutdownTime := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339)
		sandboxManifest := fmt.Sprintf(`
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata:
  name: %s
  namespace: ao-sandboxes
spec:
  shutdownPolicy: Delete
  shutdownTime: "%s"
  podTemplate:
    spec:
      containers:
        - name: sandboxd
          image: us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/sandboxd:latest-main
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /v1/health
              port: 8080
`, sandboxName, shutdownTime)

		subH.ApplyManifest(sandboxManifest)

		// Wait for sandbox Ready condition
		subH.WaitForCondition("ao-sandboxes", "sandbox/"+sandboxName, "condition=Ready", 120*time.Second)

		// Port-forward to sandbox pod
		localPort, stop := subH.PortForward("ao-sandboxes", "pod/"+sandboxName, 8080)
		defer stop()

		// Query health endpoint
		client := &http.Client{Timeout: 2 * time.Second}
		healthURL := fmt.Sprintf("http://127.0.0.1:%d/v1/health", localPort)
		healthOK := false
		var lastBody string

		start := time.Now()
		for time.Since(start) < 30*time.Second {
			resp, err := client.Get(healthURL)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			body, readErr := io.ReadAll(resp.Body)
			if closeErr := resp.Body.Close(); closeErr != nil {
				t.Logf("warning: failed to close response body: %v", closeErr)
			}
			if readErr != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			lastBody = string(body)
			if strings.Contains(lastBody, `"status":"ok"`) {
				healthOK = true
				break
			}
			time.Sleep(1 * time.Second)
		}

		if !healthOK {
			t.Fatalf("Sandbox health check failed on port %d. Expected {\"status\":\"ok\"}, got: %s", localPort, lastBody)
		}
		t.Logf("PASS: Sandbox %s reported healthy status.", sandboxName)
	})

	t.Run("ObjectFSPVCWrite", func(t *testing.T) {
		subH := h.WithT(t)
		pvcName := "smoke-test-objectfs-pvc"
		podName := "smoke-test-objectfs-pod"

		t.Cleanup(func() {
			subH.Delete("default", "pod", podName)
			subH.Delete("default", "pvc", pvcName)
		})

		storageManifest := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: default
spec:
  accessModes:
    - ReadWriteOncePod
  storageClassName: objectfs-exclusive-local
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
spec:
  restartPolicy: Never
  volumes:
    - name: objectfs-vol
      persistentVolumeClaim:
        claimName: %s
  containers:
    - name: writer
      image: busybox:latest
      command: ["sh", "-c", "echo 'smoke test data' > /workspace/test.txt && cat /workspace/test.txt && exit 0"]
      volumeMounts:
        - name: objectfs-vol
          mountPath: /workspace
`, pvcName, podName, pvcName)

		subH.ApplyManifest(storageManifest)

		// Wait for pod to complete
		subH.WaitForPodSuccess("default", podName, 120*time.Second)

		// Verify pod logs
		logs := subH.GetPodLogs("default", podName)
		if !strings.Contains(logs, "smoke test data") {
			t.Fatalf("Unexpected pod logs from %s: %s", podName, logs)
		}
		t.Logf("PASS: ObjectFS PVC read/write verified successfully.")
	})

	t.Run("ServicePortalsImages", func(t *testing.T) {
		subH := h.WithT(t)
		podName := "smoke-test-service-portals"
		subH.DeleteWithCleanup("default", "pod", podName)

		imagePrefix := os.Getenv("IMAGE_PREFIX")
		if imagePrefix == "" {
			imagePrefix = "auto-optimizer.local"
		}
		imageTag := os.Getenv("IMAGE_TAG")
		if imageTag == "" {
			imageTag = "dev"
		}
		image := fmt.Sprintf("%s/init-service-portals:%s", imagePrefix, imageTag)

		manifest := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
spec:
  restartPolicy: Never
  containers:
    - name: init-service-portals
      image: %s
      command: ["init-service-portals", "--help"]
`, podName, image)

		subH.ApplyManifest(manifest)

		// Wait for pod to complete
		subH.WaitForPodSuccess("default", podName, 60*time.Second)

		// Verify pod logs
		logs := subH.GetPodLogs("default", podName)
		if !strings.Contains(logs, "Usage of") && !strings.Contains(logs, "-cert-dir") {
			t.Fatalf("Unexpected pod logs from %s: %s", podName, logs)
		}
		t.Logf("PASS: service-portals init-service-portals ran --help and exited 0.")
	})
}

func TestSession(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e session tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	gitRoot, err := h.getGitRoot()
	if err != nil {
		t.Fatalf("failed to get git root: %v", err)
	}

	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}

	sessionName := "e2esession"
	stateDir := t.TempDir()

	subH := h.WithT(t)
	subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sessionName)

	runCmd := exec.Command(aoBin, "session", "run",
		"--session", sessionName,
		"--prompt", "run: uname -a",
		"--yolo",
		"--state-dir", stateDir,
	)
	runCmd.Env = append(os.Environ(), "OPENAI_MODEL=fake-eliza")

	var stdout, stderr bytes.Buffer
	runCmd.Stdout = &stdout
	runCmd.Stderr = &stderr

	t.Logf("Running %s session run...", aoBin)
	if err := runCmd.Run(); err != nil {
		t.Fatalf("ao session run failed: %v\nStdout: %s\nStderr: %s", err, stdout.String(), stderr.String())
	}

	output := stdout.String()
	t.Logf("ao session run output:\n%s", output)

	if !strings.Contains(output, "Linux") {
		t.Fatalf("expected output to contain 'Linux', got:\n%s", output)
	}

	// Verify sandbox was created in ao-sandboxes and expires (has shutdownPolicy: Delete and shutdownTime)
	sbJSON, err := subH.Kubectl("get", "sandbox", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.shutdownPolicy} {.spec.shutdownTime}")
	if err != nil {
		t.Fatalf("failed to get sandbox %s in ao-sandboxes: %v", sessionName, err)
	}

	parts := strings.Fields(sbJSON)
	if len(parts) < 2 {
		t.Fatalf("expected shutdownPolicy and shutdownTime, got: %q", sbJSON)
	}
	shutdownPolicy := parts[0]
	shutdownTime := parts[1]

	if shutdownPolicy != "Delete" {
		t.Errorf("shutdownPolicy = %q, want Delete", shutdownPolicy)
	}
	if shutdownTime == "" {
		t.Errorf("shutdownTime is empty")
	}

	t.Logf("PASS: ao session run successfully executed in sandbox, verified output contains Linux, shutdownPolicy=%s, shutdownTime=%s", shutdownPolicy, shutdownTime)
}

func TestSessionAPI(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e session API tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	gitRoot, err := h.getGitRoot()
	if err != nil {
		t.Fatalf("failed to get git root: %v", err)
	}

	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}

	// Allocate a free port for ao serve.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	serverAddr := listener.Addr().String()
	_ = listener.Close()

	sessionName := "e2eapi"
	stateDir := t.TempDir()

	subH := h.WithT(t)
	subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sessionName)

	serveCmd := exec.Command(aoBin, "serve",
		"--listen", serverAddr,
		"--state-dir", stateDir,
	)
	serveCmd.Env = append(os.Environ(), "OPENAI_MODEL=fake-eliza")

	var serveStdout, serveStderr bytes.Buffer
	serveCmd.Stdout = &serveStdout
	serveCmd.Stderr = &serveStderr

	t.Logf("Starting ao serve on %s...", serverAddr)
	if err := serveCmd.Start(); err != nil {
		t.Fatalf("failed to start ao serve: %v", err)
	}
	t.Cleanup(func() {
		if serveCmd.Process != nil {
			_ = serveCmd.Process.Signal(syscall.SIGTERM)
			_ = serveCmd.Wait()
		}
	})

	// Wait for server to accept connections.
	serverReady := false
	for range 50 {
		conn, err := net.DialTimeout("tcp", serverAddr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			serverReady = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !serverReady {
		t.Fatalf("timed out waiting for ao serve to become ready on %s\nStderr: %s", serverAddr, serveStderr.String())
	}

	// 1. ao session send e2eapi "run: uname -a"
	sendCmd := exec.Command(aoBin, "session", "send", sessionName, "run: uname -a",
		"--server", serverAddr,
	)
	sendCmd.Env = append(os.Environ(), "OPENAI_MODEL=fake-eliza")

	var sendStdout, sendStderr bytes.Buffer
	sendCmd.Stdout = &sendStdout
	sendCmd.Stderr = &sendStderr

	t.Logf("Running ao session send %s...", sessionName)
	if err := sendCmd.Run(); err != nil {
		t.Fatalf("ao session send failed: %v\nStdout: %s\nStderr: %s\nServer Stderr: %s",
			err, sendStdout.String(), sendStderr.String(), serveStderr.String())
	}

	sendOutput := sendStdout.String()
	t.Logf("ao session send output:\n%s", sendOutput)

	if !strings.Contains(sendOutput, "Linux") {
		t.Fatalf("expected output to contain 'Linux', got:\n%s", sendOutput)
	}

	// 2. ao session list --server <serverAddr>
	listCmd := exec.Command(aoBin, "session", "list",
		"--server", serverAddr,
	)
	var listStdout, listStderr bytes.Buffer
	listCmd.Stdout = &listStdout
	listCmd.Stderr = &listStderr

	t.Logf("Running ao session list...")
	if err := listCmd.Run(); err != nil {
		t.Fatalf("ao session list failed: %v\nStdout: %s\nStderr: %s", err, listStdout.String(), listStderr.String())
	}

	listOutput := listStdout.String()
	t.Logf("ao session list output:\n%s", listOutput)
	if !strings.Contains(listOutput, sessionName) {
		t.Fatalf("expected ao session list to contain %q, got:\n%s", sessionName, listOutput)
	}

	// 3. ao session get e2eapi --server <serverAddr>
	getCmd := exec.Command(aoBin, "session", "get", sessionName,
		"--server", serverAddr,
	)
	var getStdout, getStderr bytes.Buffer
	getCmd.Stdout = &getStdout
	getCmd.Stderr = &getStderr

	t.Logf("Running ao session get %s...", sessionName)
	if err := getCmd.Run(); err != nil {
		t.Fatalf("ao session get failed: %v\nStdout: %s\nStderr: %s", err, getStdout.String(), getStderr.String())
	}

	getOutput := getStdout.String()
	t.Logf("ao session get output:\n%s", getOutput)
	if !strings.Contains(getOutput, "run: uname -a") || !strings.Contains(getOutput, "Linux") {
		t.Fatalf("expected ao session get output to contain messages with 'run: uname -a' and 'Linux', got:\n%s", getOutput)
	}

	// 3b. Verify ao session get --json produces valid JSON
	getJSONCmd := exec.Command(aoBin,
		"session", "get", sessionName,
		"--server", serverAddr,
		"--json",
	)
	var getJSONStdout, getJSONStderr bytes.Buffer
	getJSONCmd.Stdout = &getJSONStdout
	getJSONCmd.Stderr = &getJSONStderr

	t.Logf("Running ao session get %s --json...", sessionName)
	if err := getJSONCmd.Run(); err != nil {
		t.Fatalf("ao session get --json failed: %v\nStdout: %s\nStderr: %s", err, getJSONStdout.String(), getJSONStderr.String())
	}
	getJSONOutput := getJSONStdout.String()
	if !strings.Contains(getJSONOutput, `"messages"`) || !strings.Contains(getJSONOutput, `"role"`) {
		t.Fatalf("expected JSON output from ao session get --json, got:\n%s", getJSONOutput)
	}

	// 4. Verify sandbox was created in ao-sandboxes with shutdownPolicy: Delete and shutdownTime
	sbJSON, err := subH.Kubectl("get", "sandbox", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.shutdownPolicy} {.spec.shutdownTime}")
	if err != nil {
		t.Fatalf("failed to get sandbox %s in ao-sandboxes: %v", sessionName, err)
	}

	parts := strings.Fields(sbJSON)
	if len(parts) < 2 {
		t.Fatalf("expected shutdownPolicy and shutdownTime, got: %q", sbJSON)
	}
	shutdownPolicy := parts[0]
	shutdownTime := parts[1]

	if shutdownPolicy != "Delete" {
		t.Errorf("shutdownPolicy = %q, want Delete", shutdownPolicy)
	}
	if shutdownTime == "" {
		t.Errorf("shutdownTime is empty")
	}

	t.Logf("PASS: ao serve + session send/list/get successfully verified in kind, shutdownPolicy=%s, shutdownTime=%s", shutdownPolicy, shutdownTime)
}
