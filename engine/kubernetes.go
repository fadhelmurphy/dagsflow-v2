package engine

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dagsflow-go/dag"

	"gopkg.in/yaml.v3"
)

// KubernetesExecutor (~ Airflow's KubernetesExecutor) runs every task attempt
// in its own pod. The pod runs `dagsflow-go worker --job-id N`, which claims
// that job from the metadata database, runs the task and reports the result;
// the scheduler watches the pod so crashes (OOMKilled, image pull errors)
// fail the attempt instead of hanging.
//
// Configuration (environment of the scheduler):
//
//	DAGSFLOW_K8S_NAMESPACE     namespace for task pods (default: the scheduler's own, or "default")
//	DAGSFLOW_K8S_POD_TEMPLATE  pod template file (YAML/JSON); the container named "base" (or the first) runs the task
//	DAGSFLOW_K8S_IMAGE         image when there is no template (must contain dagsflow-go and the DAG folder)
//	DAGSFLOW_K8S_ENV_SECRET    secret with DAGSFLOW_DATABASE_URL etc. for the default template (default "dagsflow-env")
//	DAGSFLOW_K8S_COMMAND       dagsflow-go binary in the image (default "dagsflow-go")
//	DAGSFLOW_K8S_DELETE_POD    success (default) | always | never
//	DAGSFLOW_K8S_API_URL, DAGSFLOW_K8S_TOKEN, DAGSFLOW_K8S_CA_FILE: API access outside the cluster
//	(in-cluster the service account is used)
type KubernetesExecutor struct {
	QueueExecutor
	api       string
	token     string
	client    *http.Client
	namespace string
	template  map[string]any
	command   string
	deleteOn  string
}

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

func NewKubernetesExecutor(st *Store) (*KubernetesExecutor, error) {
	k := &KubernetesExecutor{command: envOr("DAGSFLOW_K8S_COMMAND", "dagsflow-go"), deleteOn: envOr("DAGSFLOW_K8S_DELETE_POD", "success")}
	k.QueueExecutor = QueueExecutor{Store: st, QueuePrefix: "kubernetes/", launch: k.launchPod, check: k.checkPod, cleanup: k.cleanupPod, handle: k.podName}

	caFile := os.Getenv("DAGSFLOW_K8S_CA_FILE")
	switch {
	case os.Getenv("DAGSFLOW_K8S_API_URL") != "":
		k.api = strings.TrimRight(os.Getenv("DAGSFLOW_K8S_API_URL"), "/")
		k.token = os.Getenv("DAGSFLOW_K8S_TOKEN")
	case os.Getenv("KUBERNETES_SERVICE_HOST") != "":
		k.api = "https://" + os.Getenv("KUBERNETES_SERVICE_HOST") + ":" + envOr("KUBERNETES_SERVICE_PORT", "443")
		tok, err := os.ReadFile(saDir + "/token")
		if err != nil {
			return nil, fmt.Errorf("kubernetes executor: read service account token: %w", err)
		}
		k.token = strings.TrimSpace(string(tok))
		if caFile == "" {
			caFile = saDir + "/ca.crt"
		}
	default:
		return nil, fmt.Errorf("kubernetes executor: not running in a cluster; set DAGSFLOW_K8S_API_URL (and DAGSFLOW_K8S_TOKEN)")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("kubernetes executor: CA: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	k.client = &http.Client{Transport: tr, Timeout: 30 * time.Second}

	k.namespace = os.Getenv("DAGSFLOW_K8S_NAMESPACE")
	if k.namespace == "" {
		if b, err := os.ReadFile(saDir + "/namespace"); err == nil {
			k.namespace = strings.TrimSpace(string(b))
		} else {
			k.namespace = "default"
		}
	}
	if p := os.Getenv("DAGSFLOW_K8S_POD_TEMPLATE"); p != "" {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("kubernetes executor: pod template: %w", err)
		}
		var raw any
		if err := yaml.Unmarshal(b, &raw); err != nil {
			return nil, fmt.Errorf("kubernetes executor: pod template %s: %w", p, err)
		}
		if err := jsonNormalize(raw, &k.template); err != nil {
			return nil, fmt.Errorf("kubernetes executor: pod template %s: %w", p, err)
		}
	} else {
		img := os.Getenv("DAGSFLOW_K8S_IMAGE")
		if img == "" {
			return nil, fmt.Errorf("kubernetes executor: set DAGSFLOW_K8S_POD_TEMPLATE or DAGSFLOW_K8S_IMAGE")
		}
		k.template = map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"spec": map[string]any{
				"containers": []any{map[string]any{
					"name": "base", "image": img,
					"envFrom": []any{map[string]any{"secretRef": map[string]any{"name": envOr("DAGSFLOW_K8S_ENV_SECRET", "dagsflow-env")}}},
				}},
			},
		}
	}
	return k, nil
}

func (k *KubernetesExecutor) Name() string { return "kubernetes" }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func jsonNormalize(in any, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

var labelBad = regexp.MustCompile(`[^a-z0-9-]+`)

// dnsLabel makes s usable in a pod name / label value.
func dnsLabel(s string, max int) string {
	s = strings.Trim(labelBad.ReplaceAllString(strings.ToLower(s), "-"), "-")
	if len(s) > max {
		s = strings.Trim(s[:max], "-")
	}
	if s == "" {
		s = "x"
	}
	return s
}

func labelValue(s string) string {
	v := regexp.MustCompile(`[^A-Za-z0-9._-]+`).ReplaceAllString(s, "_")
	if len(v) > 63 {
		v = v[:63]
	}
	return strings.Trim(v, "._-")
}

// deepMerge merges override into base (maps recursively; containers by name).
func deepMerge(base, override map[string]any) map[string]any {
	for k, ov := range override {
		bv, ok := base[k]
		switch o := ov.(type) {
		case map[string]any:
			if b, isMap := bv.(map[string]any); ok && isMap {
				base[k] = deepMerge(b, o)
				continue
			}
		case []any:
			if b, isList := bv.([]any); ok && isList && (k == "containers" || k == "initContainers") {
				base[k] = mergeByName(b, o)
				continue
			}
		}
		base[k] = ov
	}
	return base
}

func mergeByName(base, over []any) []any {
	out := append([]any(nil), base...)
	for _, o := range over {
		om, _ := o.(map[string]any)
		matched := false
		for i, b := range out {
			if bm, _ := b.(map[string]any); bm != nil && om != nil && bm["name"] == om["name"] {
				out[i] = deepMerge(bm, om)
				matched = true
				break
			}
		}
		if !matched {
			out = append(out, o)
		}
	}
	return out
}

// PodFor builds the pod manifest of a job (exported for `tasks render-pod`).
func (k *KubernetesExecutor) PodFor(t *dag.Task, id int64, p JobPayload) (map[string]any, error) {
	var pod map[string]any
	if err := jsonNormalize(k.template, &pod); err != nil { // deep copy
		return nil, err
	}
	if po := t.PodOverride(); po != nil {
		var o map[string]any
		if err := jsonNormalize(po, &o); err != nil {
			return nil, err
		}
		pod = deepMerge(pod, o)
	}
	pod["apiVersion"], pod["kind"] = "v1", "Pod"
	meta, _ := pod["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	name := podName(p.DagID, p.TaskID, id)
	meta["name"] = name
	delete(meta, "generateName")
	labels, _ := meta["labels"].(map[string]any)
	if labels == nil {
		labels = map[string]any{}
	}
	labels["app.kubernetes.io/managed-by"] = "dagsflow"
	labels["dagsflow/dag-id"] = labelValue(p.DagID)
	labels["dagsflow/task-id"] = labelValue(p.TaskID)
	labels["dagsflow/try-number"] = strconv.Itoa(p.Try)
	labels["dagsflow/map-index"] = strconv.Itoa(p.MapIndex)
	labels["dagsflow/job-id"] = strconv.FormatInt(id, 10)
	meta["labels"] = labels
	ann, _ := meta["annotations"].(map[string]any)
	if ann == nil {
		ann = map[string]any{}
	}
	ann["dagsflow/run-id"] = p.RunID
	meta["annotations"] = ann
	pod["metadata"] = meta

	spec, _ := pod["spec"].(map[string]any)
	if spec == nil {
		return nil, fmt.Errorf("pod template has no spec")
	}
	spec["restartPolicy"] = "Never"
	cs, _ := spec["containers"].([]any)
	if len(cs) == 0 {
		return nil, fmt.Errorf("pod template has no containers")
	}
	idx := 0
	for i, c := range cs {
		if cm, _ := c.(map[string]any); cm != nil && cm["name"] == "base" {
			idx = i
		}
	}
	c, _ := cs[idx].(map[string]any)
	if c == nil {
		return nil, fmt.Errorf("invalid container in pod template")
	}
	c["command"] = []any{k.command}
	c["args"] = []any{"worker", "--job-id", strconv.FormatInt(id, 10)}
	return pod, nil
}

func podName(dagID, taskID string, id int64) string {
	return fmt.Sprintf("%s-%d", dnsLabel("df-"+dagID+"-"+taskID, 45), id)
}

func (k *KubernetesExecutor) podName(t *dag.Task, id int64) (string, error) {
	return podName(t.DAG().ID, t.ID, id), nil
}

func (k *KubernetesExecutor) do(ctx context.Context, method, path string, body any) (map[string]any, int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, k.api+path, rd)
	if err != nil {
		return nil, 0, err
	}
	if k.token != "" {
		req.Header.Set("Authorization", "Bearer "+k.token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	if resp.StatusCode >= 300 {
		msg, _ := out["message"].(string)
		if msg == "" {
			msg = strings.TrimSpace(string(b))
		}
		return out, resp.StatusCode, fmt.Errorf("kubernetes %s %s: %d %s", method, path, resp.StatusCode, msg)
	}
	return out, resp.StatusCode, nil
}

func (k *KubernetesExecutor) podPath(name string) string {
	return "/api/v1/namespaces/" + k.namespace + "/pods" + map[bool]string{true: "", false: "/" + name}[name == ""]
}

func (k *KubernetesExecutor) launchPod(ctx context.Context, t *dag.Task, id int64) (string, error) {
	j, err := k.Store.be().getJob(id)
	if err != nil {
		return "", err
	}
	pod, err := k.PodFor(t, id, j.Payload)
	if err != nil {
		return "", fmt.Errorf("build pod: %w", err)
	}
	cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, _, err := k.do(cctx, http.MethodPost, k.podPath(""), pod); err != nil {
		return "", fmt.Errorf("create pod: %w", err)
	}
	name := pod["metadata"].(map[string]any)["name"].(string)
	return name, nil
}

var fatalWaiting = map[string]bool{"ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true,
	"CreateContainerConfigError": true, "CreateContainerError": true}

func (k *KubernetesExecutor) checkPod(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pod, code, err := k.do(ctx, http.MethodGet, k.podPath(name), nil)
	if code == http.StatusNotFound {
		return fmt.Errorf("pod %s was deleted before the task finished", name)
	}
	if err != nil {
		metricErrors.inc("k8s_get_pod")
		return nil // API hiccup: keep waiting
	}
	status, _ := pod["status"].(map[string]any)
	phase, _ := status["phase"].(string)
	detail := ""
	cstats, _ := status["containerStatuses"].([]any)
	for _, c := range cstats {
		cm, _ := c.(map[string]any)
		st, _ := cm["state"].(map[string]any)
		if term, _ := st["terminated"].(map[string]any); term != nil {
			detail = fmt.Sprintf(" (%v, exit code %v)", term["reason"], term["exitCode"])
		}
		if wait, _ := st["waiting"].(map[string]any); wait != nil {
			reason, _ := wait["reason"].(string)
			if fatalWaiting[reason] {
				meta, _ := pod["metadata"].(map[string]any)
				created, _ := time.Parse(time.RFC3339, fmt.Sprint(meta["creationTimestamp"]))
				if time.Since(created) > 2*time.Minute {
					return fmt.Errorf("pod %s cannot start: %s: %v", name, reason, wait["message"])
				}
			}
		}
	}
	switch phase {
	case "Failed":
		return fmt.Errorf("pod %s failed%s", name, detail)
	case "Succeeded":
		return fmt.Errorf("pod %s exited without reporting a result%s", name, detail)
	}
	return nil
}

func (k *KubernetesExecutor) cleanupPod(name string, success bool) {
	if k.deleteOn == "never" || (k.deleteOn == "success" && !success) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, code, err := k.do(ctx, http.MethodDelete, k.podPath(name), nil); err != nil && code != http.StatusNotFound {
		metricErrors.inc("k8s_delete_pod")
	}
}
