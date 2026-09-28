package dag

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Connection ~ Airflow Connection (conn_id -> type + extras).
type Connection struct {
	Type   string         `json:"Type"`
	Config map[string]any `json:"Config"`
}

func (c Connection) String(key string) string {
	s, _ := c.Config[key].(string)
	return s
}

// SecretsBackend ~ Airflow secrets backend. Backends are consulted in order; the
// first one that has the key wins (Airflow: custom backend → env → metastore).
type SecretsBackend interface {
	Name() string
	GetVariable(key string) (string, bool, error)
	GetConnection(id string) (*Connection, bool, error)
}

var (
	backends  []SecretsBackend
	backendMu sync.RWMutex
)

func SetSecretsBackends(b ...SecretsBackend) {
	backendMu.Lock()
	defer backendMu.Unlock()
	backends = b
}

func SecretsBackends() []SecretsBackend {
	backendMu.RLock()
	defer backendMu.RUnlock()
	return append([]SecretsBackend(nil), backends...)
}

// GetVariable ~ Variable.get(key).
func GetVariable(key string) (string, error) {
	for _, b := range SecretsBackends() {
		v, ok, err := b.GetVariable(key)
		if err != nil {
			return "", fmt.Errorf("%s: %w", b.Name(), err)
		}
		if ok {
			return v, nil
		}
	}
	return "", fmt.Errorf("variable %q not found", key)
}

// GetVariableJSON ~ Variable.get(key, deserialize_json=True).
func GetVariableJSON[T any](key string) (T, error) {
	var out T
	s, err := GetVariable(key)
	if err != nil {
		return out, err
	}
	return out, json.Unmarshal([]byte(s), &out)
}

func GetConnection(id string) (Connection, bool) {
	for _, b := range SecretsBackends() {
		c, ok, err := b.GetConnection(id)
		if err == nil && ok {
			return *c, true
		}
	}
	return Connection{}, false
}

// ---- Env backend: DAGSFLOW_VAR_<KEY>, DAGSFLOW_CONN_<ID> (JSON) ---------------

type EnvBackend struct{}

func (EnvBackend) Name() string { return "env" }

func envKey(prefix, k string) string {
	return prefix + strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(k))
}

func (EnvBackend) GetVariable(key string) (string, bool, error) {
	v, ok := os.LookupEnv(envKey("DAGSFLOW_VAR_", key))
	return v, ok, nil
}

func (EnvBackend) GetConnection(id string) (*Connection, bool, error) {
	if id == "google_cloud_default" {
		if cred := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); cred != "" {
			return &Connection{Type: "google_cloud_platform", Config: map[string]any{
				"credentials_path": cred, "project_id": os.Getenv("GOOGLE_CLOUD_PROJECT")}}, true, nil
		}
	}
	raw, ok := os.LookupEnv(envKey("DAGSFLOW_CONN_", id))
	if !ok {
		return nil, false, nil
	}
	var c Connection
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, false, fmt.Errorf("DAGSFLOW_CONN_%s: %w", id, err)
	}
	return &c, true, nil
}

// ---- File backend: connections/*.json (plain text, for local dev) ------------

type FileBackend struct{ Dir string }

func (f FileBackend) Name() string                           { return "file:" + f.Dir }
func (FileBackend) GetVariable(string) (string, bool, error) { return "", false, nil }
func (f FileBackend) GetConnection(id string) (*Connection, bool, error) {
	entries, err := os.ReadDir(f.Dir)
	if err != nil {
		return nil, false, nil
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(f.Dir, e.Name()))
		if err != nil {
			continue
		}
		var m map[string]Connection
		if json.Unmarshal(raw, &m) == nil {
			if c, ok := m[id]; ok {
				return &c, true, nil
			}
		}
	}
	return nil, false, nil
}
