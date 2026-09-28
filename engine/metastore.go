package engine

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"dagsflow-go/dag"
)

// Small JSON "tables" next to the runs. A process-wide mutex serialises
// read-modify-write within one process; cross-process writes are last-writer-wins.
var tableMu sync.Mutex

func (s *Store) tablePath(name string) string { return filepath.Join(s.Home, "state", name+".json") }

func (s *Store) readTable(name string, v any) error {
	err := readJSON(s.tablePath(name), v)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *Store) updateTable(name string, v any, fn func() error) error {
	tableMu.Lock()
	defer tableMu.Unlock()
	if err := s.readTable(name, v); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return writeJSONAtomic(s.tablePath(name), v)
}

// ---- encryption (Airflow's fernet_key) ----------------------------------------

// GenerateKey returns a new base64 key for DAGSFLOW_FERNET_KEY.
func GenerateKey() string {
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	return base64.StdEncoding.EncodeToString(k)
}

func cipherFromEnv() (cipher.AEAD, error) {
	raw := os.Getenv("DAGSFLOW_FERNET_KEY")
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(key) != 32 {
		return nil, errors.New("DAGSFLOW_FERNET_KEY must be 32 bytes, base64 (use `dagsflow-go config gen-key`)")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// EncryptionEnabled reports whether secrets are stored encrypted.
func EncryptionEnabled() bool { a, err := cipherFromEnv(); return a != nil && err == nil }

type secretValue struct {
	Encrypted bool   `json:"encrypted"`
	Value     string `json:"value"`
}

func seal(plain string) (secretValue, error) {
	aead, err := cipherFromEnv()
	if err != nil {
		return secretValue{}, err
	}
	if aead == nil {
		return secretValue{Value: plain}, nil
	}
	nonce := make([]byte, aead.NonceSize())
	_, _ = rand.Read(nonce)
	ct := aead.Seal(nonce, nonce, []byte(plain), nil)
	return secretValue{Encrypted: true, Value: base64.StdEncoding.EncodeToString(ct)}, nil
}

func unseal(v secretValue) (string, error) {
	if !v.Encrypted {
		return v.Value, nil
	}
	aead, err := cipherFromEnv()
	if err != nil {
		return "", err
	}
	if aead == nil {
		return "", errors.New("value is encrypted but DAGSFLOW_FERNET_KEY is not set")
	}
	ct, err := base64.StdEncoding.DecodeString(v.Value)
	if err != nil || len(ct) < aead.NonceSize() {
		return "", errors.New("corrupt encrypted value")
	}
	pt, err := aead.Open(nil, ct[:aead.NonceSize()], ct[aead.NonceSize():], nil)
	if err != nil {
		return "", errors.New("cannot decrypt: wrong DAGSFLOW_FERNET_KEY?")
	}
	return string(pt), nil
}

// ---- variables ------------------------------------------------------------------

type Variable struct {
	Key         string      `json:"key"`
	Secret      secretValue `json:"secret"`
	Description string      `json:"description,omitempty"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

func (s *Store) SetVariable(key, value, description string) error {
	sv, err := seal(value)
	if err != nil {
		return err
	}
	m := map[string]*Variable{}
	return s.updateTable("variables", &m, func() error {
		m[key] = &Variable{Key: key, Secret: sv, Description: description, UpdatedAt: time.Now()}
		return nil
	})
}

func (s *Store) DeleteVariable(key string) error {
	m := map[string]*Variable{}
	return s.updateTable("variables", &m, func() error {
		if _, ok := m[key]; !ok {
			return fmt.Errorf("variable %q not found", key)
		}
		delete(m, key)
		return nil
	})
}

func (s *Store) Variables() ([]*Variable, error) {
	m := map[string]*Variable{}
	if err := s.readTable("variables", &m); err != nil {
		return nil, err
	}
	out := make([]*Variable, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (v *Variable) Value() (string, error) { return unseal(v.Secret) }

// ---- connections ------------------------------------------------------------------

type StoredConnection struct {
	ID          string      `json:"conn_id"`
	Type        string      `json:"conn_type"`
	Secret      secretValue `json:"config"` // JSON of Config, encrypted when a key is set
	Description string      `json:"description,omitempty"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

func (s *Store) SetConnection(id string, c dag.Connection, description string) error {
	b, err := json.Marshal(c.Config)
	if err != nil {
		return err
	}
	sv, err := seal(string(b))
	if err != nil {
		return err
	}
	m := map[string]*StoredConnection{}
	return s.updateTable("connections", &m, func() error {
		m[id] = &StoredConnection{ID: id, Type: c.Type, Secret: sv, Description: description, UpdatedAt: time.Now()}
		return nil
	})
}

func (s *Store) DeleteConnection(id string) error {
	m := map[string]*StoredConnection{}
	return s.updateTable("connections", &m, func() error {
		if _, ok := m[id]; !ok {
			return fmt.Errorf("connection %q not found", id)
		}
		delete(m, id)
		return nil
	})
}

func (s *Store) Connections() ([]*StoredConnection, error) {
	m := map[string]*StoredConnection{}
	if err := s.readTable("connections", &m); err != nil {
		return nil, err
	}
	out := make([]*StoredConnection, 0, len(m))
	for _, c := range m {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (c *StoredConnection) Decode() (dag.Connection, error) {
	raw, err := unseal(c.Secret)
	if err != nil {
		return dag.Connection{}, err
	}
	conn := dag.Connection{Type: c.Type, Config: map[string]any{}}
	return conn, json.Unmarshal([]byte(raw), &conn.Config)
}

// MetastoreBackend exposes variables/connections of the store as a secrets backend.
type MetastoreBackend struct{ Store *Store }

func (MetastoreBackend) Name() string { return "metastore" }

func (b MetastoreBackend) GetVariable(key string) (string, bool, error) {
	vars, err := b.Store.Variables()
	if err != nil {
		return "", false, err
	}
	for _, v := range vars {
		if v.Key == key {
			val, err := v.Value()
			return val, err == nil, err
		}
	}
	return "", false, nil
}

func (b MetastoreBackend) GetConnection(id string) (*dag.Connection, bool, error) {
	conns, err := b.Store.Connections()
	if err != nil {
		return nil, false, err
	}
	for _, c := range conns {
		if c.ID == id {
			conn, err := c.Decode()
			return &conn, err == nil, err
		}
	}
	return nil, false, nil
}

// ---- pools --------------------------------------------------------------------------

const DefaultPoolSlots = 128

type Pool struct {
	Name        string `json:"name"`
	Slots       int    `json:"slots"`
	Description string `json:"description,omitempty"`
}

func (s *Store) Pools() (map[string]*Pool, error) {
	m := map[string]*Pool{}
	if err := s.readTable("pools", &m); err != nil {
		return nil, err
	}
	if _, ok := m[dag.DefaultPool]; !ok {
		m[dag.DefaultPool] = &Pool{Name: dag.DefaultPool, Slots: DefaultPoolSlots, Description: "Default pool"}
	}
	return m, nil
}

func (s *Store) SetPool(name string, slots int, description string) error {
	if slots < 0 {
		return errors.New("slots must be >= 0")
	}
	m := map[string]*Pool{}
	return s.updateTable("pools", &m, func() error {
		m[name] = &Pool{Name: name, Slots: slots, Description: description}
		return nil
	})
}

func (s *Store) DeletePool(name string) error {
	if name == dag.DefaultPool {
		return errors.New("default_pool cannot be deleted")
	}
	m := map[string]*Pool{}
	return s.updateTable("pools", &m, func() error {
		if _, ok := m[name]; !ok {
			return fmt.Errorf("pool %q not found", name)
		}
		delete(m, name)
		return nil
	})
}

// ---- datasets -----------------------------------------------------------------------

type DatasetEvent struct {
	Timestamp   time.Time      `json:"timestamp"`
	SourceDag   string         `json:"source_dag_id"`
	SourceTask  string         `json:"source_task_id"`
	SourceRunID string         `json:"source_run_id"`
	Extra       map[string]any `json:"extra,omitempty"`
}

type Dataset struct {
	URI         string         `json:"uri"`
	LastUpdated time.Time      `json:"last_updated"`
	Events      []DatasetEvent `json:"events"` // newest last, capped
}

func (s *Store) AddDatasetEvent(uri string, ev DatasetEvent) error {
	m := map[string]*Dataset{}
	return s.updateTable("datasets", &m, func() error {
		d := m[uri]
		if d == nil {
			d = &Dataset{URI: uri}
			m[uri] = d
		}
		d.LastUpdated = ev.Timestamp
		d.Events = append(d.Events, ev)
		if len(d.Events) > 50 {
			d.Events = d.Events[len(d.Events)-50:]
		}
		return nil
	})
}

func (s *Store) Datasets() (map[string]*Dataset, error) {
	m := map[string]*Dataset{}
	return m, s.readTable("datasets", &m)
}

// ---- users & RBAC ------------------------------------------------------------------

type Role string

const (
	RoleViewer Role = "Viewer" // read only
	RoleUser   Role = "User"   // + trigger, pause, clear
	RoleOp     Role = "Op"     // + variables, connections, pools
	RoleAdmin  Role = "Admin"  // + users
)

func (r Role) Level() int {
	return map[Role]int{RoleViewer: 1, RoleUser: 2, RoleOp: 3, RoleAdmin: 4}[r]
}

func ParseRole(s string) (Role, error) {
	r := Role(s)
	if r.Level() == 0 {
		return "", fmt.Errorf("unknown role %q (Viewer, User, Op, Admin)", s)
	}
	return r, nil
}

type User struct {
	Username     string     `json:"username"`
	FirstName    string     `json:"first_name,omitempty"`
	LastName     string     `json:"last_name,omitempty"`
	Email        string     `json:"email,omitempty"`
	Role         Role       `json:"role"`
	Disabled     bool       `json:"disabled,omitempty"` // inactive users cannot sign in
	PasswordHash string     `json:"password_hash"`
	Tokens       []string   `json:"api_token_hashes,omitempty"` // sha256 of issued tokens
	CreatedAt    time.Time  `json:"created_at"`
	ChangedAt    *time.Time `json:"changed_at,omitempty"`
	LastLogin    *time.Time `json:"last_login,omitempty"`
	LoginCount   int        `json:"login_count,omitempty"`
}

func (u *User) Active() bool { return !u.Disabled }

// RolePermissions describes what each role may do (shown on Security → List Roles).
var RolePermissions = []struct {
	Role        Role     `json:"role"`
	Permissions []string `json:"permissions"`
}{
	{RoleViewer, []string{"View DAGs, runs, task instances, logs, XComs, code", "View pools, datasets, import errors, event log"}},
	{RoleUser, []string{"Everything Viewer can do", "Trigger DAGs, pause/unpause", "Clear tasks and runs, mark state as success/failed", "Reparse DAG files"}},
	{RoleOp, []string{"Everything User can do", "Manage pools, variables, connections", "Delete DAG runs and DAG metadata"}},
	{RoleAdmin, []string{"Everything Op can do", "Manage users, roles and API tokens"}},
}

// UserUpdate: nil fields are left unchanged.
type UserUpdate struct {
	FirstName, LastName, Email *string
	Role                       *Role
	Active                     *bool
	Password                   *string
}

func activeAdmins(m map[string]*User, except string) int {
	n := 0
	for name, u := range m {
		if name != except && u.Role == RoleAdmin && u.Active() {
			n++
		}
	}
	return n
}

func (s *Store) UpdateUser(username string, up UserUpdate) error {
	if up.Password != nil && len(*up.Password) < 8 {
		return errors.New("password must be at least 8 characters")
	}
	m := map[string]*User{}
	return s.updateTable("users", &m, func() error {
		u, ok := m[username]
		if !ok {
			return fmt.Errorf("user %q not found", username)
		}
		losesAdmin := (up.Role != nil && *up.Role != RoleAdmin) || (up.Active != nil && !*up.Active)
		if u.Role == RoleAdmin && u.Active() && losesAdmin && activeAdmins(m, username) == 0 {
			return errors.New("cannot demote or deactivate the last active Admin")
		}
		if up.FirstName != nil {
			u.FirstName = strings.TrimSpace(*up.FirstName)
		}
		if up.LastName != nil {
			u.LastName = strings.TrimSpace(*up.LastName)
		}
		if up.Email != nil {
			u.Email = strings.TrimSpace(*up.Email)
		}
		if up.Role != nil {
			u.Role = *up.Role
		}
		if up.Active != nil {
			u.Disabled = !*up.Active
		}
		if up.Password != nil {
			u.PasswordHash = hashPassword(*up.Password) // also invalidates web sessions
			u.Tokens = nil
		}
		now := time.Now()
		u.ChangedAt = &now
		return nil
	})
}

// RecordLogin updates last login / login count after a successful web sign-in.
func (s *Store) RecordLogin(username string) {
	m := map[string]*User{}
	_ = s.updateTable("users", &m, func() error {
		if u, ok := m[username]; ok {
			now := time.Now()
			u.LastLogin = &now
			u.LoginCount++
		}
		return nil
	})
}

// PBKDF2-HMAC-SHA256 (stdlib only), 210k iterations as recommended by OWASP.
func pbkdf2(pw, salt []byte, iter int) []byte {
	mac := hmac.New(sha256.New, pw)
	mac.Write(salt)
	mac.Write([]byte{0, 0, 0, 1})
	u := mac.Sum(nil)
	out := append([]byte(nil), u...)
	for i := 1; i < iter; i++ {
		mac.Reset()
		mac.Write(u)
		u = mac.Sum(u[:0])
		for j := range out {
			out[j] ^= u[j]
		}
	}
	return out
}

const pbkdf2Iter = 210000

func hashPassword(pw string) string {
	salt := make([]byte, 16)
	_, _ = rand.Read(salt)
	return fmt.Sprintf("pbkdf2-sha256$%d$%s$%s", pbkdf2Iter, hex.EncodeToString(salt), hex.EncodeToString(pbkdf2([]byte(pw), salt, pbkdf2Iter)))
}

func checkPassword(hash, pw string) bool {
	var iter int
	var saltHex, sumHex string
	if _, err := fmt.Sscanf(replaceDollar(hash), "pbkdf2-sha256 %d %s %s", &iter, &saltHex, &sumHex); err != nil {
		return false
	}
	salt, err1 := hex.DecodeString(saltHex)
	want, err2 := hex.DecodeString(sumHex)
	if err1 != nil || err2 != nil {
		return false
	}
	return subtle.ConstantTimeCompare(pbkdf2([]byte(pw), salt, iter), want) == 1
}

func replaceDollar(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] == '$' {
			b[i] = ' '
		}
	}
	return string(b)
}

func tokenHash(tok string) string { h := sha256.Sum256([]byte(tok)); return hex.EncodeToString(h[:]) }

func (s *Store) Users() (map[string]*User, error) {
	m := map[string]*User{}
	return m, s.readTable("users", &m)
}

func (s *Store) CreateUser(username, password string, role Role) error {
	if username == "" || len(password) < 8 {
		return errors.New("username required and password must be at least 8 characters")
	}
	m := map[string]*User{}
	return s.updateTable("users", &m, func() error {
		if _, ok := m[username]; ok {
			return fmt.Errorf("user %q already exists", username)
		}
		m[username] = &User{Username: username, Role: role, PasswordHash: hashPassword(password), CreatedAt: time.Now()}
		return nil
	})
}

func (s *Store) DeleteUser(username string) error {
	m := map[string]*User{}
	return s.updateTable("users", &m, func() error {
		u, ok := m[username]
		if !ok {
			return fmt.Errorf("user %q not found", username)
		}
		if u.Role == RoleAdmin && u.Active() && activeAdmins(m, username) == 0 {
			return errors.New("cannot delete the last active Admin")
		}
		delete(m, username)
		return nil
	})
}

// IssueToken creates an API token for the user; only its hash is stored.
func (s *Store) IssueToken(username string) (string, error) {
	raw := make([]byte, 24)
	_, _ = rand.Read(raw)
	tok := "dft_" + hex.EncodeToString(raw)
	m := map[string]*User{}
	return tok, s.updateTable("users", &m, func() error {
		u, ok := m[username]
		if !ok {
			return fmt.Errorf("user %q not found", username)
		}
		u.Tokens = append(u.Tokens, tokenHash(tok))
		return nil
	})
}

// Authenticate checks a password or (when password is "") an API token.
func (s *Store) Authenticate(username, password, token string) (*User, bool) {
	users, err := s.Users()
	if err != nil {
		return nil, false
	}
	if token != "" {
		h := tokenHash(token)
		for _, u := range users {
			for _, t := range u.Tokens {
				if subtle.ConstantTimeCompare([]byte(t), []byte(h)) == 1 {
					return u, u.Active()
				}
			}
		}
		return nil, false
	}
	u, ok := users[username]
	if !ok || !u.Active() || !checkPassword(u.PasswordHash, password) {
		return nil, false
	}
	return u, true
}
