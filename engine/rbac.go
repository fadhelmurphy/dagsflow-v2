package engine

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"dagsflow-go/dag"
)

// RBAC ~ Airflow's roles & permissions: a role is a set of permissions
// (action + resource); a user has one or more roles. DAG permissions can be
// limited to specific DAGs: resource "*" (all), a DAG id, a glob
// ("sales_*") or a tag ("tag:finance").

// Actions.
const (
	ActDagRead     = "dag.read"    // view DAG, runs, task instances, logs, XCom, code
	ActDagEdit     = "dag.edit"    // pause, clear, mark state, notes, reparse
	ActDagTrigger  = "dag.trigger" // trigger runs
	ActDagDelete   = "dag.delete"  // delete runs / DAG metadata
	ActPoolsRead   = "pools.read"
	ActPoolsEdit   = "pools.edit"
	ActVarsRead    = "variables.read"
	ActVarsEdit    = "variables.edit"
	ActConnsRead   = "connections.read"
	ActConnsEdit   = "connections.edit"
	ActEventsRead  = "events.read"   // event log (entries of readable DAGs)
	ActDatasets    = "datasets.read" // datasets page
	ActSystemRead  = "system.read"   // import errors, components
	ActUsersManage = "users.manage"
	ActRolesManage = "roles.manage"
	ActBuilderEdit = "builder.edit" // visual builder: create/change/delete YAML workflows
	ActCatalogRead = "catalog.read" // data catalog; resource: dataset ids ("*", "bigquery:proj.mart.*")
)

// ActionInfo describes an action (for the role editor).
type ActionInfo struct {
	Name        string `json:"action"`
	Description string `json:"description"`
	DagScoped   bool   `json:"dag_scoped"`
	// DatasetScoped: the resource is a pattern of catalog dataset ids
	DatasetScoped bool `json:"dataset_scoped,omitempty"`
}

var Actions = []ActionInfo{
	{Name: ActDagRead, Description: "View DAGs, runs, task instances, logs, XComs, code", DagScoped: true},
	{Name: ActDagEdit, Description: "Pause/unpause, clear, mark state, notes, reparse", DagScoped: true},
	{Name: ActDagTrigger, Description: "Trigger DAG runs", DagScoped: true},
	{Name: ActDagDelete, Description: "Delete DAG runs and DAG metadata", DagScoped: true},
	{Name: ActPoolsRead, Description: "View pools", DagScoped: false},
	{Name: ActPoolsEdit, Description: "Create, change and delete pools", DagScoped: false},
	{Name: ActVarsRead, Description: "View variables", DagScoped: false},
	{Name: ActVarsEdit, Description: "Create, change and delete variables", DagScoped: false},
	{Name: ActConnsRead, Description: "View connections (secrets masked)", DagScoped: false},
	{Name: ActConnsEdit, Description: "Create, change and delete connections", DagScoped: false},
	{Name: ActEventsRead, Description: "View the event log", DagScoped: false},
	{Name: ActDatasets, Description: "View datasets", DagScoped: false},
	{Name: ActSystemRead, Description: "View import errors and components", DagScoped: false},
	{Name: ActUsersManage, Description: "Manage users and API tokens", DagScoped: false},
	{Name: ActRolesManage, Description: "Manage roles and permissions", DagScoped: false},
	{Name: ActBuilderEdit, Description: "Use the visual builder (writes workflow files, can run shell commands and SQL: treat like write access to DAG code)", DagScoped: false},
	{Name: ActCatalogRead, Description: "View the data catalog: tables, columns and lineage of the datasets matching the pattern", DatasetScoped: true},
}

func actionInfo(name string) (ActionInfo, bool) {
	for _, a := range Actions {
		if a.Name == name {
			return a, true
		}
	}
	return ActionInfo{}, false
}

// implied: holding the key also grants these (on the same DAGs).
var implied = map[string][]string{
	ActDagEdit: {ActDagRead}, ActDagTrigger: {ActDagRead}, ActDagDelete: {ActDagRead},
	ActPoolsEdit: {ActPoolsRead}, ActVarsEdit: {ActVarsRead}, ActConnsEdit: {ActConnsRead},
}

// Permission: Action on Resource (DAG actions only; "*" = every DAG).
type Permission struct {
	Action   string `json:"action"`
	Resource string `json:"resource,omitempty"`
}

func (p Permission) String() string {
	if p.Resource == "" {
		return p.Action
	}
	return p.Action + ":" + p.Resource
}

// ParsePermission reads "dag.read:sales_*", "dag.edit:tag:finance", "pools.read".
func ParsePermission(s string) (Permission, error) {
	s = strings.TrimSpace(s)
	act, res, _ := strings.Cut(s, ":")
	p := Permission{Action: act, Resource: res}
	return p, p.validate()
}

func (p *Permission) validate() error {
	info, ok := actionInfo(p.Action)
	if !ok {
		names := make([]string, len(Actions))
		for i, a := range Actions {
			names[i] = a.Name
		}
		return fmt.Errorf("unknown action %q (one of: %s)", p.Action, strings.Join(names, ", "))
	}
	p.Resource = strings.TrimSpace(p.Resource)
	if info.DatasetScoped {
		if p.Resource == "" {
			p.Resource = "*"
		}
		return nil
	}
	if !info.DagScoped {
		if p.Resource != "" {
			return fmt.Errorf("%s is not DAG-scoped: drop %q", p.Action, p.Resource)
		}
		return nil
	}
	if p.Resource == "" {
		p.Resource = "*"
	}
	if strings.HasPrefix(p.Resource, "tag:") {
		if strings.TrimPrefix(p.Resource, "tag:") == "" {
			return errors.New("tag: needs a tag name")
		}
		return nil
	}
	if _, err := path.Match(p.Resource, "x"); err != nil {
		return fmt.Errorf("invalid DAG pattern %q: %v", p.Resource, err)
	}
	return nil
}

// matchesDAG: resource "*", a glob on the DAG id, or "tag:<tag>".
func (p Permission) matchesDAG(dagID string, tags []string) bool {
	if p.Resource == "*" || p.Resource == "" {
		return true
	}
	if t, ok := strings.CutPrefix(p.Resource, "tag:"); ok {
		for _, x := range tags {
			if x == t {
				return true
			}
		}
		return false
	}
	ok, _ := path.Match(p.Resource, dagID)
	return ok
}

// RoleDef is a named set of permissions.
type RoleDef struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Permissions []Permission `json:"permissions"`
	Builtin     bool         `json:"builtin,omitempty"`
	CreatedAt   *time.Time   `json:"created_at,omitempty"`
	UpdatedAt   *time.Time   `json:"updated_at,omitempty"`
}

func all(acts ...string) []Permission {
	var out []Permission
	for _, a := range acts {
		p := Permission{Action: a}
		if info, _ := actionInfo(a); info.DagScoped || info.DatasetScoped {
			p.Resource = "*"
		}
		out = append(out, p)
	}
	return out
}

// builtinRoles: the roles every install has (not editable).
func builtinRoles() map[string]*RoleDef {
	viewer := []string{ActDagRead, ActPoolsRead, ActEventsRead, ActDatasets, ActSystemRead, ActCatalogRead}
	user := append(append([]string{}, viewer...), ActDagEdit, ActDagTrigger)
	op := append(append([]string{}, user...), ActDagDelete, ActPoolsEdit, ActVarsEdit, ActConnsEdit)
	admin := append(append([]string{}, op...), ActUsersManage, ActRolesManage, ActBuilderEdit)
	return map[string]*RoleDef{
		string(RoleViewer): {Name: string(RoleViewer), Description: "Read-only access to every DAG", Permissions: all(viewer...), Builtin: true},
		string(RoleUser):   {Name: string(RoleUser), Description: "Viewer + trigger, pause, clear and mark every DAG", Permissions: all(user...), Builtin: true},
		string(RoleOp):     {Name: string(RoleOp), Description: "User + delete runs, pools, variables, connections", Permissions: all(op...), Builtin: true},
		string(RoleAdmin):  {Name: string(RoleAdmin), Description: "Everything, including users and roles", Permissions: all(admin...), Builtin: true},
	}
}

// Roles returns the built-in roles plus the custom ones.
func (s *Store) Roles() (map[string]*RoleDef, error) {
	m := map[string]*RoleDef{}
	if err := s.readTable("roles", &m); err != nil {
		return nil, err
	}
	for name, r := range builtinRoles() {
		m[name] = r
	}
	return m, nil
}

// SortedRoles: built-in roles first (by power), then custom roles by name.
func SortedRoles(m map[string]*RoleDef) []*RoleDef {
	order := map[string]int{string(RoleViewer): 1, string(RoleUser): 2, string(RoleOp): 3, string(RoleAdmin): 4}
	out := make([]*RoleDef, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		oi, oj := order[out[i].Name], order[out[j].Name]
		if (oi == 0) != (oj == 0) {
			return oi != 0
		}
		if oi != oj {
			return oi < oj
		}
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})
	return out
}

func normalizeRole(def *RoleDef) error {
	def.Name = strings.TrimSpace(def.Name)
	if def.Name == "" || len(def.Name) > 64 || strings.ContainsAny(def.Name, ",/\\\"'<>") {
		return errors.New("role name is required (max 64 characters, no , / \\ \" ' < >)")
	}
	if _, builtin := builtinRoles()[def.Name]; builtin {
		return fmt.Errorf("%s is a built-in role and cannot be changed (create a custom role instead)", def.Name)
	}
	seen := map[string]bool{}
	var perms []Permission
	for _, p := range def.Permissions {
		if err := p.validate(); err != nil {
			return err
		}
		if !seen[p.String()] {
			seen[p.String()] = true
			perms = append(perms, p)
		}
	}
	sort.Slice(perms, func(i, j int) bool { return perms[i].String() < perms[j].String() })
	def.Permissions = perms
	def.Builtin = false
	return nil
}

// CreateRole adds a custom role.
func (s *Store) CreateRole(def RoleDef) error {
	if err := normalizeRole(&def); err != nil {
		return err
	}
	m := map[string]*RoleDef{}
	return s.updateTable("roles", &m, func() error {
		for name := range m {
			if strings.EqualFold(name, def.Name) {
				return fmt.Errorf("role %q already exists", name)
			}
		}
		now := time.Now()
		def.CreatedAt, def.UpdatedAt = &now, &now
		m[def.Name] = &def
		return nil
	})
}

// UpdateRole replaces the description and permissions of a custom role.
func (s *Store) UpdateRole(name string, def RoleDef) error {
	def.Name = name
	if err := normalizeRole(&def); err != nil {
		return err
	}
	m := map[string]*RoleDef{}
	return s.updateTable("roles", &m, func() error {
		old, ok := m[name]
		if !ok {
			return fmt.Errorf("role %q not found", name)
		}
		now := time.Now()
		def.CreatedAt, def.UpdatedAt = old.CreatedAt, &now
		m[name] = &def
		return nil
	})
}

// DeleteRole removes a custom role that no user has.
func (s *Store) DeleteRole(name string) error {
	if _, builtin := builtinRoles()[name]; builtin {
		return fmt.Errorf("%s is a built-in role and cannot be deleted", name)
	}
	users, err := s.Users()
	if err != nil {
		return err
	}
	var holders []string
	for _, u := range users {
		for _, r := range u.RoleNames() {
			if r == name {
				holders = append(holders, u.Username)
			}
		}
	}
	if len(holders) > 0 {
		sort.Strings(holders)
		return fmt.Errorf("role %q is still assigned to: %s", name, strings.Join(holders, ", "))
	}
	m := map[string]*RoleDef{}
	return s.updateTable("roles", &m, func() error {
		if _, ok := m[name]; !ok {
			return fmt.Errorf("role %q not found", name)
		}
		delete(m, name)
		return nil
	})
}

// ValidateRoles checks that every role exists (returns them deduplicated).
func (s *Store) ValidateRoles(names []string) ([]string, error) {
	roles, err := s.Roles()
	if err != nil {
		return nil, err
	}
	var out []string
	seen := map[string]bool{}
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" || seen[n] {
			continue
		}
		if roles[n] == nil {
			return nil, fmt.Errorf("unknown role %q", n)
		}
		seen[n] = true
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("at least one role is required")
	}
	return out, nil
}

// Access is what a user may do (the union of their roles' permissions).
type Access struct {
	Roles []string
	perms []Permission
	all   bool
}

// FullAccess is used when auth is off (no users defined).
func FullAccess() *Access { return &Access{all: true, Roles: []string{string(RoleAdmin)}} }

// AccessFor resolves a user's roles into permissions. Roles that no longer
// exist are ignored.
func AccessFor(u *User, roles map[string]*RoleDef) *Access {
	a := &Access{Roles: u.RoleNames()}
	for _, name := range a.Roles {
		if r := roles[name]; r != nil {
			for _, p := range r.Permissions {
				a.perms = append(a.perms, p)
				for _, imp := range implied[p.Action] {
					a.perms = append(a.perms, Permission{Action: imp, Resource: p.Resource})
				}
			}
		}
	}
	return a
}

// Can: a global action, or a DAG action on at least one DAG.
func (a *Access) Can(action string) bool {
	if a.all {
		return true
	}
	for _, p := range a.perms {
		if p.Action == action {
			return true
		}
	}
	return false
}

// CanDAG: a DAG action on this DAG.
func (a *Access) CanDAG(action, dagID string, tags []string) bool {
	if a.all {
		return true
	}
	for _, p := range a.perms {
		if p.Action == action && p.matchesDAG(dagID, tags) {
			return true
		}
	}
	return false
}

// CanDataset: the catalog dataset id is visible. Rows passed between nodes
// (task:<dag>.<task>) follow dag.read on their DAG.
func (a *Access) CanDataset(id string) bool {
	if a.all {
		return true
	}
	if rest, ok := strings.CutPrefix(id, "task:"); ok {
		dagID, _, _ := strings.Cut(rest, ".")
		return a.Can(ActCatalogRead) && a.DAG(ActDagRead, dagID)
	}
	for _, p := range a.perms {
		if p.Action == ActCatalogRead && globMatch(p.Resource, id) {
			return true
		}
	}
	return false
}

// globMatch: * is any run of characters (slashes and dots too), ? one
// character; case-insensitive.
func globMatch(pattern, s string) bool {
	if pattern == "*" || pattern == "" {
		return true
	}
	p, t := []rune(strings.ToLower(pattern)), []rune(strings.ToLower(s))
	star, mark := -1, 0
	i, j := 0, 0
	for j < len(t) {
		switch {
		case i < len(p) && (p[i] == '?' || p[i] == t[j]):
			i++
			j++
		case i < len(p) && p[i] == '*':
			star, mark = i, j
			i++
		case star >= 0:
			i = star + 1
			mark++
			j = mark
		default:
			return false
		}
	}
	for i < len(p) && p[i] == '*' {
		i++
	}
	return i == len(p)
}

// DAG is CanDAG for a loaded DAG (tags known); unknown DAGs match by id only.
func (a *Access) DAG(action, dagID string) bool {
	var tags []string
	if d, ok := dag.Get(dagID); ok {
		tags = d.Tags
	}
	return a.CanDAG(action, dagID, tags)
}

// DagPermissions: what the user may do on one DAG (for the UI).
func (a *Access) DagPermissions(dagID string) map[string]bool {
	return map[string]bool{
		"read": a.DAG(ActDagRead, dagID), "edit": a.DAG(ActDagEdit, dagID),
		"trigger": a.DAG(ActDagTrigger, dagID), "delete": a.DAG(ActDagDelete, dagID),
	}
}

// Global: the non-DAG actions the user has (for the UI).
func (a *Access) Global() map[string]bool {
	out := map[string]bool{}
	for _, info := range Actions {
		if !info.DagScoped { // dataset-scoped: true when some datasets are visible
			out[info.Name] = a.Can(info.Name)
		}
	}
	return out
}

// Permissions lists the effective permissions (including implied reads).
func (a *Access) Permissions() []string {
	if a.all {
		return []string{"*"}
	}
	seen := map[string]bool{}
	var out []string
	for _, p := range a.perms {
		if !seen[p.String()] {
			seen[p.String()] = true
			out = append(out, p.String())
		}
	}
	sort.Strings(out)
	return out
}
