package dag

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// Custom builder nodes.
//
// A custom node type shows up in the builder palette next to the built-in
// ones. It can come from three places, all feeding the same registry:
//
//  1. a YAML file in <dags>/builder/nodes/*.yaml (hand written),
//  2. the builder UI ("Create node"), which writes that same YAML file,
//  3. Go code calling RegisterNodeType with a Build function.
//
// In a workflow file a custom node is stored as
//
//	- id: notify
//	  operator: custom
//	  node: slack_message
//	  with: {text: "done"}
//
// YAML-defined types expand into one built-in operator by substituting
// ${field} placeholders in a template. Go-defined types run their Build func.

// NodeBuilder adds the task of a Go-defined custom node to d. with holds the
// values of the node's fields; opts carry the common settings (retries, ...).
type NodeBuilder func(d *DAG, id string, with map[string]any, opts ...TaskOption) error

// CustomNodeDef is the YAML / JSON form of a custom node type.
type CustomNodeDef struct {
	Type        string         `yaml:"type" json:"type"`
	Label       string         `yaml:"label" json:"label"`
	Category    string         `yaml:"category,omitempty" json:"category"`
	Description string         `yaml:"description,omitempty" json:"description"`
	Icon        string         `yaml:"icon,omitempty" json:"icon"`
	Fields      []NodeField    `yaml:"fields,omitempty" json:"fields"`
	NeedsInput  bool           `yaml:"needs_input,omitempty" json:"needs_input,omitempty"`
	MinInputs   int            `yaml:"min_inputs,omitempty" json:"min_inputs,omitempty"`
	Operator    string         `yaml:"operator" json:"operator"` // the built-in operator it expands to
	Template    map[string]any `yaml:"template" json:"template"` // operator settings with ${field} placeholders
}

type customEntry struct {
	nt    NodeType
	def   *CustomNodeDef // YAML-defined
	build NodeBuilder    // Go-defined
}

var (
	customMu     sync.RWMutex
	customYAML   = map[string]*customEntry{}
	customGo     = map[string]*customEntry{}
	customErrs   = map[string]string{} // file -> error
	customStamp  string
	customTypeRe = regexp.MustCompile(`^[a-z][a-z0-9_]{1,39}$`)
	placeholder  = regexp.MustCompile(`\$\{([A-Za-z0-9_]+)\}`)
)

// reserved keys a custom field cannot use (they belong to the task itself).
func reservedField(name string) bool {
	switch name {
	case "id", "operator", "upstream", "node", "with":
		return true
	}
	for _, c := range CommonFields {
		if c.Name == name {
			return true
		}
	}
	return false
}

func builtinTaken(t string) bool {
	for _, n := range Nodes {
		if n.Type == t || n.Operator == t {
			return true
		}
	}
	return t == "custom"
}

var baseOperators = map[string]bool{"empty": true, "bash": true, "go": true, "bigquery": true, "http": true, "postgres": true,
	"bigquery_query": true, "bigquery_insert": true, "gsheet_read": true, "gsheet_write": true, "file_read": true,
	"merge": true, "transform": true, "gcs_write": true, "s3_write": true, "bigquery_external_table": true, "bigquery_load_table": true, "bigquery_delete_table": true, "athena_query": true, "athena_external_table": true, "athena_load_table": true, "athena_delete_table": true, "redshift_query": true, "redshift_load_table": true, "redshift_delete_table": true, "bigquery_catalog_sync": true, "redshift_catalog_sync": true, "trigger_dagrun": true,
	"file_sensor": true, "time_delta_sensor": true, "external_task_sensor": true, "http_sensor": true}

func checkNodeMeta(nt NodeType) error {
	if !customTypeRe.MatchString(nt.Type) {
		return fmt.Errorf("node type %q must be lowercase letters, digits and _ (2-40, start with a letter)", nt.Type)
	}
	if builtinTaken(nt.Type) {
		return fmt.Errorf("node type %q is already a built-in node", nt.Type)
	}
	if strings.TrimSpace(nt.Label) == "" {
		return errors.New("label is required")
	}
	seen := map[string]bool{}
	for _, f := range nt.Fields {
		switch {
		case !customTypeRe.MatchString(f.Name):
			return fmt.Errorf("field name %q must be lowercase letters, digits and _", f.Name)
		case reservedField(f.Name):
			return fmt.Errorf("field name %q is reserved", f.Name)
		case seen[f.Name]:
			return fmt.Errorf("duplicate field %q", f.Name)
		}
		seen[f.Name] = true
		if f.Kind == "" {
			return fmt.Errorf("field %q needs a kind (text, code, number, bool, select, list, kv, json)", f.Name)
		}
	}
	return nil
}

func (d *CustomNodeDef) nodeType() NodeType {
	cat := d.Category
	if cat == "" {
		cat = "Custom"
	}
	icon := d.Icon
	if icon == "" {
		icon = "bolt"
	}
	return NodeType{Type: d.Type, Operator: "custom", Label: d.Label, Category: cat, Description: d.Description,
		Icon: icon, Fields: d.Fields, NeedsInput: d.NeedsInput, MinInputs: d.MinInputs, Custom: true, Source: "yaml"}
}

// Validate checks a YAML definition (used by the loader and the UI API).
func (d *CustomNodeDef) Validate() error {
	if err := checkNodeMeta(d.nodeType()); err != nil {
		return err
	}
	if !baseOperators[d.Operator] {
		return fmt.Errorf("operator %q is not a built-in operator", d.Operator)
	}
	if d.Operator == "empty" && len(d.Template) > 0 {
		return errors.New("the empty operator takes no template")
	}
	if _, err := d.expand(nil, true); err != nil {
		return fmt.Errorf("template: %w", err)
	}
	return nil
}

type dropT struct{}

func fieldNames(fs []NodeField) map[string]bool {
	m := map[string]bool{}
	for _, f := range fs {
		m[f.Name] = true
	}
	return m
}

// subst replaces ${field} in v. A string that is exactly one placeholder keeps
// the value's type (number, list...); an empty one drops the key.
func subst(v any, with map[string]any, names map[string]bool) any {
	switch x := v.(type) {
	case string:
		if m := placeholder.FindStringSubmatch(x); m != nil && m[0] == x && names[m[1]] {
			val, ok := with[m[1]]
			if !ok || clean(val) == nil {
				return dropT{}
			}
			return val
		}
		return placeholder.ReplaceAllStringFunc(x, func(s string) string {
			name := placeholder.FindStringSubmatch(s)[1]
			if !names[name] {
				return s // e.g. a shell ${VAR}
			}
			val := with[name]
			if val == nil {
				return ""
			}
			return fmt.Sprint(val)
		})
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			if r := subst(e, with, names); r != (dropT{}) {
				out[k] = r
			}
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, e := range x {
			if r := subst(e, with, names); r != (dropT{}) {
				out = append(out, r)
			}
		}
		return out
	}
	return v
}

// expand turns the node's values into the settings of its base operator.
// lax skips the required-field check (definition validation).
func (d *CustomNodeDef) expand(with map[string]any, lax bool) (yamlTask, error) {
	if !lax {
		for _, f := range d.Fields {
			if f.Required && clean(with[f.Name]) == nil {
				return yamlTask{}, fmt.Errorf("%s is required", firstNonEmpty(f.Label, f.Name))
			}
		}
	}
	m, _ := subst(d.Template, with, fieldNames(d.Fields)).(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	m["operator"] = d.Operator
	raw, err := yaml.Marshal(m)
	if err != nil {
		return yamlTask{}, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	var inner yamlTask
	if err := dec.Decode(&inner); err != nil {
		return yamlTask{}, errors.New(cleanYAMLErr(err))
	}
	return inner, nil
}

// RegisterNodeType adds a Go-defined node to the builder palette. Call it
// before the scheduler / webserver start (for example from main or init).
func RegisterNodeType(nt NodeType, build NodeBuilder) error {
	if build == nil {
		return errors.New("RegisterNodeType needs a Build function")
	}
	nt.Operator, nt.Custom, nt.Source = "custom", true, "go"
	if nt.Category == "" {
		nt.Category = "Custom"
	}
	if nt.Icon == "" {
		nt.Icon = "bolt"
	}
	if err := checkNodeMeta(nt); err != nil {
		return err
	}
	customMu.Lock()
	if _, dup := customGo[nt.Type]; dup {
		customMu.Unlock()
		return fmt.Errorf("node type %q is already registered", nt.Type)
	}
	if _, dup := customYAML[nt.Type]; dup {
		customMu.Unlock()
		return fmt.Errorf("node type %q is already defined in a YAML file", nt.Type)
	}
	customGo[nt.Type] = &customEntry{nt: nt, build: build}
	customMu.Unlock()
	invalidateAllYAML()
	return nil
}

// invalidateAllYAML makes the next LoadDagFolder re-parse every DAG file,
// because a node type they use may have appeared or changed.
func invalidateAllYAML() {
	folderMu.Lock()
	folderStamp = map[string]string{}
	folderMu.Unlock()
}

func lookupCustom(t string) (*customEntry, bool) {
	customMu.RLock()
	defer customMu.RUnlock()
	if e, ok := customYAML[t]; ok {
		return e, true
	}
	e, ok := customGo[t]
	return e, ok
}

// CustomNodes lists the registered custom node types (YAML first, by name).
func CustomNodes() []NodeType {
	customMu.RLock()
	defer customMu.RUnlock()
	var out []NodeType
	for _, m := range []map[string]*customEntry{customYAML, customGo} {
		var part []NodeType
		for _, e := range m {
			part = append(part, e.nt)
		}
		sort.Slice(part, func(i, j int) bool { return part[i].Type < part[j].Type })
		out = append(out, part...)
	}
	return out
}

// AllNodes is the full palette: built-in nodes plus custom ones.
func AllNodes() []NodeType {
	return append(append([]NodeType{}, Nodes...), CustomNodes()...)
}

// CustomNodeDefOf returns the YAML definition of a type (for editing in the UI).
func CustomNodeDefOf(t string) (*CustomNodeDef, bool) {
	customMu.RLock()
	defer customMu.RUnlock()
	e, ok := customYAML[t]
	if !ok {
		return nil, false
	}
	return e.def, true
}

// CustomNodeErrors reports node files that failed to load (file -> message).
func CustomNodeErrors() map[string]string {
	customMu.RLock()
	defer customMu.RUnlock()
	out := make(map[string]string, len(customErrs))
	for k, v := range customErrs {
		out[k] = v
	}
	return out
}

// ParseCustomNodeDef strictly decodes one node definition file.
func ParseCustomNodeDef(data []byte) (*CustomNodeDef, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var d CustomNodeDef
	if err := dec.Decode(&d); err != nil {
		return nil, errors.New(cleanYAMLErr(err))
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// MarshalCustomNodeDef renders a definition as the YAML file content.
func MarshalCustomNodeDef(d *CustomNodeDef) ([]byte, error) {
	return yaml.Marshal(d)
}

// LoadCustomNodes (re)reads every *.yaml in dir. It reports whether anything
// changed since the previous call. A broken file is skipped and listed by
// CustomNodeErrors; it never stops the others.
func LoadCustomNodes(dir string) bool {
	entries, _ := os.ReadDir(dir)
	var stamp strings.Builder
	var files []string
	for _, e := range entries {
		if e.IsDir() || !isYAMLName(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		fmt.Fprintf(&stamp, "%s|%d|%d;", e.Name(), info.ModTime().UnixNano(), info.Size())
		files = append(files, filepath.Join(dir, e.Name()))
	}
	customMu.Lock()
	defer customMu.Unlock()
	if stamp.String() == customStamp {
		return false
	}
	customStamp = stamp.String()
	customYAML = map[string]*customEntry{}
	customErrs = map[string]string{}
	for _, p := range files {
		data, err := os.ReadFile(p)
		if err != nil {
			customErrs[p] = err.Error()
			continue
		}
		def, err := ParseCustomNodeDef(data)
		if err != nil {
			customErrs[p] = err.Error()
			continue
		}
		if _, dup := customGo[def.Type]; dup {
			customErrs[p] = fmt.Sprintf("node type %q is already registered from Go", def.Type)
			continue
		}
		if _, dup := customYAML[def.Type]; dup {
			customErrs[p] = fmt.Sprintf("node type %q is defined twice", def.Type)
			continue
		}
		customYAML[def.Type] = &customEntry{nt: def.nodeType(), def: def}
	}
	return true
}

func isYAMLName(n string) bool {
	e := strings.ToLower(filepath.Ext(n))
	return e == ".yaml" || e == ".yml"
}

// addCustom builds the task of a `operator: custom` node.
func (yt yamlTask) addCustom(d *DAG, baseDir string, opts []TaskOption) error {
	if yt.Node == "" {
		return errors.New("custom operator needs node: <type>")
	}
	e, ok := lookupCustom(yt.Node)
	if !ok {
		return fmt.Errorf("unknown custom node %q: not registered in Go code and no builder/nodes/%s.yaml", yt.Node, yt.Node)
	}
	if e.build != nil {
		for _, f := range e.nt.Fields {
			if f.Required && clean(yt.With[f.Name]) == nil {
				return fmt.Errorf("%s is required", firstNonEmpty(f.Label, f.Name))
			}
		}
		return e.build(d, yt.ID, yt.With, opts...)
	}
	inner, err := e.def.expand(yt.With, false)
	if err != nil {
		return err
	}
	inner.ID = yt.ID
	return inner.add(d, baseDir, opts)
}
