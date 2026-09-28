package dag

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// SourceFile is one file shown in the UI's Code view.
type SourceFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

var (
	builtinMu  sync.RWMutex
	builtinSrc fs.FS
)

// SetBuiltinSources registers the embedded source of the compiled-in DAG
// package, so the Code view works even when the source tree is not deployed.
func SetBuiltinSources(f fs.FS) {
	builtinMu.Lock()
	builtinSrc = f
	builtinMu.Unlock()
}

// IsFileSource reports whether the DAG comes from the DAG folder (and can be reparsed).
func (d *DAG) IsFileSource() bool { return d.Source != "go" }

// SourceFiles returns the code that defines the DAG.
func (d *DAG) SourceFiles() ([]SourceFile, error) {
	read := func(p string) (SourceFile, error) {
		b, err := os.ReadFile(p)
		return SourceFile{Path: p, Content: string(b)}, err
	}
	if d.Source == "go" {
		builtinMu.RLock()
		src := builtinSrc
		builtinMu.RUnlock()
		name := filepath.Base(d.File)
		if src != nil && name != "" {
			if b, err := fs.ReadFile(src, name); err == nil {
				return []SourceFile{{Path: "builtin_dags/" + name, Content: string(b)}}, nil
			}
		}
		if d.File != "" {
			if f, err := read(d.File); err == nil {
				return []SourceFile{f}, nil
			}
		}
		return nil, errors.New("source of this compiled-in DAG is not available")
	}
	if d.File != "" {
		if f, err := read(d.File); err == nil {
			return []SourceFile{f}, nil
		}
	}
	info, err := os.Stat(d.Source)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		f, err := read(d.Source)
		return []SourceFile{f}, err
	}
	entries, err := os.ReadDir(d.Source)
	if err != nil {
		return nil, err
	}
	var out []SourceFile
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			if f, err := read(filepath.Join(d.Source, e.Name())); err == nil {
				out = append(out, f)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}
