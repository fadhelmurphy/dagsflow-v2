package bundle

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"dagsflow-go/dag"
)

// Loader finds Go DAG files in the DAG folder, compiles them and registers
// proxy DAGs whose tasks run inside the compiled bundle process.
//
// A bundle is either a single `package main` .go file at the top of the DAG
// folder, or a sub-directory whose `package main` files form one program.
// Files of other packages (e.g. the compiled-in `package dags`) are ignored.
type Loader struct {
	Home   string // $DAGSFLOW_HOME: build cache lives in Home/build
	SDKDir string // dagsflow-go source (module "dagsflow-go"); auto-detected when empty
	GoBin  string // go toolchain, default "go"
	Log    func(format string, a ...any)

	mu      sync.Mutex
	stamps  map[string]string // bundle key -> file stamps
	known   map[string]bool
	current map[string]string // bundle key -> binary in use
	sdkHash string
}

type bundleSrc struct {
	key   string   // file path or directory: registry source
	dir   string   // directory containing the files
	files []string // absolute .go paths
}

func (l *Loader) logf(format string, a ...any) {
	if l.Log != nil {
		l.Log(format, a...)
	}
}

// LoadFolder (re)builds changed bundles and unregisters deleted ones.
func (l *Loader) LoadFolder(dir string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stamps == nil {
		l.stamps, l.known, l.current = map[string]string{}, map[string]bool{}, map[string]string{}
	}
	srcs := discover(dir)
	seen := map[string]bool{}
	for _, b := range srcs {
		seen[b.key] = true
		st := stamp(b.files)
		if l.stamps[b.key] == st {
			continue
		}
		dags, err := l.load(b)
		l.stamps[b.key] = st // errors are retried when the file changes
		l.known[b.key] = true
		dag.ReplaceSource(b.key, dags, err)
	}
	for k := range l.known {
		if !seen[k] {
			delete(l.known, k)
			delete(l.stamps, k)
			delete(l.current, k)
			dag.ReplaceSource(k, nil, nil)
		}
	}
	l.gc()
}

// Invalidate forces the bundle (a file or directory key) to be reloaded on the
// next LoadFolder, even if its files did not change.
func (l *Loader) Invalidate(key string) {
	l.mu.Lock()
	delete(l.stamps, key)
	l.mu.Unlock()
}

// gc removes cached bundle binaries that are no longer current and unused
// for a day (runs started before a DAG change keep using their old binary).
func (l *Loader) gc() {
	dir := filepath.Join(l.Home, "build", "bin")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	inUse := map[string]bool{}
	for _, b := range l.current {
		inUse[b] = true
	}
	for _, e := range entries {
		p, _ := filepath.Abs(filepath.Join(dir, e.Name()))
		if inUse[p] {
			continue
		}
		if fi, err := e.Info(); err == nil && time.Since(fi.ModTime()) > 24*time.Hour {
			_ = os.Remove(p)
		}
	}
}

func discover(root string) []bundleSrc {
	byDir := map[string][]string{}
	_ = filepath.WalkDir(root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		name := e.Name()
		if e.IsDir() {
			if p != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.PackageClauseOnly)
		if err != nil || f.Name.Name != "main" {
			return nil
		}
		abs, _ := filepath.Abs(p)
		byDir[filepath.Dir(p)] = append(byDir[filepath.Dir(p)], abs)
		return nil
	})
	var out []bundleSrc
	for d, files := range byDir {
		sort.Strings(files)
		abs, _ := filepath.Abs(d)
		if filepath.Clean(d) == filepath.Clean(root) {
			for i, f := range files { // top level: one bundle per file
				out = append(out, bundleSrc{key: filepath.Join(d, filepath.Base(files[i])), dir: abs, files: []string{f}})
			}
			continue
		}
		out = append(out, bundleSrc{key: d, dir: abs, files: files})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func stamp(files []string) string {
	var b strings.Builder
	for _, f := range files {
		if fi, err := os.Stat(f); err == nil {
			fmt.Fprintf(&b, "%s|%d|%d;", f, fi.ModTime().UnixNano(), fi.Size())
		}
	}
	return b.String()
}

func (l *Loader) load(b bundleSrc) ([]*dag.DAG, error) {
	bin, err := l.build(b)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := dag.CommandContext(ctx, bin, "describe")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("describe failed: %v\n%s", err, tail(stderr.String(), 2000))
	}
	var desc Describe
	if err := json.Unmarshal(out, &desc); err != nil {
		return nil, fmt.Errorf("describe: bad output (does a DAG print to stdout in init?): %v", err)
	}
	if len(desc.ImportErrors) > 0 {
		var msgs []string
		for id, e := range desc.ImportErrors {
			msgs = append(msgs, id+": "+e)
		}
		sort.Strings(msgs)
		return nil, errors.New(strings.Join(msgs, "\n"))
	}
	if len(desc.Dags) == 0 {
		return nil, errors.New("no DAG registered (call dag.Register in init)")
	}
	p := &proxy{bin: bin}
	var dags []*dag.DAG
	for _, spec := range desc.Dags {
		d := p.build(spec)
		for _, f := range b.files {
			if filepath.Base(f) == spec.File {
				d.File = f
			}
		}
		dags = append(dags, d)
	}
	return dags, nil
}

// ---- building ---------------------------------------------------------------------

const mainGen = `// Code generated by dagsflow. DO NOT EDIT.

//go:build dagsflow

package main

import "dagsflow-go/bundle"

func main() { bundle.Main() }
`

func (l *Loader) goBin() string {
	if l.GoBin != "" {
		return l.GoBin
	}
	if g := os.Getenv("DAGSFLOW_GO"); g != "" {
		return g
	}
	return "go"
}

func (l *Loader) sdk() (string, error) {
	if l.SDKDir == "" {
		l.SDKDir = os.Getenv("DAGSFLOW_SDK_DIR")
	}
	if l.SDKDir == "" { // walk up from cwd and from the executable
		starts := []string{}
		if wd, err := os.Getwd(); err == nil {
			starts = append(starts, wd)
		}
		if exe, err := os.Executable(); err == nil {
			starts = append(starts, filepath.Dir(exe))
		}
		for _, s := range starts {
			for d := s; ; d = filepath.Dir(d) {
				if b, err := os.ReadFile(filepath.Join(d, "go.mod")); err == nil && strings.Contains(string(b), "module dagsflow-go\n") {
					l.SDKDir = d
					break
				}
				if filepath.Dir(d) == d {
					break
				}
			}
			if l.SDKDir != "" {
				break
			}
		}
	}
	if l.SDKDir == "" {
		return "", errors.New("dagsflow-go source not found: set DAGSFLOW_SDK_DIR to the repo checkout (needed to compile Go DAGs)")
	}
	abs, err := filepath.Abs(l.SDKDir)
	l.SDKDir = abs
	return abs, err
}

// sdkFingerprint changes whenever the SDK source or the Go version changes,
// so bundles are rebuilt against the current dag package.
func (l *Loader) sdkFingerprint(sdk string) (string, error) {
	if l.sdkHash != "" {
		return l.sdkHash, nil
	}
	h := sha256.New()
	// No Go version in the fingerprint: a node without a toolchain (e.g. a
	// webserver) can still use binaries already built into a shared build dir.
	_ = filepath.WalkDir(sdk, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(sdk, p)
		if e.IsDir() {
			top := strings.Split(rel, string(filepath.Separator))[0]
			if rel != "." && (strings.HasPrefix(e.Name(), ".") || top == "dags" || top == "state" || top == "logs" || top == "build") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".go") || e.Name() == "go.mod" || e.Name() == "go.sum" {
			if b, err := os.ReadFile(p); err == nil {
				h.Write([]byte(rel))
				h.Write(b)
			}
		}
		return nil
	})
	l.sdkHash = hex.EncodeToString(h.Sum(nil))
	return l.sdkHash, nil
}

func (l *Loader) build(b bundleSrc) (string, error) {
	sdk, err := l.sdk()
	if err != nil {
		return "", err
	}
	fp, err := l.sdkFingerprint(sdk)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	h.Write([]byte(fp))
	contents := map[string][]byte{}
	for _, f := range b.files {
		data, err := os.ReadFile(f)
		if err != nil {
			return "", err
		}
		contents[filepath.Base(f)] = data
		h.Write([]byte(filepath.Base(f)))
		h.Write(data)
	}
	sum := hex.EncodeToString(h.Sum(nil))[:16]
	home, err := filepath.Abs(l.Home)
	if err != nil {
		return "", err
	}
	binDir := filepath.Join(home, "build", "bin")
	bin := filepath.Join(binDir, sum)
	if isWindows() {
		bin += ".exe"
	}
	l.current[b.key] = bin
	if _, err := os.Stat(bin); err == nil {
		_ = os.Chtimes(bin, time.Now(), time.Now()) // keep it out of gc
		return bin, nil                             // cached
	}

	work, err := os.MkdirTemp(mkdirAll(home, "build", "src"), sum+"-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(work)
	for name, data := range contents {
		if name == "zz_dagsflow_main.go" {
			return "", errors.New("zz_dagsflow_main.go is reserved")
		}
		if err := os.WriteFile(filepath.Join(work, name), data, 0o644); err != nil {
			return "", err
		}
	}
	if err := os.WriteFile(filepath.Join(work, "zz_dagsflow_main.go"), []byte(mainGen), 0o644); err != nil {
		return "", err
	}
	if err := l.writeModule(sdk, work); err != nil {
		return "", err
	}

	l.logf("building Go DAG bundle %s", b.key)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	rnd := make([]byte, 6)
	_, _ = rand.Read(rnd)
	// random suffix: scheduler and webserver containers may both be pid 1
	tmp := filepath.Join(mkdirAll(binDir), fmt.Sprintf(".%s.%x.tmp", sum, rnd))
	cmd := exec.CommandContext(ctx, l.goBin(), "build", "-tags", "dagsflow", "-o", tmp, ".")
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.Remove(tmp)
		msg := strings.ReplaceAll(string(out), work+string(filepath.Separator), "")
		msg = strings.ReplaceAll(msg, "./", "")
		return "", fmt.Errorf("compile failed:\n%s", tail(msg, 4000))
	}
	if err := os.Rename(tmp, bin); err != nil {
		return "", err
	}
	l.logf("built %s in %s", b.key, time.Since(start).Round(time.Millisecond))
	return bin, nil
}

// writeModule creates a go.mod that uses the SDK checkout (replace) and its
// dependency versions, plus the SDK's go.sum, so builds work offline with the
// same versions as the scheduler binary.
func (l *Loader) writeModule(sdk, work string) error {
	out, err := exec.Command(l.goBin(), "mod", "edit", "-json", filepath.Join(sdk, "go.mod")).Output()
	if err != nil {
		return fmt.Errorf("read sdk go.mod: %v", err)
	}
	var m struct {
		Go      string
		Require []struct{ Path, Version string }
		Replace []struct {
			Old struct{ Path, Version string }
			New struct{ Path, Version string }
		}
	}
	if err := json.Unmarshal(out, &m); err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "module dagsflow-bundle\n\ngo %s\n\nrequire dagsflow-go v0.0.0\n\nrequire (\n", m.Go)
	for _, r := range m.Require {
		fmt.Fprintf(&b, "\t%s %s\n", r.Path, r.Version)
	}
	fmt.Fprintf(&b, ")\n\nreplace dagsflow-go => %s\n", quote(sdk))
	for _, r := range m.Replace {
		np := r.New.Path
		if r.New.Version == "" && !filepath.IsAbs(np) { // local path relative to the SDK
			np = filepath.Join(sdk, np)
		}
		old := r.Old.Path
		if r.Old.Version != "" {
			old += " " + r.Old.Version
		}
		if r.New.Version != "" {
			fmt.Fprintf(&b, "replace %s => %s %s\n", old, np, r.New.Version)
		} else {
			fmt.Fprintf(&b, "replace %s => %s\n", old, quote(np))
		}
	}
	if err := os.WriteFile(filepath.Join(work, "go.mod"), []byte(b.String()), 0o644); err != nil {
		return err
	}
	sum, err := os.ReadFile(filepath.Join(sdk, "go.sum"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(filepath.Join(work, "go.sum"), sum, 0o644)
}

func quote(p string) string {
	if strings.ContainsAny(p, " \t") {
		return fmt.Sprintf("%q", p)
	}
	return p
}

func mkdirAll(parts ...string) string {
	p := filepath.Join(parts...)
	_ = os.MkdirAll(p, 0o755)
	return p
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return "…" + s[len(s)-n:]
	}
	return s
}

// ---- proxy DAGs (host side) ----------------------------------------------------

type proxy struct{ bin string }

func (p *proxy) build(s DagSpec) *dag.DAG {
	opts := []dag.DAGOption{dag.WithStartDate(s.StartDate), dag.WithCatchup(s.Catchup),
		dag.WithMaxActiveRuns(s.MaxActiveRuns), dag.WithParams(s.Params), dag.WithDescription(s.Description),
		dag.WithTags(s.Tags...), func(d *dag.DAG) { d.DefaultArgs.Owner = s.Owner }}
	if len(s.Datasets) > 0 {
		opts = append(opts, dag.WithDatasetSchedule(s.Datasets...))
	}
	if s.OnSuccess > 0 {
		opts = append(opts, dag.WithOnSuccess(p.callback(s.ID, "", "dag_on_success")))
	}
	if s.OnFailure > 0 {
		opts = append(opts, dag.WithOnFailure(p.callback(s.ID, "", "dag_on_failure")))
	}
	if s.OnSLAMiss > 0 {
		opts = append(opts, dag.WithSLAMiss(p.callback(s.ID, "", "dag_on_sla_miss")))
	}
	d := dag.NewDAG(s.ID, s.Schedule, opts...)
	tasks := map[string]*dag.Task{}
	for _, ts := range s.Tasks {
		ts := ts
		o := []dag.TaskOption{dag.WithTriggerRule(dag.TriggerRule(ts.TriggerRule)), dag.WithRetries(ts.Retries, ts.RetryDelay),
			dag.WithTimeout(ts.Timeout), dag.WithPool(ts.Pool), dag.WithPoolSlots(ts.PoolSlots), dag.WithQueue(ts.Queue), dag.WithPodOverride(ts.PodOverride),
			dag.WithPriorityWeight(ts.Priority), dag.WithSLA(ts.SLA), dag.WithOutlets(ts.Outlets...)}
		if ts.Operator != "" {
			o = append(o, dag.WithOperatorName(ts.Operator))
		}
		for _, l := range ts.ExtraLinks {
			o = append(o, dag.WithExtraLink(l.Name, l.XComKey))
		}
		if len(ts.Templates) > 0 {
			o = append(o, dag.WithTemplateFields(ts.Templates...))
		}
		if ts.Map != nil {
			o = append(o, dag.Expand(*ts.Map))
		}
		if ts.OnSuccess > 0 {
			o = append(o, dag.WithOnSuccessCallback(p.callback(s.ID, ts.ID, "task_on_success")))
		}
		if ts.OnFailure > 0 {
			o = append(o, dag.WithOnFailureCallback(p.callback(s.ID, ts.ID, "task_on_failure")))
		}
		if ts.OnRetry > 0 {
			o = append(o, dag.WithOnRetryCallback(p.callback(s.ID, ts.ID, "task_on_retry")))
		}
		switch {
		case ts.Sensor != nil:
			tasks[ts.ID] = d.NewSensor(ts.ID, func(ctx *dag.TaskContext) (bool, error) {
				r, err := p.invoke(ctx, ctx, Request{Action: "poke", DagID: s.ID, TaskID: ts.ID, Info: ctx.RunInfo}, ctx.LogWriter())
				return r.PokeOK, err
			}, *ts.Sensor, o...)
		case ts.Branch:
			tasks[ts.ID] = d.NewBranchTask(ts.ID, func(ctx *dag.TaskContext) ([]string, error) {
				r, err := p.invoke(ctx, ctx, Request{Action: "task", DagID: s.ID, TaskID: ts.ID, Info: ctx.RunInfo}, ctx.LogWriter())
				return r.Selected, err
			}, o...)
		default:
			tasks[ts.ID] = d.NewTask(ts.ID, func(ctx *dag.TaskContext) error {
				_, err := p.invoke(ctx, ctx, Request{Action: "task", DagID: s.ID, TaskID: ts.ID, Info: ctx.RunInfo}, ctx.LogWriter())
				return err
			}, o...)
		}
	}
	for _, ts := range s.Tasks {
		for _, u := range ts.Upstream {
			if ut, ok := tasks[u]; ok {
				ut.ThenAll(tasks[ts.ID])
			}
		}
	}
	return d
}

func (p *proxy) callback(dagID, taskID, which string) dag.Callback {
	return func(ctx context.Context, cc dag.CallbackContext) error {
		var buf bytes.Buffer
		_, err := p.invoke(ctx, nil, Request{Action: "callback", DagID: dagID, TaskID: taskID,
			Callback: which, CC: toWire(cc)}, &buf)
		if err != nil && buf.Len() > 0 {
			return fmt.Errorf("%w\n%s", err, tail(buf.String(), 1000))
		}
		return err
	}
}

// invoke runs one action in a fresh bundle process. The process is killed
// (whole process group) when ctx is cancelled, e.g. on task timeout.
func (p *proxy) invoke(ctx context.Context, tctx *dag.TaskContext, req Request, logOut io.Writer) (Result, error) {
	h, err := getHost()
	if err != nil {
		return Result{}, fmt.Errorf("start rpc host: %w", err)
	}
	tok, sess := h.open(tctx)
	defer h.close(tok)
	body, err := json.Marshal(req)
	if err != nil {
		return Result{}, err
	}
	cmd := dag.CommandContext(ctx, p.bin, "run")
	cmd.Env = append(os.Environ(), "DAGSFLOW_RPC_ADDR="+h.addr, "DAGSFLOW_RPC_TOKEN="+tok)
	cmd.Stdin = bytes.NewReader(body)
	cmd.Stdout, cmd.Stderr = logOut, logOut
	runErr := cmd.Run()

	sess.mu.Lock()
	res := sess.result
	sess.mu.Unlock()
	switch {
	case ctx.Err() != nil:
		return res, ctx.Err()
	case res.Err != "":
		return res, errors.New(res.Err)
	case runErr != nil:
		return res, fmt.Errorf("DAG bundle process failed: %w", runErr)
	case !res.Done:
		return res, errors.New("DAG bundle process exited without reporting a result")
	}
	return res, nil
}

func isWindows() bool { return os.PathSeparator == '\\' }
