package dag

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
	"unicode"
)

// Template functions behind the Jinja translation (filters, operators, tests).
// Values from XComs/params are JSON values (float64, string, bool, nil,
// []any, map[string]any), so numbers compare as numbers whatever their Go type.

func jnum(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

func jstr(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1e15 {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "True"
		}
		return "False"
	case []any, map[string]any:
		b, _ := json.Marshal(x)
		return string(b)
	}
	return fmt.Sprint(v)
}

func jcmp(a, b any) (int, error) {
	if fa, ok := jnum(a); ok {
		if fb, ok := jnum(b); ok {
			switch {
			case fa < fb:
				return -1, nil
			case fa > fb:
				return 1, nil
			}
			return 0, nil
		}
	}
	sa, oka := a.(string)
	sb, okb := b.(string)
	if oka && okb {
		return strings.Compare(sa, sb), nil
	}
	return 0, fmt.Errorf("cannot compare %T and %T", a, b)
}

func jequal(a, b any) bool {
	if fa, ok := jnum(a); ok {
		if fb, ok := jnum(b); ok {
			return fa == fb
		}
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.DeepEqual(a, b)
}

func jtruthy(v any) bool {
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	if f, ok := jnum(v); ok {
		return f != 0
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String, reflect.Slice, reflect.Map, reflect.Array:
		return rv.Len() > 0
	case reflect.Pointer, reflect.Interface:
		return !rv.IsNil()
	}
	return true
}

func jlen(v any) int {
	if v == nil {
		return 0
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return len([]rune(rv.String()))
	case reflect.Slice, reflect.Map, reflect.Array:
		return rv.Len()
	}
	return 0
}

func jslice(v any) []any {
	if v == nil {
		return nil
	}
	if s, ok := v.([]any); ok {
		return s
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = rv.Index(i).Interface()
		}
		return out
	case reflect.Map:
		keys := rv.MapKeys()
		out := make([]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, k.Interface())
		}
		sort.Slice(out, func(i, j int) bool { return jstr(out[i]) < jstr(out[j]) })
		return out
	case reflect.String:
		var out []any
		for _, r := range rv.String() {
			out = append(out, string(r))
		}
		return out
	}
	return []any{v}
}

// jget walks keys (map keys, struct fields, list indexes) and returns nil when
// one is missing: Jinja's "undefined".
func jget(root any, keys ...any) any {
	cur := root
	for _, k := range keys {
		if cur == nil {
			return nil
		}
		switch c := cur.(type) {
		case map[string]any:
			v, ok := c[jstr(k)]
			if !ok {
				return nil
			}
			cur = v
			continue
		case []any:
			f, ok := jnum(k)
			if !ok {
				return nil
			}
			i := int(f)
			if i < 0 {
				i += len(c)
			}
			if i < 0 || i >= len(c) {
				return nil
			}
			cur = c[i]
			continue
		}
		rv := reflect.ValueOf(cur)
		for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
			if rv.IsNil() {
				return nil
			}
			rv = rv.Elem()
		}
		switch rv.Kind() {
		case reflect.Map:
			mv := rv.MapIndex(reflect.ValueOf(jstr(k)))
			if !mv.IsValid() {
				return nil
			}
			cur = mv.Interface()
		case reflect.Struct:
			f := rv.FieldByName(jstr(k))
			if !f.IsValid() || !f.CanInterface() {
				return nil
			}
			cur = f.Interface()
		case reflect.Slice, reflect.Array:
			f, ok := jnum(k)
			if !ok || int(f) < 0 || int(f) >= rv.Len() {
				return nil
			}
			cur = rv.Index(int(f)).Interface()
		default:
			return nil
		}
	}
	return cur
}

func jarith(op string, a, b any) (any, error) {
	if op == "+" {
		if sa, ok := a.(string); ok {
			if sb, ok := b.(string); ok {
				return sa + sb, nil
			}
		}
		if la, ok := a.([]any); ok {
			if lb, ok := b.([]any); ok {
				return append(append([]any{}, la...), lb...), nil
			}
		}
	}
	fa, oka := jnum(a)
	fb, okb := jnum(b)
	if !oka || !okb {
		return nil, fmt.Errorf("cannot use %s on %T and %T", op, a, b)
	}
	var r float64
	switch op {
	case "+":
		r = fa + fb
	case "-":
		r = fa - fb
	case "*":
		r = fa * fb
	case "/":
		if fb == 0 {
			return nil, errors.New("division by zero")
		}
		return fa / fb, nil
	case "//":
		if fb == 0 {
			return nil, errors.New("division by zero")
		}
		r = math.Floor(fa / fb)
	case "%":
		if fb == 0 {
			return nil, errors.New("division by zero")
		}
		r = math.Mod(fa, fb)
	}
	_, ia := a.(float64)
	_, ib := b.(float64)
	if r == math.Trunc(r) && !ia && !ib { // int op int stays an int
		return int(r), nil
	}
	return r, nil
}

func jtoInt(v any) int {
	if f, ok := jnum(v); ok {
		return int(f)
	}
	if s, ok := v.(string); ok {
		s = strings.TrimSpace(s)
		if i, err := strconv.Atoi(s); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int(f)
		}
	}
	if b, ok := v.(bool); ok && b {
		return 1
	}
	return 0
}

// splitOpts separates trailing "name", value option pairs given as
// "attribute"/"reverse" from the value (always last).
func splitOpts(args []any) (opts map[string]any, rest []any, val any) {
	opts = map[string]any{}
	if len(args) == 0 {
		return opts, nil, nil
	}
	val = args[len(args)-1]
	args = args[:len(args)-1]
	for i := 0; i+1 < len(args); {
		if s, ok := args[i].(string); ok && (s == "attribute" || s == "reverse") {
			opts[s] = args[i+1]
			args = append(args[:i], args[i+2:]...)
			continue
		}
		i++
	}
	return opts, args, val
}

var tagRe = regexp.MustCompile(`<[^>]*>`)

func jinjaFuncs(c *TaskContext, depth int) template.FuncMap {
	sortKey := func(opts map[string]any) func(any) any {
		if a, ok := opts["attribute"]; ok {
			keys := strings.Split(jstr(a), ".")
			return func(v any) any {
				ks := make([]any, len(keys))
				for i, k := range keys {
					ks[i] = k
				}
				return jget(v, ks...)
			}
		}
		return func(v any) any { return v }
	}
	applyFilter := func(name string, v any) (any, error) {
		switch name {
		case "upper":
			return strings.ToUpper(jstr(v)), nil
		case "lower":
			return strings.ToLower(jstr(v)), nil
		case "trim":
			return strings.TrimSpace(jstr(v)), nil
		case "int":
			return jtoInt(v), nil
		case "string":
			return jstr(v), nil
		case "length":
			return jlen(v), nil
		}
		return nil, fmt.Errorf("map('%s') is not supported", name)
	}
	return template.FuncMap{
		"jeq": jequal,
		"jne": func(a, b any) bool { return !jequal(a, b) },
		"jlt": func(a, b any) (bool, error) { c, err := jcmp(a, b); return c < 0, err },
		"jle": func(a, b any) (bool, error) { c, err := jcmp(a, b); return c <= 0, err },
		"jgt": func(a, b any) (bool, error) { c, err := jcmp(a, b); return c > 0, err },
		"jge": func(a, b any) (bool, error) { c, err := jcmp(a, b); return c >= 0, err },
		"jin": func(a, b any) bool {
			switch x := b.(type) {
			case string:
				return strings.Contains(x, jstr(a))
			case map[string]any:
				_, ok := x[jstr(a)]
				return ok
			}
			for _, e := range jslice(b) {
				if jequal(a, e) {
					return true
				}
			}
			return false
		},
		"jadd":      func(a, b any) (any, error) { return jarith("+", a, b) },
		"jsub":      func(a, b any) (any, error) { return jarith("-", a, b) },
		"jmul":      func(a, b any) (any, error) { return jarith("*", a, b) },
		"jdiv":      func(a, b any) (any, error) { return jarith("/", a, b) },
		"jfloordiv": func(a, b any) (any, error) { return jarith("//", a, b) },
		"jmod":      func(a, b any) (any, error) { return jarith("%", a, b) },
		"jconcat":   func(a, b any) string { return jstr(a) + jstr(b) },
		"jget":      jget,
		"jgetor": func(m any, key any, def any) any {
			if v := jget(m, key); v != nil {
				return v
			}
			return def
		},
		"jindex": func(m any, key any) (any, error) {
			v := jget(m, key)
			if v == nil {
				return nil, fmt.Errorf("no item %v", key)
			}
			return v, nil
		},
		"jdefined": func(v any) bool { return v != nil },
		"jif": func(c, a, b any) any {
			if jtruthy(c) {
				return a
			}
			return b
		},
		"jlist": func(v ...any) []any { return v },
		"jdict": func(kv ...any) map[string]any {
			m := map[string]any{}
			for i := 0; i+1 < len(kv); i += 2 {
				m[jstr(kv[i])] = kv[i+1]
			}
			return m
		},
		"jislast": func(i any, coll any) bool { return jtoInt(i) == jlen(coll)-1 },
		"jitems": func(m any) []any {
			var out []any
			for _, k := range jslice(m) {
				out = append(out, []any{k, jget(m, k)})
			}
			return out
		},
		"jkeys": func(m any) []any { return jslice(m) },
		"jvalues": func(m any) []any {
			var out []any
			for _, k := range jslice(m) {
				out = append(out, jget(m, k))
			}
			return out
		},
		"jsplit": func(sep string, s any) []any {
			var parts []string
			if sep == " " {
				parts = strings.Fields(jstr(s))
			} else {
				parts = strings.Split(jstr(s), sep)
			}
			out := make([]any, len(parts))
			for i, p := range parts {
				out[i] = p
			}
			return out
		},
		"jstartswith": func(p string, s any) bool { return strings.HasPrefix(jstr(s), p) },
		"jendswith":   func(p string, s any) bool { return strings.HasSuffix(jstr(s), p) },
		"jtest": func(name string, v any) bool {
			switch name {
			case "string":
				_, ok := v.(string)
				return ok
			case "number":
				_, ok := jnum(v)
				return ok
			case "mapping":
				_, ok := v.(map[string]any)
				return ok
			case "sequence", "iterable":
				k := reflect.ValueOf(v).Kind()
				return v != nil && (k == reflect.Slice || k == reflect.Array || k == reflect.String)
			case "true":
				return v == true
			case "false":
				return v == false
			case "odd":
				return jtoInt(v)%2 != 0
			case "even":
				return jtoInt(v)%2 == 0
			case "empty":
				return !jtruthy(v)
			}
			return false
		},
		"jfromjson": func(v any) (any, error) {
			var out any
			err := json.Unmarshal([]byte(jstr(v)), &out)
			return out, err
		},
		"ds_format": func(ds, in, out string) (string, error) {
			t, err := time.Parse(strftimeToGo(in), ds)
			if err != nil {
				return "", err
			}
			return t.Format(strftimeToGo(out)), nil
		},
		"jnow": func() time.Time { return time.Now() },
		"jrange": func(a ...any) ([]any, error) {
			start, stop, step := 0, 0, 1
			switch len(a) {
			case 1:
				stop = jtoInt(a[0])
			case 2:
				start, stop = jtoInt(a[0]), jtoInt(a[1])
			case 3:
				start, stop, step = jtoInt(a[0]), jtoInt(a[1]), jtoInt(a[2])
			default:
				return nil, errors.New("range needs 1 to 3 arguments")
			}
			if step == 0 || (stop-start)/step > 100000 {
				return nil, errors.New("range is too large")
			}
			var out []any
			for i := start; (step > 0 && i < stop) || (step < 0 && i > stop); i += step {
				out = append(out, i)
			}
			return out, nil
		},
		// filters (value last)
		"upper":      func(v any) string { return strings.ToUpper(jstr(v)) },
		"lower":      func(v any) string { return strings.ToLower(jstr(v)) },
		"trim":       func(v any) string { return strings.TrimSpace(jstr(v)) },
		"title":      func(v any) string { return titleCase(jstr(v)) },
		"capitalize": func(v any) string { s := strings.ToLower(jstr(v)); return upperFirst(s) },
		"default": func(args ...any) any {
			if len(args) < 2 {
				return nil
			}
			def, v := args[0], args[len(args)-1]
			falsy := len(args) == 3 && jtruthy(args[1])
			if v == nil || (falsy && !jtruthy(v)) {
				return def
			}
			return v
		},
		"replace": func(old, new string, v any) string { return strings.ReplaceAll(jstr(v), old, new) },
		"int":     func(args ...any) int { return jtoInt(args[len(args)-1]) },
		"float": func(args ...any) float64 {
			v := args[len(args)-1]
			if f, ok := jnum(v); ok {
				return f
			}
			f, _ := strconv.ParseFloat(strings.TrimSpace(jstr(v)), 64)
			return f
		},
		"string": jstr,
		"length": jlen,
		"join": func(args ...any) string {
			sep := ""
			if len(args) == 2 {
				sep = jstr(args[0])
			}
			var parts []string
			for _, e := range jslice(args[len(args)-1]) {
				parts = append(parts, jstr(e))
			}
			return strings.Join(parts, sep)
		},
		"tojson": func(args ...any) (string, error) {
			b, err := json.Marshal(args[len(args)-1])
			return string(b), err
		},
		"jescape": func(v any) string { return html.EscapeString(jstr(v)) },
		"jfirst": func(v any) any {
			if s := jslice(v); len(s) > 0 {
				return s[0]
			}
			return nil
		},
		"jlast": func(v any) any {
			if s := jslice(v); len(s) > 0 {
				return s[len(s)-1]
			}
			return nil
		},
		"jround": func(args ...any) float64 {
			prec := 0
			if len(args) > 1 {
				prec = jtoInt(args[0])
			}
			f, _ := jnum(args[len(args)-1])
			p := math.Pow(10, float64(prec))
			return math.Round(f*p) / p
		},
		"jabs":    func(v any) float64 { f, _ := jnum(v); return math.Abs(f) },
		"jtolist": func(v any) []any { return jslice(v) },
		"jsort": func(args ...any) []any {
			opts, _, val := splitOpts(args)
			s := append([]any{}, jslice(val)...)
			key := sortKey(opts)
			sort.SliceStable(s, func(i, j int) bool {
				c, err := jcmp(key(s[i]), key(s[j]))
				if err != nil {
					return jstr(key(s[i])) < jstr(key(s[j]))
				}
				return c < 0
			})
			if jtruthy(opts["reverse"]) {
				for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
					s[i], s[j] = s[j], s[i]
				}
			}
			return s
		},
		"junique": func(args ...any) []any {
			opts, _, val := splitOpts(args)
			key := sortKey(opts)
			var out []any
			seen := map[string]bool{}
			for _, e := range jslice(val) {
				k := jstr(key(e))
				if !seen[k] {
					seen[k] = true
					out = append(out, e)
				}
			}
			return out
		},
		"jreverse": func(v any) any {
			if s, ok := v.(string); ok {
				r := []rune(s)
				for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
					r[i], r[j] = r[j], r[i]
				}
				return string(r)
			}
			s := append([]any{}, jslice(v)...)
			for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
				s[i], s[j] = s[j], s[i]
			}
			return s
		},
		"jsum": func(args ...any) float64 {
			opts, _, val := splitOpts(args)
			key := sortKey(opts)
			t := 0.0
			for _, e := range jslice(val) {
				f, _ := jnum(key(e))
				t += f
			}
			return t
		},
		"jmin": func(args ...any) any {
			opts, _, val := splitOpts(args)
			key := sortKey(opts)
			var best any
			for _, e := range jslice(val) {
				if c, err := jcmp(key(e), key(best)); best == nil || (err == nil && c < 0) {
					best = e
				}
			}
			return best
		},
		"jmax": func(args ...any) any {
			opts, _, val := splitOpts(args)
			key := sortKey(opts)
			var best any
			for _, e := range jslice(val) {
				if c, err := jcmp(key(e), key(best)); best == nil || (err == nil && c > 0) {
					best = e
				}
			}
			return best
		},
		"jmap": func(args ...any) ([]any, error) {
			opts, rest, val := splitOpts(args)
			var out []any
			for _, e := range jslice(val) {
				switch {
				case opts["attribute"] != nil:
					out = append(out, sortKey(opts)(e))
				case len(rest) == 1:
					v, err := applyFilter(jstr(rest[0]), e)
					if err != nil {
						return nil, err
					}
					out = append(out, v)
				default:
					return nil, errors.New("map needs attribute=... or a filter name")
				}
			}
			return out, nil
		},
		"jselect": func(args ...any) []any {
			_, _, val := splitOpts(args)
			var out []any
			for _, e := range jslice(val) {
				if jtruthy(e) {
					out = append(out, e)
				}
			}
			return out
		},
		"jreject": func(args ...any) []any {
			_, _, val := splitOpts(args)
			var out []any
			for _, e := range jslice(val) {
				if !jtruthy(e) {
					out = append(out, e)
				}
			}
			return out
		},
		"jbatch": func(n any, v any) []any {
			size := jtoInt(n)
			if size <= 0 {
				size = 1
			}
			var out []any
			s := jslice(v)
			for i := 0; i < len(s); i += size {
				out = append(out, s[i:min(i+size, len(s))])
			}
			return out
		},
		"jtruncate": func(args ...any) string {
			n := 255
			if len(args) > 1 {
				n = jtoInt(args[0])
			}
			r := []rune(jstr(args[len(args)-1]))
			if len(r) <= n {
				return string(r)
			}
			return string(r[:max(0, n-3)]) + "..."
		},
		"jindent": func(args ...any) string {
			n := 4
			if len(args) > 1 {
				n = jtoInt(args[0])
			}
			return strings.ReplaceAll(jstr(args[len(args)-1]), "\n", "\n"+strings.Repeat(" ", n))
		},
		"jformat": func(args ...any) string { // "%s-%s" | format(a, b): the format is the value
			f := jstr(args[len(args)-1])
			return fmt.Sprintf(f, args[:len(args)-1]...)
		},
		"jcenter": func(args ...any) string {
			w := 80
			if len(args) > 1 {
				w = jtoInt(args[0])
			}
			s := jstr(args[len(args)-1])
			pad := w - len([]rune(s))
			if pad <= 0 {
				return s
			}
			return strings.Repeat(" ", pad/2) + s + strings.Repeat(" ", pad-pad/2)
		},
		"jwordcount":   func(v any) int { return len(strings.Fields(jstr(v))) },
		"jstriptags":   func(v any) string { return strings.Join(strings.Fields(tagRe.ReplaceAllString(jstr(v), " ")), " ") },
		"jquote":       func(v any) string { return strconv.Quote(jstr(v)) },
		"jsqlquote":    func(v any) string { return "'" + strings.ReplaceAll(jstr(v), "'", "''") + "'" },
		"jinclude":     func(name any) (string, error) { return includeTemplate(c, jstr(name), depth, false) },
		"jinclude_opt": func(name any) (string, error) { return includeTemplate(c, jstr(name), depth, true) },
	}
}

// includeTemplate renders another file with the same context, like Jinja's
// {% include %}. Relative names are resolved against the DAG file's folder.
func includeTemplate(c *TaskContext, name string, depth int, optional bool) (string, error) {
	if depth >= 10 {
		return "", errors.New("include: too deeply nested (a file includes itself?)")
	}
	p := name
	if !filepath.IsAbs(p) {
		base := "."
		if c.DAG != nil && c.DAG.File != "" {
			base = filepath.Dir(c.DAG.File)
		}
		p = filepath.Join(base, name)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if optional && errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("include %q: %w", name, err)
	}
	return c.renderDepth(string(b), nil, depth+1)
}

func titleCase(s string) string {
	prev := ' '
	return strings.Map(func(r rune) rune {
		defer func() { prev = r }()
		if unicode.IsSpace(prev) || prev == '-' || prev == '_' {
			return unicode.ToUpper(r)
		}
		return unicode.ToLower(r)
	}, s)
}

func upperFirst(s string) string {
	for i, r := range s {
		return string(unicode.ToUpper(r)) + s[i+len(string(r)):]
	}
	return s
}

// strftimeToGo converts the common strftime directives (macros.ds_format).
func strftimeToGo(f string) string {
	r := strings.NewReplacer("%Y", "2006", "%m", "01", "%d", "02", "%H", "15", "%M", "04", "%S", "05",
		"%y", "06", "%b", "Jan", "%B", "January", "%a", "Mon", "%A", "Monday", "%j", "002", "%f", "000000", "%z", "-0700", "%Z", "MST", "%%", "%")
	return r.Replace(f)
}
