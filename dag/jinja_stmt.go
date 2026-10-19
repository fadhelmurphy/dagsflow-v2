package dag

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Jinja → Go template translation for the parts that need more than a regex:
// expressions with operators and filters, and statements
//
//	{% if %} {% elif %} {% else %} {% endif %}
//	{% for x in xs %} ... {% else %} ... {% endfor %}   (loop.index, loop.first, ...)
//	{% set x = ... %}   {% include "file.sql" %}   {% raw %}...{% endraw %}   {# comment #}
//
// A {{ ... }} that does not parse as Jinja (a Go-style action such as
// {{ .ds }} or {{ xcom_pull "a" "k" }}) goes through the older rewrite, so
// both styles keep working side by side.

// ---- tokenizer of the template --------------------------------------------------

type tchunk struct {
	kind       byte // 't' text, 'e' {{ }}, 's' {% %}
	body       string
	trimL, trR bool
}

func splitTemplate(raw string) ([]tchunk, error) {
	var out []tchunk
	for len(raw) > 0 {
		i := strings.IndexAny(raw, "{")
		for i >= 0 && (i+1 >= len(raw) || !strings.ContainsRune("{%#", rune(raw[i+1]))) {
			j := strings.IndexAny(raw[i+1:], "{")
			if j < 0 {
				i = -1
				break
			}
			i += j + 1
		}
		if i < 0 {
			out = append(out, tchunk{kind: 't', body: raw})
			break
		}
		if i > 0 {
			out = append(out, tchunk{kind: 't', body: raw[:i]})
		}
		open := raw[i+1]
		closeTok := map[byte]string{'{': "}}", '%': "%}", '#': "#}"}[open]
		rest := raw[i+2:]
		end := findClose(rest, closeTok)
		if end < 0 {
			return nil, fmt.Errorf("unclosed %q", raw[i:i+2])
		}
		body := rest[:end]
		raw = rest[end+2:]
		if open == '#' {
			continue // comment
		}
		c := tchunk{kind: 'e', body: body}
		if open == '%' {
			c.kind = 's'
		}
		if strings.HasPrefix(c.body, "-") {
			c.trimL, c.body = true, c.body[1:]
		}
		if strings.HasSuffix(c.body, "-") {
			c.trR, c.body = true, c.body[:len(c.body)-1]
		}
		if c.kind == 's' && strings.TrimSpace(c.body) == "raw" {
			e := strings.Index(raw, "endraw")
			for e >= 0 {
				st := strings.LastIndex(raw[:e], "{%")
				if st >= 0 && strings.Trim(raw[st+2:e], " -") == "" {
					cl := strings.Index(raw[e:], "%}")
					if cl < 0 {
						break
					}
					out = append(out, tchunk{kind: 'r', body: raw[:st]})
					raw = raw[e+cl+2:]
					break
				}
				nx := strings.Index(raw[e+6:], "endraw")
				if nx < 0 {
					e = -1
				} else {
					e += 6 + nx
				}
			}
			if e < 0 {
				return nil, errors.New("{% raw %} without {% endraw %}")
			}
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

// findClose finds closeTok outside string literals.
func findClose(s, closeTok string) int {
	var q byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case q != 0:
			if c == '\\' {
				i++
			} else if c == q {
				q = 0
			}
		case c == '"' || c == '\'' || c == '`':
			q = c
		case strings.HasPrefix(s[i:], closeTok):
			return i
		}
	}
	return -1
}

// ---- expression lexer ------------------------------------------------------------

type jtok struct {
	k  string // id, num, str, op, eof
	v  string
	sp bool // preceded by whitespace
}

func lexJinja(s string) ([]jtok, error) {
	var out []jtok
	sp := false
	add := func(t jtok) { t.sp = sp; sp = false; out = append(out, t) }
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			sp = true
			i++
		case c == '"' || c == '\'':
			j := i + 1
			var b strings.Builder
			for j < len(s) && s[j] != c {
				if s[j] == '\\' && j+1 < len(s) {
					j++
					switch s[j] {
					case 'n':
						b.WriteByte('\n')
					case 't':
						b.WriteByte('\t')
					default:
						b.WriteByte(s[j])
					}
					j++
					continue
				}
				b.WriteByte(s[j])
				j++
			}
			if j >= len(s) {
				return nil, errors.New("unclosed string")
			}
			add(jtok{k: "str", v: b.String()})
			i = j + 1
		case c >= '0' && c <= '9':
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.' && j+1 < len(s) && s[j+1] >= '0' && s[j+1] <= '9') {
				j++
			}
			add(jtok{k: "num", v: s[i:j]})
			i = j
		case c == '_' || unicode.IsLetter(rune(c)):
			j := i
			for j < len(s) && (s[j] == '_' || unicode.IsLetter(rune(s[j])) || s[j] >= '0' && s[j] <= '9') {
				j++
			}
			add(jtok{k: "id", v: s[i:j]})
			i = j
		default:
			two := ""
			if i+1 < len(s) {
				two = s[i : i+2]
			}
			switch two {
			case "==", "!=", "<=", ">=", "//", "**":
				add(jtok{k: "op", v: two})
				i += 2
				continue
			}
			if strings.ContainsRune("+-*/%~<>()[]{}.,|:=", rune(c)) {
				add(jtok{k: "op", v: string(c)})
				i++
				continue
			}
			return nil, fmt.Errorf("unexpected %q", c) // e.g. "$" or a Go-style "." start
		}
	}
	return append(out, jtok{k: "eof"}), nil
}

// ---- parser → AST ------------------------------------------------------------------

type jnode struct {
	op   string // lit, name, attr, index, call, filter, bin, not, neg, test, cond, list
	v    string // literal text (already a Go literal) / name / attr / filter / op
	args []*jnode
	kw   map[string]*jnode
}

type jparser struct {
	t []jtok
	i int
}

func (p *jparser) peek() jtok { return p.t[p.i] }
func (p *jparser) next() jtok { t := p.t[p.i]; p.i++; return t }
func (p *jparser) is(k, v string) bool {
	t := p.peek()
	return t.k == k && t.v == v
}
func (p *jparser) accept(k, v string) bool {
	if p.is(k, v) {
		p.i++
		return true
	}
	return false
}
func (p *jparser) expect(k, v string) error {
	if !p.accept(k, v) {
		return fmt.Errorf("expected %q, got %q", v, p.peek().v)
	}
	return nil
}

func parseJinjaExpr(s string) (*jnode, error) {
	toks, err := lexJinja(s)
	if err != nil {
		return nil, err
	}
	p := &jparser{t: toks}
	n, err := p.expr()
	if err != nil {
		return nil, err
	}
	if p.peek().k != "eof" {
		return nil, fmt.Errorf("unexpected %q", p.peek().v)
	}
	return n, nil
}

func (p *jparser) expr() (*jnode, error) {
	a, err := p.or()
	if err != nil {
		return nil, err
	}
	if p.accept("id", "if") {
		c, err := p.or()
		if err != nil {
			return nil, err
		}
		var b *jnode = &jnode{op: "lit", v: `""`}
		if p.accept("id", "else") {
			if b, err = p.expr(); err != nil {
				return nil, err
			}
		}
		return &jnode{op: "cond", args: []*jnode{c, a, b}}, nil
	}
	return a, nil
}

func (p *jparser) binLoop(sub func() (*jnode, error), ops map[string]string) (*jnode, error) {
	a, err := sub()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		name, ok := ops[t.v]
		if !ok || (t.k != "op" && t.k != "id") {
			return a, nil
		}
		p.i++
		b, err := sub()
		if err != nil {
			return nil, err
		}
		a = &jnode{op: "bin", v: name, args: []*jnode{a, b}}
	}
}

func (p *jparser) or() (*jnode, error) {
	return p.binLoop(p.and, map[string]string{"or": "or"})
}
func (p *jparser) and() (*jnode, error) {
	return p.binLoop(p.not, map[string]string{"and": "and"})
}
func (p *jparser) not() (*jnode, error) {
	if p.accept("id", "not") {
		a, err := p.not()
		if err != nil {
			return nil, err
		}
		return &jnode{op: "not", args: []*jnode{a}}, nil
	}
	return p.cmp()
}

func (p *jparser) cmp() (*jnode, error) {
	a, err := p.concat()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		var op string
		switch {
		case t.k == "op" && (t.v == "==" || t.v == "!=" || t.v == "<" || t.v == "<=" || t.v == ">" || t.v == ">="):
			op = map[string]string{"==": "jeq", "!=": "jne", "<": "jlt", "<=": "jle", ">": "jgt", ">=": "jge"}[t.v]
			p.i++
		case t.k == "id" && t.v == "in":
			op = "jin"
			p.i++
		case t.k == "id" && t.v == "not" && p.t[p.i+1].k == "id" && p.t[p.i+1].v == "in":
			p.i += 2
			b, err := p.concat()
			if err != nil {
				return nil, err
			}
			a = &jnode{op: "not", args: []*jnode{{op: "bin", v: "jin", args: []*jnode{a, b}}}}
			continue
		case t.k == "id" && t.v == "is":
			p.i++
			neg := p.accept("id", "not")
			tt := p.next()
			if tt.k != "id" {
				return nil, errors.New("expected a test name after 'is'")
			}
			n := &jnode{op: "test", v: tt.v, args: []*jnode{a}}
			if tt.v == "divisibleby" || tt.v == "sameas" || tt.v == "equalto" || tt.v == "eq" {
				arg, err := p.postfix()
				if err != nil {
					return nil, err
				}
				n.args = append(n.args, arg)
			}
			if neg {
				n = &jnode{op: "not", args: []*jnode{n}}
			}
			a = n
			continue
		default:
			return a, nil
		}
		b, err := p.concat()
		if err != nil {
			return nil, err
		}
		a = &jnode{op: "bin", v: op, args: []*jnode{a, b}}
	}
}

func (p *jparser) concat() (*jnode, error) {
	return p.binLoop(p.add, map[string]string{"~": "jconcat"})
}
func (p *jparser) add() (*jnode, error) {
	return p.binLoop(p.mul, map[string]string{"+": "jadd", "-": "jsub"})
}
func (p *jparser) mul() (*jnode, error) {
	return p.binLoop(p.unary, map[string]string{"*": "jmul", "/": "jdiv", "//": "jfloordiv", "%": "jmod"})
}
func (p *jparser) unary() (*jnode, error) {
	if p.accept("op", "-") {
		a, err := p.unary()
		if err != nil {
			return nil, err
		}
		if a.op == "lit" && (a.v[0] >= '0' && a.v[0] <= '9') {
			a.v = "-" + a.v
			return a, nil
		}
		return &jnode{op: "neg", args: []*jnode{a}}, nil
	}
	return p.postfix()
}

func (p *jparser) postfix() (*jnode, error) {
	a, err := p.primary()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.is("op", ".") && !p.peek().sp: // "len .rows" is a Go call, not len.rows
			p.i++
			t := p.next()
			if t.k != "id" && t.k != "num" {
				return nil, errors.New("expected a name after '.'")
			}
			a = &jnode{op: "attr", v: t.v, args: []*jnode{a}}
		case p.accept("op", "["):
			k, err := p.expr()
			if err != nil {
				return nil, err
			}
			if err := p.expect("op", "]"); err != nil {
				return nil, err
			}
			a = &jnode{op: "index", args: []*jnode{a, k}}
		case p.is("op", "("):
			args, kw, err := p.callArgs()
			if err != nil {
				return nil, err
			}
			a = &jnode{op: "call", args: append([]*jnode{a}, args...), kw: kw}
		case p.accept("op", "|"):
			t := p.next()
			if t.k != "id" {
				return nil, errors.New("expected a filter name after '|'")
			}
			f := &jnode{op: "filter", v: t.v, args: []*jnode{a}}
			if p.is("op", "(") {
				args, kw, err := p.callArgs()
				if err != nil {
					return nil, err
				}
				f.args, f.kw = append(f.args, args...), kw
			}
			a = f
		default:
			return a, nil
		}
	}
}

func (p *jparser) callArgs() ([]*jnode, map[string]*jnode, error) {
	_ = p.expect("op", "(")
	var args []*jnode
	kw := map[string]*jnode{}
	for !p.accept("op", ")") {
		if p.peek().k == "id" && p.t[p.i+1].k == "op" && p.t[p.i+1].v == "=" {
			name := p.next().v
			p.i++
			v, err := p.expr()
			if err != nil {
				return nil, nil, err
			}
			kw[name] = v
		} else {
			v, err := p.expr()
			if err != nil {
				return nil, nil, err
			}
			args = append(args, v)
		}
		if !p.accept("op", ",") {
			if err := p.expect("op", ")"); err != nil {
				return nil, nil, err
			}
			break
		}
	}
	return args, kw, nil
}

func (p *jparser) primary() (*jnode, error) {
	t := p.next()
	switch t.k {
	case "num":
		return &jnode{op: "lit", v: t.v}, nil
	case "str":
		return &jnode{op: "lit", v: strconv.Quote(t.v)}, nil
	case "id":
		switch t.v {
		case "true", "True":
			return &jnode{op: "lit", v: "true"}, nil
		case "false", "False":
			return &jnode{op: "lit", v: "false"}, nil
		case "none", "None", "nil":
			return &jnode{op: "lit", v: "nil"}, nil
		case "range":
			if !p.is("op", "(") {
				return nil, fmt.Errorf("unexpected %q", t.v) // Go-style {{ range ... }}
			}
		case "if", "else", "and", "or", "not", "in", "is", "end", "with", "define", "template", "block":
			return nil, fmt.Errorf("unexpected %q", t.v) // Go-style action
		}
		return &jnode{op: "name", v: t.v}, nil
	case "op":
		switch t.v {
		case "(":
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			return e, p.expect("op", ")")
		case "[":
			l := &jnode{op: "list"}
			for !p.accept("op", "]") {
				e, err := p.expr()
				if err != nil {
					return nil, err
				}
				l.args = append(l.args, e)
				if !p.accept("op", ",") {
					if err := p.expect("op", "]"); err != nil {
						return nil, err
					}
					break
				}
			}
			return l, nil
		}
	}
	return nil, fmt.Errorf("unexpected %q", t.v)
}

// ---- code generation ---------------------------------------------------------------

type jscope struct {
	vars  map[string]string // jinja name -> Go variable
	loops []jloop
}

type jloop struct{ idx, coll string }

type jgen struct {
	known map[string]bool
	sc    []jscope
	n     int
}

func (g *jgen) local(name string) (string, bool) {
	for i := len(g.sc) - 1; i >= 0; i-- {
		if v, ok := g.sc[i].vars[name]; ok {
			return v, true
		}
	}
	return "", false
}

func (g *jgen) inLoop() bool {
	for _, s := range g.sc {
		if len(s.loops) > 0 {
			return true
		}
	}
	return false
}

func (g *jgen) loop() (jloop, bool) {
	for i := len(g.sc) - 1; i >= 0; i-- {
		if l := g.sc[i].loops; len(l) > 0 {
			return l[len(l)-1], true
		}
	}
	return jloop{}, false
}

// root returns how top-level data is reached ("." changes inside range).
func (g *jgen) root() string {
	if g.inLoop() {
		return "$."
	}
	return "."
}

func paren(s string) string {
	if strings.ContainsAny(s, " ") && !(strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") && balanced(s[1:len(s)-1])) {
		return "(" + s + ")"
	}
	return s
}

func balanced(s string) bool {
	d := 0
	var q byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if q != 0 {
			if c == '\\' {
				i++
			} else if c == q {
				q = 0
			}
			continue
		}
		switch c {
		case '"', '`':
			q = c
		case '(':
			d++
		case ')':
			d--
			if d < 0 {
				return false
			}
		}
	}
	return d == 0
}

// chain returns (root, keys) when n is a plain name/attr/index chain.
func (g *jgen) chain(n *jnode) (string, []string, bool) {
	switch n.op {
	case "name":
		if v, ok := g.local(n.v); ok {
			return v, nil, true
		}
		switch n.v {
		case "dag_run", "loop", "var", "macros", "ti", "task_instance":
			return "", nil, false
		}
		return "$", []string{strconv.Quote(n.v)}, true
	case "attr":
		r, k, ok := g.chain(n.args[0])
		if !ok {
			return "", nil, false
		}
		return r, append(k, strconv.Quote(n.v)), true
	case "index":
		r, k, ok := g.chain(n.args[0])
		if !ok || n.args[1].op != "lit" {
			return "", nil, false
		}
		return r, append(k, n.args[1].v), true
	}
	return "", nil, false
}

// safe generates an access that yields nil instead of failing when a key is
// missing (Jinja's "undefined"), for default / is defined / conditions.
func (g *jgen) safe(n *jnode) (string, error) {
	if n.op == "attr" && n.args[0].op == "name" && n.args[0].v == "dag_run" && n.v == "conf" {
		return g.root() + "conf", nil
	}
	if r, keys, ok := g.chain(n); ok && len(keys) > 0 {
		return "jget " + r + " " + strings.Join(keys, " "), nil
	}
	return g.gen(n)
}

func (g *jgen) gen(n *jnode) (string, error) {
	switch n.op {
	case "lit":
		return n.v, nil
	case "list":
		parts := []string{"jlist"}
		for _, a := range n.args {
			s, err := g.gen(a)
			if err != nil {
				return "", err
			}
			parts = append(parts, paren(s))
		}
		return strings.Join(parts, " "), nil
	case "name":
		if v, ok := g.local(n.v); ok {
			return v, nil
		}
		if n.v == "loop" {
			return "", errors.New("use loop.index, loop.index0, loop.first, loop.last or loop.length")
		}
		return g.root() + n.v, nil
	case "attr":
		base := n.args[0]
		if base.op == "name" {
			switch base.v {
			case "dag_run":
				if n.v == "conf" {
					return g.root() + "conf", nil
				}
				if n.v == "run_id" {
					return g.root() + "run_id", nil
				}
			case "loop":
				l, ok := g.loop()
				if !ok {
					return "", errors.New("loop.* outside a for loop")
				}
				switch n.v {
				case "index0":
					return l.idx, nil
				case "index":
					return "jadd " + l.idx + " 1", nil
				case "first":
					return "jeq " + l.idx + " 0", nil
				case "last":
					return "jislast " + l.idx + " " + paren(l.coll), nil
				case "length":
					return "len " + paren(l.coll), nil
				case "revindex":
					return "jsub (len " + paren(l.coll) + ") " + l.idx, nil
				}
				return "", fmt.Errorf("loop.%s is not supported", n.v)
			case "var":
				// var.value.x / var.json.x handled one level up
			}
		}
		if base.op == "attr" && base.args[0].op == "name" && base.args[0].v == "var" {
			switch base.v {
			case "value":
				return `var "` + n.v + `"`, nil
			case "json":
				return `jfromjson (var "` + n.v + `")`, nil
			}
		}
		b, err := g.gen(base)
		if err != nil {
			return "", err
		}
		if !strings.ContainsAny(b, " ()") && (strings.HasPrefix(b, ".") || strings.HasPrefix(b, "$")) && isIdent(n.v) {
			if b == "." {
				return "." + n.v, nil
			}
			return b + "." + n.v, nil
		}
		return "index " + paren(b) + " " + strconv.Quote(n.v), nil
	case "index":
		b, err := g.gen(n.args[0])
		if err != nil {
			return "", err
		}
		if n.args[1].op == "lit" && strings.HasPrefix(n.args[1].v, `"`) {
			key, _ := strconv.Unquote(n.args[1].v)
			if !strings.ContainsAny(b, " ()") && (strings.HasPrefix(b, ".") || strings.HasPrefix(b, "$")) && isIdent(key) {
				return b + "." + key, nil
			}
		}
		k, err := g.gen(n.args[1])
		if err != nil {
			return "", err
		}
		return "jindex " + paren(b) + " " + paren(k), nil
	case "call":
		return g.call(n)
	case "filter":
		return g.filter(n)
	case "bin":
		a, err := g.operand(n.args[0], n.v)
		if err != nil {
			return "", err
		}
		b, err := g.operand(n.args[1], n.v)
		if err != nil {
			return "", err
		}
		return n.v + " " + paren(a) + " " + paren(b), nil
	case "not":
		a, err := g.operand(n.args[0], "not")
		if err != nil {
			return "", err
		}
		return "not " + paren(a), nil
	case "neg":
		a, err := g.gen(n.args[0])
		if err != nil {
			return "", err
		}
		return "jsub 0 " + paren(a), nil
	case "cond":
		c, err := g.operand(n.args[0], "if")
		if err != nil {
			return "", err
		}
		a, err := g.gen(n.args[1])
		if err != nil {
			return "", err
		}
		b, err := g.gen(n.args[2])
		if err != nil {
			return "", err
		}
		return "jif " + paren(c) + " " + paren(a) + " " + paren(b), nil
	case "test":
		x, err := g.safe(n.args[0])
		if err != nil {
			return "", err
		}
		switch n.v {
		case "defined":
			return "jdefined " + paren(x), nil
		case "undefined":
			return "not (jdefined " + paren(x) + ")", nil
		case "none":
			return "jeq " + paren(x) + " nil", nil
		case "string", "number", "mapping", "sequence", "iterable", "true", "false", "odd", "even", "empty":
			return "jtest " + strconv.Quote(n.v) + " " + paren(x), nil
		case "divisibleby", "sameas", "equalto", "eq":
			y, err := g.gen(n.args[1])
			if err != nil {
				return "", err
			}
			if n.v == "divisibleby" {
				return "jeq (jmod " + paren(x) + " " + paren(y) + ") 0", nil
			}
			return "jeq " + paren(x) + " " + paren(y), nil
		}
		return "", fmt.Errorf("unknown test %q", n.v)
	}
	return "", fmt.Errorf("cannot translate %s", n.op)
}

// operand: inside conditions and/or/not, a missing key is falsy, like Jinja.
func (g *jgen) operand(n *jnode, op string) (string, error) {
	switch op {
	case "and", "or", "not", "if", "jin":
		return g.safe(n)
	}
	return g.gen(n)
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if !(r == '_' || unicode.IsLetter(r) || i > 0 && unicode.IsDigit(r)) {
			return false
		}
	}
	return true
}

func (g *jgen) args(ns []*jnode) ([]string, error) {
	var out []string
	for _, a := range ns {
		s, err := g.gen(a)
		if err != nil {
			return nil, err
		}
		out = append(out, paren(s))
	}
	return out, nil
}

func (g *jgen) call(n *jnode) (string, error) {
	fn := n.args[0]
	args := n.args[1:]
	name := ""
	switch fn.op {
	case "name":
		name = fn.v
	case "attr":
		if fn.args[0].op == "name" {
			name = fn.args[0].v + "." + fn.v
		} else if fn.v == "get" || fn.v == "items" || fn.v == "keys" || fn.v == "values" || fn.v == "upper" || fn.v == "lower" || fn.v == "strip" || fn.v == "replace" || fn.v == "split" || fn.v == "startswith" || fn.v == "endswith" {
			name = "." + fn.v
		}
	}
	if strings.HasSuffix(name, ".get") || strings.HasSuffix(name, ".items") || strings.HasSuffix(name, ".keys") || strings.HasSuffix(name, ".values") ||
		strings.HasSuffix(name, ".upper") || strings.HasSuffix(name, ".lower") || strings.HasSuffix(name, ".strip") || strings.HasSuffix(name, ".replace") ||
		strings.HasSuffix(name, ".split") || strings.HasSuffix(name, ".startswith") || strings.HasSuffix(name, ".endswith") {
		if !(fn.args[0].op == "name" && (fn.args[0].v == "macros" || fn.args[0].v == "ti" || fn.args[0].v == "task_instance")) {
			method := fn.v
			recv, err := g.gen(fn.args[0])
			if method == "get" {
				recv, err = g.safe(fn.args[0])
			}
			if err != nil {
				return "", err
			}
			as, err := g.args(args)
			if err != nil {
				return "", err
			}
			switch method {
			case "get":
				def := "nil"
				if len(as) > 1 {
					def = as[1]
				}
				if len(as) == 0 {
					return "", errors.New(".get needs a key")
				}
				return "jgetor " + paren(recv) + " " + as[0] + " " + def, nil
			case "items", "keys", "values":
				return "j" + method + " " + paren(recv), nil
			case "upper", "lower":
				return method + " " + paren(recv), nil
			case "strip":
				return "trim " + paren(recv), nil
			case "replace":
				if len(as) != 2 {
					return "", errors.New(".replace needs two arguments")
				}
				return "replace " + as[0] + " " + as[1] + " " + paren(recv), nil
			case "split":
				sep := `" "`
				if len(as) > 0 {
					sep = as[0]
				}
				return "jsplit " + sep + " " + paren(recv), nil
			case "startswith", "endswith":
				if len(as) != 1 {
					return "", fmt.Errorf(".%s needs one argument", method)
				}
				return "j" + method + " " + as[0] + " " + paren(recv), nil
			}
		}
	}
	as, err := g.args(args)
	if err != nil {
		return "", err
	}
	kw := func(k string) (string, error) {
		if v, ok := n.kw[k]; ok {
			s, err := g.gen(v)
			return paren(s), err
		}
		return "", nil
	}
	switch name {
	case "ti.xcom_pull", "task_instance.xcom_pull":
		task, err := kw("task_ids")
		if err != nil {
			return "", err
		}
		key, err := kw("key")
		if err != nil {
			return "", err
		}
		if task == "" && len(as) > 0 {
			task = as[0]
		}
		if key == "" && len(as) > 1 {
			key = as[1]
		}
		if key == "" {
			key = `"` + ReturnKey + `"`
		}
		if task == "" {
			return "xcom " + key, nil
		}
		return "xcom_pull " + task + " " + key, nil
	case "macros.ds_add":
		if len(as) != 2 {
			return "", errors.New("macros.ds_add needs (ds, days)")
		}
		return "ds_add " + as[0] + " " + as[1], nil
	case "macros.ds_format":
		if len(as) != 3 {
			return "", errors.New("macros.ds_format needs (ds, input_format, output_format)")
		}
		return "ds_format " + strings.Join(as, " "), nil
	case "macros.datetime.now", "macros.datetime.utcnow":
		return "jnow", nil
	case "range":
		return "jrange " + strings.Join(as, " "), nil
	case "dict":
		var parts []string
		for k, v := range n.kw {
			s, err := g.gen(v)
			if err != nil {
				return "", err
			}
			parts = append(parts, strconv.Quote(k), paren(s))
		}
		return strings.TrimSpace("jdict " + strings.Join(parts, " ")), nil
	case "var", "xcom_pull", "xcom", "json", "ds_add", "len", "upper", "lower", "trim":
		return strings.TrimSpace(name + " " + strings.Join(as, " ")), nil
	}
	if fn.op == "name" && jinjaFilterNames[fn.v] {
		return strings.TrimSpace(fn.v + " " + strings.Join(as, " ")), nil
	}
	return "", fmt.Errorf("function %q is not supported in templates", strings.TrimPrefix(name, "."))
}

// filters: the value goes last (Go pipelines pass it as the last argument).
var jinjaFilterNames = map[string]bool{"upper": true, "lower": true, "trim": true, "title": true, "capitalize": true,
	"default": true, "d": true, "replace": true, "int": true, "float": true, "string": true, "length": true, "count": true,
	"join": true, "first": true, "last": true, "tojson": true, "round": true, "abs": true, "list": true, "sort": true,
	"unique": true, "reverse": true, "sum": true, "min": true, "max": true, "truncate": true, "indent": true,
	"striptags": true, "safe": true, "e": true, "escape": true, "quote": true, "sqlquote": true, "fromjson": true,
	"map": true, "select": true, "reject": true, "batch": true, "format": true, "center": true, "wordcount": true}

func (g *jgen) filter(n *jnode) (string, error) {
	name := n.v
	if !jinjaFilterNames[name] {
		return "", fmt.Errorf("filter %q is not supported", name)
	}
	var val string
	var err error
	if name == "default" || name == "d" {
		val, err = g.safe(n.args[0])
	} else {
		val, err = g.gen(n.args[0])
	}
	if err != nil {
		return "", err
	}
	as, err := g.args(n.args[1:])
	if err != nil {
		return "", err
	}
	if v, ok := n.kw["attribute"]; ok && (name == "map" || name == "sort" || name == "sum" || name == "min" || name == "max" || name == "unique") {
		s, err := g.gen(v)
		if err != nil {
			return "", err
		}
		as = append([]string{`"attribute"`, paren(s)}, as...)
	}
	if v, ok := n.kw["reverse"]; ok && name == "sort" {
		s, err := g.gen(v)
		if err != nil {
			return "", err
		}
		as = append(as, `"reverse"`, paren(s))
	}
	switch name {
	case "d":
		name = "default"
	case "count":
		name = "length"
	case "e", "escape":
		name = "jescape"
	case "safe":
		return val, nil
	case "list":
		name = "jtolist"
	case "map", "select", "reject", "sort", "unique", "reverse", "sum", "min", "max", "batch", "truncate", "indent", "format", "center", "wordcount", "round", "abs", "first", "last", "striptags", "fromjson", "quote", "sqlquote":
		name = "j" + name
	}
	return strings.TrimSpace(name+" "+strings.Join(as, " ")) + " " + paren(val), nil
}

// ---- statements & whole-template translation ---------------------------------------

// translateJinja rewrites a template with Jinja statements/expressions into Go
// template syntax. legacy translates one {{ }} body that isn't Jinja.
func translateJinja(raw string, known map[string]bool, legacy func(string) string) (string, error) {
	chunks, err := splitTemplate(raw)
	if err != nil {
		return "", err
	}
	g := &jgen{known: known, sc: []jscope{{vars: map[string]string{}}}}
	var b strings.Builder
	type block struct{ kind string }
	var stack []block
	open := func(c tchunk) string {
		if c.trimL {
			return "{{- "
		}
		return "{{ "
	}
	closeT := func(c tchunk) string {
		if c.trR {
			return " -}}"
		}
		return " }}"
	}
	for _, c := range chunks {
		switch c.kind {
		case 't':
			b.WriteString(c.body)
		case 'r':
			b.WriteString("{{ " + strconv.Quote(c.body) + " }}")
		case 'e':
			body := strings.TrimSpace(c.body)
			n, perr := parseJinjaExpr(body)
			if perr != nil {
				b.WriteString(legacy(map[bool]string{true: "{{-", false: "{{"}[c.trimL] + c.body + map[bool]string{true: "-}}", false: "}}"}[c.trR]))
				continue
			}
			s, gerr := g.gen(n)
			if gerr != nil {
				return "", fmt.Errorf("{{ %s }}: %v", body, gerr)
			}
			b.WriteString(open(c) + s + closeT(c))
		case 's':
			body := strings.TrimSpace(c.body)
			word, rest, _ := strings.Cut(body, " ")
			rest = strings.TrimSpace(rest)
			switch word {
			case "if", "elif":
				n, err := parseJinjaExpr(rest)
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				s, err := g.operand(n, "if")
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				if word == "if" {
					stack = append(stack, block{"if"})
					g.sc = append(g.sc, jscope{vars: map[string]string{}})
					b.WriteString(open(c) + "if " + s + closeT(c))
				} else {
					if len(stack) == 0 || stack[len(stack)-1].kind != "if" {
						return "", errors.New("{% elif %} without {% if %}")
					}
					b.WriteString(open(c) + "else if " + s + closeT(c))
				}
			case "else":
				if len(stack) == 0 {
					return "", errors.New("{% else %} without {% if %} or {% for %}")
				}
				b.WriteString(open(c) + "else" + closeT(c))
			case "endif", "endfor":
				want := strings.TrimPrefix(word, "end")
				if len(stack) == 0 || stack[len(stack)-1].kind != want {
					return "", fmt.Errorf("{%% %s %%} without {%% %s %%}", word, want)
				}
				stack = stack[:len(stack)-1]
				g.sc = g.sc[:len(g.sc)-1]
				b.WriteString(open(c) + "end" + closeT(c))
			case "for":
				target, coll, ok := strings.Cut(rest, " in ")
				if !ok {
					return "", fmt.Errorf("{%% %s %%}: expected 'for x in ...'", body)
				}
				names := strings.Split(target, ",")
				for i := range names {
					names[i] = strings.TrimSpace(names[i])
					if !isIdent(names[i]) {
						return "", fmt.Errorf("{%% %s %%}: bad loop variable %q", body, names[i])
					}
				}
				coll = strings.TrimSpace(coll)
				n, err := parseJinjaExpr(coll)
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				// for k, v in d.items() → range over the map
				if len(names) == 2 && n.op == "call" && n.args[0].op == "attr" && n.args[0].v == "items" && len(n.args) == 1 {
					n = n.args[0].args[0]
				}
				cs, err := g.safe(n)
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				g.n++
				idx := fmt.Sprintf("$jl%d", g.n)
				sc := jscope{vars: map[string]string{}}
				var rng string
				if len(names) == 2 {
					sc.vars[names[0]], sc.vars[names[1]] = "$"+names[0], "$"+names[1]
					idx = "$" + names[0]
					rng = "range $" + names[0] + ", $" + names[1] + " := " + cs
				} else {
					sc.vars[names[0]] = "$" + names[0]
					rng = "range " + idx + ", $" + names[0] + " := " + cs
				}
				sc.loops = []jloop{{idx: idx, coll: cs}}
				stack = append(stack, block{"for"})
				g.sc = append(g.sc, sc)
				b.WriteString(open(c) + rng + closeT(c))
			case "set":
				name, expr, ok := strings.Cut(rest, "=")
				name = strings.TrimSpace(name)
				if !ok || !isIdent(name) {
					return "", fmt.Errorf("{%% %s %%}: expected 'set name = value' (block set is not supported)", body)
				}
				n, err := parseJinjaExpr(strings.TrimSpace(expr))
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				s, err := g.gen(n)
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				assign := " := "
				if _, exists := g.sc[len(g.sc)-1].vars[name]; exists {
					assign = " = "
				} else if _, outer := g.local(name); outer {
					assign = " = "
				}
				g.sc[len(g.sc)-1].vars[name] = "$" + name
				b.WriteString(open(c) + "$" + name + assign + s + closeT(c))
			case "include":
				n, err := parseJinjaExpr(strings.TrimSuffix(strings.TrimSuffix(rest, "ignore missing"), " "))
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				s, err := g.gen(n)
				if err != nil {
					return "", fmt.Errorf("{%% %s %%}: %v", body, err)
				}
				fn := "jinclude"
				if strings.HasSuffix(rest, "ignore missing") {
					fn = "jinclude_opt"
				}
				b.WriteString(open(c) + fn + " " + paren(s) + closeT(c))
			case "":
				return "", errors.New("empty {% %}")
			default:
				return "", fmt.Errorf("Jinja statement {%% %s %%} is not supported (supported: if, elif, else, for, set, include, raw)", word)
			}
		}
	}
	if len(stack) > 0 {
		return "", fmt.Errorf("{%% %s %%} is not closed (missing {%% end%s %%})", stack[len(stack)-1].kind, stack[len(stack)-1].kind)
	}
	return b.String(), nil
}
