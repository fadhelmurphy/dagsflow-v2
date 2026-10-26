package dag

import (
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// A small SQL reader for lineage: which tables a statement reads and writes,
// and for each written column the source columns it comes from. It covers
// the statements pipelines run (SELECT, CREATE TABLE/VIEW … AS, INSERT,
// MERGE, UPDATE, CTEs, subqueries, joins, UNION, *), in BigQuery and
// Redshift / PostgreSQL syntax. It does not validate SQL: what it can't
// follow (dynamic SQL, procedures) just gives less lineage.

// SQLRef is one source column.
type SQLRef struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// SQLColumn is one output (or written) column.
type SQLColumn struct {
	Name string   `json:"name"`
	From []SQLRef `json:"from"`
	Expr string   `json:"expr,omitempty"` // the expression when it is not a plain column
}

// SQLLineage of one statement.
type SQLLineage struct {
	Kind    string      `json:"kind"`   // select, create, insert, merge, update
	Target  string      `json:"target"` // written table ("" for a SELECT)
	Sources []string    `json:"sources"`
	Columns []SQLColumn `json:"columns"`
	Temp    bool        `json:"temp,omitempty"` // CREATE TEMP TABLE: lives only in this script / session
	View    bool        `json:"view,omitempty"`
}

// ColumnsOf returns the known columns of a table (for * and unqualified
// names in joins); nil when unknown.
type ColumnsOf func(table string) []string

// ParseSQLLineage reads every statement of sql. bigquery: "…" is a string
// literal (BigQuery) instead of an identifier (Redshift / PostgreSQL).
func ParseSQLLineage(sql string, bigquery bool, cols ColumnsOf) []SQLLineage {
	var out []SQLLineage
	for _, st := range splitStatements(sql, bigquery) {
		if l, ok := st.parse(cols); ok {
			out = append(out, l)
		}
	}
	return out
}

// sqlStatement is one statement, parsed on demand (so the columns of tables
// made by earlier statements can be known when it is parsed).
type sqlStatement []sqlTok

func splitStatements(sql string, bigquery bool) []sqlStatement {
	toks := sqlTokens(sql, bigquery)
	var out []sqlStatement
	start := 0
	for i := 0; i <= len(toks); i++ {
		if i == len(toks) || toks[i].kind == tPunct && toks[i].text == ";" {
			if i > start {
				out = append(out, sqlStatement(toks[start:i]))
			}
			start = i + 1
		}
	}
	return out
}

func (st sqlStatement) parse(cols ColumnsOf) (l SQLLineage, ok bool) {
	defer func() { // a statement it can't follow gives no lineage, never a crash
		if recover() != nil {
			l, ok = SQLLineage{}, false
		}
	}()
	p := &sqlParser{toks: st, cols: cols, ctes: map[string]*relation{}}
	return p.statement()
}

// ---- tokens ---------------------------------------------------------------------------

type tokKind int

const (
	tIdent  tokKind = iota // bare word
	tQIdent                // quoted identifier
	tString
	tNumber
	tPunct
)

type sqlTok struct {
	kind  tokKind
	text  string // identifier text (unquoted), punctuation, literal
	raw   string
	space bool // whitespace before
}

func sqlTokens(s string, bigquery bool) []sqlTok {
	var out []sqlTok
	space := false
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			space = true
			i++
			continue
		case c == '-' && i+1 < len(s) && s[i+1] == '-', c == '#' && bigquery:
			for i < len(s) && s[i] != '\n' {
				i++
			}
			space = true
			continue
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			j := strings.Index(s[i+2:], "*/")
			if j < 0 {
				i = len(s)
			} else {
				i += j + 4
			}
			space = true
			continue
		case c == '\'' || c == '"' && bigquery:
			j, text := readQuoted(s, i)
			out = append(out, sqlTok{kind: tString, text: text, raw: s[i:j], space: space})
			i = j
		case c == '`' || c == '"':
			q := c
			j := i + 1
			for j < len(s) && s[j] != q {
				if s[j] == '\\' && q == '`' {
					j++
				}
				j++
			}
			text := s[i+1 : min(j, len(s))]
			if q == '"' {
				text = strings.ReplaceAll(text, `""`, `"`)
			}
			out = append(out, sqlTok{kind: tQIdent, text: text, raw: s[i:min(j+1, len(s))], space: space})
			i = min(j+1, len(s))
		case c >= '0' && c <= '9' || c == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.' || s[j] == 'e' || s[j] == 'E') {
				j++
			}
			out = append(out, sqlTok{kind: tNumber, text: s[i:j], raw: s[i:j], space: space})
			i = j
		case isIdentStart(rune(c)) || c >= 0x80:
			j := i
			for j < len(s) && (isIdentPart(rune(s[j])) || s[j] >= 0x80) {
				j++
			}
			word := s[i:j]
			// r'…', b'…', rb"…": string literals with a prefix
			if j < len(s) && (s[j] == '\'' || s[j] == '"' && bigquery) && len(word) <= 2 && strings.Trim(strings.ToLower(word), "rb") == "" {
				k, text := readQuoted(s, j)
				out = append(out, sqlTok{kind: tString, text: text, raw: s[i:k], space: space})
				i = k
				break
			}
			out = append(out, sqlTok{kind: tIdent, text: word, raw: word, space: space})
			i = j
		default:
			two := ""
			if i+1 < len(s) {
				two = s[i : i+2]
			}
			switch two {
			case "<=", ">=", "<>", "!=", "||", "::", "=>", "->":
				out = append(out, sqlTok{kind: tPunct, text: two, raw: two, space: space})
				i += 2
			default:
				out = append(out, sqlTok{kind: tPunct, text: string(c), raw: string(c), space: space})
				i++
			}
		}
		space = false
	}
	return out
}

func readQuoted(s string, i int) (int, string) {
	q := s[i]
	if i+2 < len(s) && s[i+1] == q && s[i+2] == q { // '''triple'''
		end := strings.Index(s[i+3:], strings.Repeat(string(q), 3))
		if end < 0 {
			return len(s), s[i+3:]
		}
		return i + 3 + end + 3, s[i+3 : i+3+end]
	}
	var b strings.Builder
	j := i + 1
	for j < len(s) {
		if s[j] == '\\' && j+1 < len(s) {
			b.WriteByte(s[j+1])
			j += 2
			continue
		}
		if s[j] == q {
			if j+1 < len(s) && s[j+1] == q { // '' escape
				b.WriteByte(q)
				j += 2
				continue
			}
			return j + 1, b.String()
		}
		b.WriteByte(s[j])
		j++
	}
	return len(s), b.String()
}

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) || r == '@' || r == '$' }
func isIdentPart(r rune) bool {
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// ---- parser ---------------------------------------------------------------------------

type relation struct {
	cols  []SQLColumn // output columns
	star  bool        // unknown columns (a table whose columns we don't know)
	table string      // base table, when this is one
}

type scope struct {
	aliases map[string]*relation // alias / table name -> relation
	order   []string
}

type sqlParser struct {
	toks    []sqlTok
	pos     int
	cols    ColumnsOf
	ctes    map[string]*relation
	sources map[string]bool
}

func (p *sqlParser) peek(n int) sqlTok {
	if p.pos+n < len(p.toks) {
		return p.toks[p.pos+n]
	}
	return sqlTok{kind: tPunct, text: ""}
}
func (p *sqlParser) eof() bool { return p.pos >= len(p.toks) }
func (p *sqlParser) isKw(n int, words ...string) bool {
	t := p.peek(n)
	if t.kind != tIdent {
		return false
	}
	for _, w := range words {
		if strings.EqualFold(t.text, w) {
			return true
		}
	}
	return false
}
func (p *sqlParser) isP(n int, s string) bool { t := p.peek(n); return t.kind == tPunct && t.text == s }
func (p *sqlParser) acceptKw(words ...string) bool {
	if p.isKw(0, words...) {
		p.pos++
		return true
	}
	return false
}

// skipParens skips a balanced (…) starting at pos (which must be "(").
func (p *sqlParser) skipParens() {
	depth := 0
	for !p.eof() {
		t := p.toks[p.pos]
		p.pos++
		if t.kind == tPunct {
			switch t.text {
			case "(", "[":
				depth++
			case ")", "]":
				depth--
				if depth == 0 {
					return
				}
			}
		}
	}
}

// name reads a qualified name: a.b.c, `p.d.t`, my-proj.d.t.
func (p *sqlParser) name() (string, bool) {
	t := p.peek(0)
	if t.kind != tIdent && t.kind != tQIdent {
		return "", false
	}
	var parts []string
	cur := t.text
	p.pos++
	for {
		// my-project.ds.t (hyphens in BigQuery project names, unquoted)
		if p.isP(0, "-") && !p.peek(0).space && (p.peek(1).kind == tIdent || p.peek(1).kind == tNumber) && !p.peek(1).space {
			cur += "-" + p.peek(1).text
			p.pos += 2
			continue
		}
		if p.isP(0, ".") && (p.peek(1).kind == tIdent || p.peek(1).kind == tQIdent || p.isP(1, "*")) {
			if p.isP(1, "*") {
				break
			}
			parts = append(parts, cur)
			cur = p.peek(1).text
			p.pos += 2
			continue
		}
		break
	}
	parts = append(parts, cur)
	return strings.Join(parts, "."), true
}

func (p *sqlParser) statement() (SQLLineage, bool) {
	p.sources = map[string]bool{}
	var l SQLLineage
	switch {
	case p.isKw(0, "WITH", "SELECT") || p.isP(0, "("):
		rel := p.query()
		l = SQLLineage{Kind: "select", Columns: rel.cols}
	case p.isKw(0, "CREATE"):
		p.pos++
		temp := false
		for p.isKw(0, "OR", "REPLACE", "TEMP", "TEMPORARY", "EXTERNAL", "MATERIALIZED", "TRANSIENT", "LOCAL", "GLOBAL", "SNAPSHOT") {
			temp = temp || p.isKw(0, "TEMP", "TEMPORARY")
			p.pos++
		}
		view := p.isKw(0, "VIEW")
		if !p.acceptKw("TABLE", "VIEW") {
			return l, false
		}
		if p.acceptKw("IF") {
			p.acceptKw("NOT")
			p.acceptKw("EXISTS")
		}
		target, ok := p.name()
		if !ok {
			return l, false
		}
		l = SQLLineage{Kind: "create", Target: target, Temp: temp, View: view}
		var declared []string
		if p.isP(0, "(") { // column list or definitions: first word of each item
			declared = p.columnList()
		}
		for !p.eof() && !p.isKw(0, "AS") { // PARTITION BY, CLUSTER BY, OPTIONS, DISTKEY, SORTKEY, LIKE, COPY …
			if p.isP(0, "(") {
				p.skipParens()
				continue
			}
			p.pos++
		}
		if !p.acceptKw("AS") || p.eof() {
			for _, n := range declared { // CREATE TABLE t (a INT, b TEXT): the columns, no sources
				l.Columns = append(l.Columns, SQLColumn{Name: n})
			}
			return l, true // DDL without a query
		}
		rel := p.query()
		l.Columns = renameByPosition(rel.cols, declared)
	case p.isKw(0, "INSERT"):
		p.pos++
		p.acceptKw("INTO")
		p.acceptKw("TABLE")
		target, ok := p.name()
		if !ok {
			return l, false
		}
		l = SQLLineage{Kind: "insert", Target: target}
		var declared []string
		if p.isP(0, "(") && !(p.isKw(1, "SELECT", "WITH")) {
			declared = p.columnList()
		}
		if p.isKw(0, "VALUES") || p.isKw(0, "DEFAULT") {
			return l, true
		}
		rel := p.query()
		if declared == nil && p.cols != nil {
			declared = p.cols(target)
			if len(declared) < len(rel.cols) {
				declared = nil
			}
		}
		l.Columns = renameByPosition(rel.cols, declared)
	case p.isKw(0, "MERGE"):
		return p.merge()
	case p.isKw(0, "UPDATE"):
		return p.update()
	case p.isKw(0, "COPY"): // Redshift: COPY t [(cols)] FROM 's3://…'
		p.pos++
		target, ok := p.name()
		if !ok {
			return l, false
		}
		l = SQLLineage{Kind: "copy", Target: target}
		if p.isP(0, "(") {
			for _, n := range p.columnList() {
				l.Columns = append(l.Columns, SQLColumn{Name: n})
			}
		}
		if !p.acceptKw("FROM") || p.peek(0).kind != tString {
			return l, false
		}
		l.Sources = []string{p.peek(0).text}
		return l, true
	default:
		return l, false
	}
	l.Sources = sortedSet(p.sources)
	return l, true
}

// columnList reads (a, b, c) or (a INT64, b STRING …): the first name of each item.
func (p *sqlParser) columnList() []string {
	var out []string
	p.pos++ // (
	depth := 1
	expect := true
	for !p.eof() && depth > 0 {
		t := p.toks[p.pos]
		p.pos++
		switch {
		case t.kind == tPunct && (t.text == "(" || t.text == "<"):
			depth++
		case t.kind == tPunct && (t.text == ")" || t.text == ">"):
			depth--
		case t.kind == tPunct && t.text == "," && depth == 1:
			expect = true
		case expect && depth == 1 && (t.kind == tIdent || t.kind == tQIdent):
			if !(t.kind == tIdent && isConstraintWord(t.text)) {
				out = append(out, t.text)
			}
			expect = false
		}
	}
	return out
}

func isConstraintWord(w string) bool {
	switch strings.ToUpper(w) {
	case "PRIMARY", "FOREIGN", "CONSTRAINT", "UNIQUE", "CHECK", "LIKE":
		return true
	}
	return false
}

func renameByPosition(cols []SQLColumn, names []string) []SQLColumn {
	if len(names) == 0 {
		return cols
	}
	out := make([]SQLColumn, 0, len(cols))
	for i, c := range cols {
		if i < len(names) {
			c.Name = names[i]
		}
		out = append(out, c)
	}
	return out
}

// query: [WITH …] select (UNION|INTERSECT|EXCEPT … select)* [ORDER BY …] [LIMIT …]
func (p *sqlParser) query() *relation {
	if p.acceptKw("WITH") {
		p.acceptKw("RECURSIVE")
		for !p.eof() {
			name, ok := p.name()
			if !ok {
				break
			}
			var declared []string
			if p.isP(0, "(") {
				declared = p.columnList()
			}
			p.acceptKw("AS")
			p.acceptKw("NOT")
			p.acceptKw("MATERIALIZED")
			if !p.isP(0, "(") {
				break
			}
			p.pos++
			rel := p.query()
			p.closeParen()
			rel.cols = renameByPosition(rel.cols, declared)
			p.ctes[strings.ToLower(name)] = rel
			if !p.isP(0, ",") {
				break
			}
			p.pos++
		}
	}
	rel := p.selectOrParen()
	for p.isKw(0, "UNION", "INTERSECT", "EXCEPT", "MINUS") {
		p.pos++
		p.acceptKw("ALL", "DISTINCT")
		other := p.selectOrParen()
		for i := range rel.cols {
			if i < len(other.cols) {
				rel.cols[i].From = mergeRefs(rel.cols[i].From, other.cols[i].From)
			}
		}
	}
	// ORDER BY / LIMIT / OFFSET after a set operation: skip to the end of this query
	p.skipToQueryEnd()
	return rel
}

func (p *sqlParser) closeParen() {
	depth := 0
	for !p.eof() {
		t := p.toks[p.pos]
		if t.kind == tPunct {
			switch t.text {
			case "(":
				depth++
			case ")":
				if depth == 0 {
					p.pos++
					return
				}
				depth--
			}
		}
		p.pos++
	}
}

// skipToQueryEnd stops before ")" closing the query, ";" or the end, at depth 0.
func (p *sqlParser) skipToQueryEnd() {
	depth := 0
	for !p.eof() {
		t := p.toks[p.pos]
		if t.kind == tPunct {
			switch t.text {
			case "(":
				depth++
			case ")":
				if depth == 0 {
					return
				}
				depth--
			}
		}
		if depth == 0 && t.kind == tIdent && (strings.EqualFold(t.text, "UNION") || strings.EqualFold(t.text, "INTERSECT") ||
			strings.EqualFold(t.text, "EXCEPT") || strings.EqualFold(t.text, "WHEN") || strings.EqualFold(t.text, "ON")) {
			return
		}
		p.pos++
	}
}

func (p *sqlParser) selectOrParen() *relation {
	if p.isP(0, "(") {
		p.pos++
		rel := p.query()
		p.closeParen()
		return rel
	}
	if !p.acceptKw("SELECT") {
		return &relation{}
	}
	p.acceptKw("DISTINCT", "ALL")
	if p.isKw(0, "AS") && p.isKw(1, "STRUCT", "VALUE") {
		p.pos += 2
	}
	if p.acceptKw("TOP") {
		p.pos++
	}
	// the select list: remember its tokens, read FROM first (aliases), then resolve
	itemsStart := p.pos
	depth := 0
	for !p.eof() {
		t := p.toks[p.pos]
		if t.kind == tPunct && (t.text == "(" || t.text == "[") {
			depth++
		} else if t.kind == tPunct && (t.text == ")" || t.text == "]") {
			if depth == 0 {
				break
			}
			depth--
		} else if depth == 0 && t.kind == tIdent && isClauseWord(t.text) &&
			!(strings.EqualFold(t.text, "EXCEPT") && p.pos > 0 && p.toks[p.pos-1].text == "*" && p.isP(1, "(")) {
			break
		}
		p.pos++
	}
	itemToks := p.toks[itemsStart:p.pos]
	sc := &scope{aliases: map[string]*relation{}}
	if p.acceptKw("FROM") {
		p.from(sc)
	}
	// WHERE / GROUP BY / … : read subqueries there for table sources, skip the rest
	p.restOfSelect(sc)
	return &relation{cols: p.selectItems(itemToks, sc)}
}

func isClauseWord(w string) bool {
	switch strings.ToUpper(w) {
	case "FROM", "WHERE", "GROUP", "HAVING", "QUALIFY", "WINDOW", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "MINUS", "INTO", "FETCH":
		return true
	}
	return false
}

// from reads table references and joins into the scope.
func (p *sqlParser) from(sc *scope) {
	for !p.eof() {
		p.tableRef(sc)
		for {
			// joins
			if p.isP(0, ",") {
				p.pos++
				break
			}
			if p.isKw(0, "LEFT", "RIGHT", "FULL", "INNER", "CROSS", "OUTER", "NATURAL", "JOIN", "SEMI", "ANTI") {
				for p.acceptKw("LEFT", "RIGHT", "FULL", "INNER", "CROSS", "OUTER", "NATURAL", "SEMI", "ANTI") {
				}
				p.acceptKw("JOIN")
				break
			}
			if p.acceptKw("ON") {
				p.skipExpr(sc)
				continue
			}
			if p.acceptKw("USING") {
				if p.isP(0, "(") {
					p.skipParens()
				}
				continue
			}
			return
		}
	}
}

// skipExpr skips a join condition, reading subqueries it contains.
func (p *sqlParser) skipExpr(sc *scope) {
	depth := 0
	for !p.eof() {
		t := p.toks[p.pos]
		if t.kind == tPunct {
			switch t.text {
			case "(":
				if p.isKw(1, "SELECT", "WITH") {
					p.pos++
					p.query()
					p.closeParen()
					continue
				}
				depth++
			case ")":
				if depth == 0 {
					return
				}
				depth--
			case ",", ";":
				if depth == 0 {
					return
				}
			}
		}
		if depth == 0 && t.kind == tIdent && (isClauseWord(t.text) || strings.EqualFold(t.text, "JOIN") || strings.EqualFold(t.text, "LEFT") ||
			strings.EqualFold(t.text, "RIGHT") || strings.EqualFold(t.text, "FULL") || strings.EqualFold(t.text, "INNER") ||
			strings.EqualFold(t.text, "CROSS") || strings.EqualFold(t.text, "WHEN") || strings.EqualFold(t.text, "NATURAL")) {
			return
		}
		p.pos++
	}
}

func (p *sqlParser) tableRef(sc *scope) {
	var rel *relation
	defName := ""
	switch {
	case p.isP(0, "(") && p.isKw(1, "SELECT", "WITH") || p.isP(0, "(") && p.isP(1, "("):
		p.pos++
		rel = p.query()
		p.closeParen()
	case p.isP(0, "("): // ( a JOIN b ) : read inside into the same scope
		p.pos++
		p.from(sc)
		p.closeParen()
		return
	case p.isKw(0, "UNNEST", "LATERAL", "TABLE") && p.isP(1, "("):
		p.pos++
		p.skipParens()
		rel = &relation{star: true}
	default:
		n, ok := p.name()
		if !ok {
			p.pos++
			return
		}
		if p.isP(0, "(") { // table function
			p.skipParens()
			rel = &relation{star: true}
			break
		}
		defName = n
		if cte := p.ctes[strings.ToLower(n)]; cte != nil {
			rel = cte
		} else {
			p.sources[n] = true
			rel = &relation{table: n}
			if p.cols != nil {
				for _, c := range p.cols(n) {
					rel.cols = append(rel.cols, SQLColumn{Name: c, From: []SQLRef{{n, c}}})
				}
			}
			if len(rel.cols) == 0 {
				rel.star = true
			}
		}
		// FOR SYSTEM_TIME AS OF …, TABLESAMPLE …
		if p.isKw(0, "FOR") && p.isKw(1, "SYSTEM_TIME", "SYSTEM") {
			for !p.eof() && !p.isKw(0, "OF") {
				p.pos++
			}
			p.pos++
			p.skipExpr(sc)
		}
	}
	alias := ""
	if p.acceptKw("AS") {
		if t := p.peek(0); t.kind == tIdent || t.kind == tQIdent {
			alias = t.text
			p.pos++
		}
	} else if t := p.peek(0); (t.kind == tIdent && !isReservedAfterTable(t.text)) || t.kind == tQIdent {
		alias = t.text
		p.pos++
	}
	if p.isP(0, "(") && alias != "" { // alias(col, …)
		names := p.columnList()
		rel = &relation{cols: renameByPosition(rel.cols, names), table: rel.table}
	}
	if p.acceptKw("TABLESAMPLE") && p.isKw(0, "SYSTEM", "BERNOULLI") {
		p.pos++
		if p.isP(0, "(") {
			p.skipParens()
		}
	}
	keys := []string{}
	if alias != "" {
		keys = append(keys, alias)
	}
	if defName != "" {
		keys = append(keys, defName)
		if i := strings.LastIndex(defName, "."); i >= 0 {
			keys = append(keys, defName[i+1:])
		}
	}
	if len(keys) == 0 {
		keys = []string{"$" + strconv.Itoa(len(sc.order))}
	}
	for _, k := range keys {
		sc.aliases[strings.ToLower(k)] = rel
	}
	sc.order = append(sc.order, strings.ToLower(keys[0]))
}

func isReservedAfterTable(w string) bool {
	switch strings.ToUpper(w) {
	case "WHERE", "GROUP", "HAVING", "QUALIFY", "WINDOW", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "MINUS",
		"LEFT", "RIGHT", "FULL", "INNER", "CROSS", "OUTER", "JOIN", "ON", "USING", "NATURAL", "WHEN", "SET", "FOR", "TABLESAMPLE",
		"SEMI", "ANTI", "FETCH", "VALUES", "SELECT", "WITH":
		return true
	}
	return false
}

// restOfSelect skips WHERE … ORDER BY …, reading subqueries for their tables.
func (p *sqlParser) restOfSelect(sc *scope) {
	depth := 0
	for !p.eof() {
		t := p.toks[p.pos]
		if t.kind == tPunct {
			switch t.text {
			case "(":
				if p.isKw(1, "SELECT", "WITH") {
					p.pos++
					p.query()
					p.closeParen()
					continue
				}
				depth++
			case ")":
				if depth == 0 {
					return
				}
				depth--
			}
		}
		if depth == 0 && t.kind == tIdent && (strings.EqualFold(t.text, "UNION") || strings.EqualFold(t.text, "INTERSECT") ||
			strings.EqualFold(t.text, "EXCEPT") || strings.EqualFold(t.text, "MINUS") || strings.EqualFold(t.text, "WHEN")) {
			return
		}
		p.pos++
	}
}

// selectItems resolves the select list against the FROM scope.
func (p *sqlParser) selectItems(toks []sqlTok, sc *scope) []SQLColumn {
	var out []SQLColumn
	for i, item := range splitTop(toks) {
		if len(item) == 0 {
			continue
		}
		// * and alias.* (with EXCEPT (…) / REPLACE (…))
		if star, qual, except := starItem(item); star {
			for _, key := range sc.order {
				if qual != "" && !strings.EqualFold(key, qual) && sc.aliases[strings.ToLower(qual)] != sc.aliases[key] {
					continue
				}
				rel := sc.aliases[key]
				if rel == nil {
					continue
				}
				for _, c := range rel.cols {
					if !except[strings.ToLower(c.Name)] {
						out = append(out, c)
					}
				}
				if qual != "" {
					break
				}
			}
			continue
		}
		expr, alias := splitAlias(item)
		col := SQLColumn{Name: alias}
		refs := p.refs(expr, sc)
		col.From = refs
		if len(expr) == 1 || isDottedName(expr) {
			if col.Name == "" {
				col.Name = expr[len(expr)-1].text
			}
		} else {
			col.Expr = rawText(expr)
		}
		if col.Name == "" {
			col.Name = "f" + strconv.Itoa(i) + "_"
		}
		out = append(out, col)
	}
	return out
}

func rawText(toks []sqlTok) string {
	var b strings.Builder
	for i, t := range toks {
		if i > 0 && t.space {
			b.WriteByte(' ')
		}
		b.WriteString(t.raw)
	}
	s := b.String()
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}

func isDottedName(toks []sqlTok) bool {
	if len(toks)%2 == 0 {
		return false
	}
	for i, t := range toks {
		if i%2 == 0 && t.kind != tIdent && t.kind != tQIdent {
			return false
		}
		if i%2 == 1 && !(t.kind == tPunct && t.text == ".") {
			return false
		}
	}
	return true
}

func splitTop(toks []sqlTok) [][]sqlTok {
	var out [][]sqlTok
	depth, start := 0, 0
	for i, t := range toks {
		if t.kind == tPunct {
			switch t.text {
			case "(", "[":
				depth++
			case ")", "]":
				depth--
			case ",":
				if depth == 0 {
					out = append(out, toks[start:i])
					start = i + 1
				}
			}
		}
		if t.kind == tIdent && depth == 0 && strings.EqualFold(t.text, "CASE") {
			depth += 1000 // CASE … END has commas only inside functions, but be safe
		}
		if t.kind == tIdent && depth >= 1000 && strings.EqualFold(t.text, "END") {
			depth -= 1000
		}
	}
	if start < len(toks) {
		out = append(out, toks[start:])
	}
	return out
}

func starItem(item []sqlTok) (bool, string, map[string]bool) {
	except := map[string]bool{}
	n := 0
	qual := ""
	switch {
	case len(item) >= 1 && item[0].kind == tPunct && item[0].text == "*":
		n = 1
	case len(item) >= 3 && item[len(item)-1].text == "*" && item[len(item)-2].text == "." && !hasKw(item, "EXCEPT", "REPLACE"):
		qual = item[len(item)-3].text
		n = len(item)
	default:
		// t.* EXCEPT (a, b)
		for i := 0; i+2 < len(item); i++ {
			if item[i+1].text == "." && item[i+2].text == "*" {
				qual, n = item[i].text, i+3
				break
			}
		}
		if n == 0 {
			return false, "", nil
		}
	}
	rest := item[n:]
	if len(rest) > 0 && rest[0].kind == tIdent && strings.EqualFold(rest[0].text, "EXCEPT") {
		for _, t := range rest[1:] {
			if t.kind == tIdent || t.kind == tQIdent {
				except[strings.ToLower(t.text)] = true
			}
		}
	}
	return true, qual, except
}

func hasKw(toks []sqlTok, words ...string) bool {
	for _, t := range toks {
		if t.kind == tIdent {
			for _, w := range words {
				if strings.EqualFold(t.text, w) {
					return true
				}
			}
		}
	}
	return false
}

// splitAlias: "expr AS a" / "expr a" -> expr, a
func splitAlias(item []sqlTok) ([]sqlTok, string) {
	n := len(item)
	if n >= 3 && item[n-2].kind == tIdent && strings.EqualFold(item[n-2].text, "AS") && (item[n-1].kind == tIdent || item[n-1].kind == tQIdent) {
		return item[:n-2], item[n-1].text
	}
	if n >= 2 && (item[n-1].kind == tIdent || item[n-1].kind == tQIdent) && !isExprKeyword(item[n-1].text) {
		prev := item[n-2]
		if !(prev.kind == tPunct && prev.text == ".") && !(prev.kind == tPunct && (prev.text == "::")) &&
			!(prev.kind == tIdent && isExprKeyword(prev.text)) {
			return item[:n-1], item[n-1].text
		}
	}
	return item, ""
}

var exprKeywords = map[string]bool{}

func init() {
	for _, w := range strings.Fields(`AND OR NOT NULL TRUE FALSE CASE WHEN THEN ELSE END AS IN IS LIKE ILIKE BETWEEN CAST SAFE_CAST TRY_CAST
		INTERVAL DATE TIME TIMESTAMP DATETIME YEAR QUARTER MONTH WEEK DAY HOUR MINUTE SECOND MILLISECOND MICROSECOND DAYOFWEEK DAYOFYEAR ISOWEEK ISOYEAR
		DISTINCT OVER PARTITION BY ORDER ASC DESC NULLS FIRST LAST ROWS RANGE UNBOUNDED PRECEDING FOLLOWING CURRENT ROW
		EXTRACT FROM AT ZONE ESCAPE SIMILAR TO ANY ALL SOME EXISTS IGNORE RESPECT LIMIT WITHIN GROUP FILTER WHERE
		STRING INT64 INTEGER INT BIGINT SMALLINT FLOAT64 FLOAT DOUBLE PRECISION NUMERIC DECIMAL BIGNUMERIC BOOL BOOLEAN BYTES JSON
		VARCHAR CHAR TEXT ARRAY STRUCT GEOGRAPHY RANGE REAL TIMESTAMPTZ SUPER VARBYTE WITH WITHOUT CHARACTER VARYING SEPARATOR
		SELECT CURRENT_DATE CURRENT_TIMESTAMP CURRENT_DATETIME CURRENT_TIME SESSION_USER CURRENT_USER LOCALTIMESTAMP SYSDATE GETDATE`) {
		exprKeywords[w] = true
	}
}

func isExprKeyword(w string) bool { return exprKeywords[strings.ToUpper(w)] }

// refs finds the columns an expression reads and resolves them to tables.
func (p *sqlParser) refs(expr []sqlTok, sc *scope) []SQLRef {
	var out []SQLRef
	for i := 0; i < len(expr); i++ {
		t := expr[i]
		if t.kind == tPunct && t.text == "(" && i+1 < len(expr) && expr[i+1].kind == tIdent && (strings.EqualFold(expr[i+1].text, "SELECT") || strings.EqualFold(expr[i+1].text, "WITH")) {
			// scalar subquery: its tables are sources; its first column feeds this one
			j := matchParen(expr, i)
			sub := &sqlParser{toks: expr[i+1 : j], cols: p.cols, ctes: p.ctes, sources: p.sources}
			rel := sub.query()
			if len(rel.cols) > 0 {
				out = mergeRefs(out, rel.cols[0].From)
			}
			i = j
			continue
		}
		if t.kind != tIdent && t.kind != tQIdent {
			continue
		}
		if t.kind == tIdent && isExprKeyword(t.text) {
			// DATE '…', CAST(x AS type): the type word is skipped as a keyword
			continue
		}
		// part of a.b.c?
		if i > 0 && expr[i-1].kind == tPunct && expr[i-1].text == "." {
			continue
		}
		parts := []string{t.text}
		j := i + 1
		for j+1 < len(expr) && expr[j].kind == tPunct && expr[j].text == "." && (expr[j+1].kind == tIdent || expr[j+1].kind == tQIdent) {
			parts = append(parts, expr[j+1].text)
			j += 2
		}
		if j < len(expr) && expr[j].kind == tPunct && expr[j].text == "(" {
			i = j - 1 // a function name
			continue
		}
		if i > 0 && expr[i-1].kind == tPunct && expr[i-1].text == "::" {
			i = j - 1 // ::type
			continue
		}
		if i > 0 && expr[i-1].kind == tIdent && strings.EqualFold(expr[i-1].text, "AS") {
			i = j - 1 // CAST(x AS type)
			continue
		}
		if j < len(expr) && expr[j].kind == tPunct && expr[j].text == "=>" {
			i = j // named argument
			continue
		}
		if strings.HasPrefix(t.text, "@") { // query parameter
			i = j - 1
			continue
		}
		out = mergeRefs(out, p.resolve(parts, sc))
		i = j - 1
	}
	return out
}

func matchParen(toks []sqlTok, i int) int {
	depth := 0
	for j := i; j < len(toks); j++ {
		if toks[j].kind == tPunct {
			switch toks[j].text {
			case "(":
				depth++
			case ")":
				depth--
				if depth == 0 {
					return j
				}
			}
		}
	}
	return len(toks) - 1
}

// resolve a.b / b against the scope: alias.column, table.column or a column
// of one of the sources (through subqueries and CTEs, down to base tables).
func (p *sqlParser) resolve(parts []string, sc *scope) []SQLRef {
	if len(parts) >= 2 {
		if rel := sc.aliases[strings.ToLower(parts[0])]; rel != nil {
			return relCol(rel, parts[1])
		}
		// project.dataset.table.column (rare): try the longest table prefix
		for k := len(parts) - 1; k >= 2; k-- {
			if rel := sc.aliases[strings.ToLower(strings.Join(parts[:k], "."))]; rel != nil {
				return relCol(rel, parts[k])
			}
		}
	}
	name := parts[0]
	var hits []*relation
	var unknown []*relation
	for _, key := range sc.order {
		rel := sc.aliases[key]
		if rel == nil {
			continue
		}
		if hasCol(rel, name) {
			hits = append(hits, rel)
		} else if rel.star {
			unknown = append(unknown, rel)
		}
	}
	if len(hits) == 0 {
		hits = unknown
	}
	if len(hits) == 0 && len(sc.order) == 0 {
		return nil
	}
	var out []SQLRef
	for _, rel := range hits {
		out = mergeRefs(out, relCol(rel, name))
	}
	return out
}

func hasCol(rel *relation, name string) bool {
	for _, c := range rel.cols {
		if strings.EqualFold(c.Name, name) {
			return true
		}
	}
	return false
}

func relCol(rel *relation, name string) []SQLRef {
	for _, c := range rel.cols {
		if strings.EqualFold(c.Name, name) {
			return c.From
		}
	}
	if rel.table != "" {
		return []SQLRef{{rel.table, name}}
	}
	return nil
}

func mergeRefs(a, b []SQLRef) []SQLRef {
	for _, r := range b {
		dup := false
		for _, x := range a {
			if strings.EqualFold(x.Table, r.Table) && strings.EqualFold(x.Column, r.Column) {
				dup = true
				break
			}
		}
		if !dup {
			a = append(a, r)
		}
	}
	return a
}

// MERGE [INTO] t [AS] a USING src [AS] b ON … WHEN … THEN UPDATE SET … | INSERT (…) VALUES (…) | INSERT ROW
func (p *sqlParser) merge() (SQLLineage, bool) {
	p.pos++
	p.acceptKw("INTO")
	target, ok := p.name()
	if !ok {
		return SQLLineage{}, false
	}
	l := SQLLineage{Kind: "merge", Target: target}
	sc := &scope{aliases: map[string]*relation{}}
	tgt := &relation{table: target}
	tAlias := ""
	if p.acceptKw("AS") || p.peek(0).kind == tIdent && !p.isKw(0, "USING") || p.peek(0).kind == tQIdent {
		tAlias = p.peek(0).text
		p.pos++
	}
	if !p.acceptKw("USING") {
		return l, false
	}
	p.tableRef(sc)
	srcKey := sc.order[len(sc.order)-1]
	src := sc.aliases[srcKey]
	if tAlias != "" {
		sc.aliases[strings.ToLower(tAlias)] = tgt
	}
	sc.aliases[strings.ToLower(target)] = tgt
	if i := strings.LastIndex(target, "."); i >= 0 {
		sc.aliases[strings.ToLower(target[i+1:])] = tgt
	}
	// only source columns feed the target: resolve unqualified names in the source
	srcOnly := &scope{aliases: sc.aliases, order: []string{srcKey}}
	cols := map[string]*SQLColumn{}
	var order []string
	add := func(name string, refs []SQLRef, expr string) {
		k := strings.ToLower(name)
		var keep []SQLRef
		for _, r := range refs {
			if !strings.EqualFold(r.Table, target) {
				keep = append(keep, r)
			}
		}
		if c := cols[k]; c != nil {
			c.From = mergeRefs(c.From, keep)
			if c.Expr == "" {
				c.Expr = expr
			}
			return
		}
		cols[k] = &SQLColumn{Name: name, From: keep, Expr: expr}
		order = append(order, k)
	}
	for !p.eof() {
		switch {
		case p.acceptKw("SET"):
			for _, item := range splitTop(p.untilWhen()) {
				eq := indexOf(item, "=")
				if eq <= 0 {
					continue
				}
				name := item[eq-1].text
				expr := item[eq+1:]
				e := ""
				if !(len(expr) == 1 || isDottedName(expr)) {
					e = rawText(expr)
				}
				add(name, p.refs(expr, srcOnly), e)
			}
		case p.isKw(0, "INSERT"):
			p.pos++
			if p.acceptKw("ROW") {
				for _, c := range src.cols {
					add(c.Name, c.From, "")
				}
				continue
			}
			var names []string
			if p.isP(0, "(") {
				names = p.columnList()
			}
			if p.acceptKw("VALUES") && p.isP(0, "(") {
				start := p.pos + 1
				p.skipParens()
				for i, item := range splitTop(p.toks[start : p.pos-1]) {
					if i < len(names) {
						e := ""
						if !(len(item) == 1 || isDottedName(item)) {
							e = rawText(item)
						}
						add(names[i], p.refs(item, srcOnly), e)
					}
				}
			}
		default:
			p.pos++
		}
	}
	for _, k := range order {
		l.Columns = append(l.Columns, *cols[k])
	}
	l.Sources = sortedSet(p.sources)
	return l, true
}

func (p *sqlParser) untilWhen() []sqlTok {
	start := p.pos
	depth := 0
	for !p.eof() {
		t := p.toks[p.pos]
		if t.kind == tPunct {
			if t.text == "(" {
				depth++
			} else if t.text == ")" {
				depth--
			}
		}
		if depth == 0 && t.kind == tIdent && (strings.EqualFold(t.text, "WHEN") || strings.EqualFold(t.text, "FROM") || strings.EqualFold(t.text, "WHERE")) {
			break
		}
		p.pos++
	}
	return p.toks[start:p.pos]
}

func indexOf(toks []sqlTok, punct string) int {
	depth := 0
	for i, t := range toks {
		if t.kind == tPunct {
			switch t.text {
			case "(":
				depth++
			case ")":
				depth--
			case punct:
				if depth == 0 {
					return i
				}
			}
		}
	}
	return -1
}

// UPDATE t [AS a] SET c = expr, … [FROM …] [WHERE …]
func (p *sqlParser) update() (SQLLineage, bool) {
	p.pos++
	target, ok := p.name()
	if !ok {
		return SQLLineage{}, false
	}
	l := SQLLineage{Kind: "update", Target: target}
	sc := &scope{aliases: map[string]*relation{}}
	tgt := &relation{table: target, star: true}
	if p.cols != nil {
		for _, c := range p.cols(target) {
			tgt.cols = append(tgt.cols, SQLColumn{Name: c, From: []SQLRef{{target, c}}})
		}
	}
	keys := []string{strings.ToLower(target)}
	if p.acceptKw("AS") || p.peek(0).kind == tIdent && !p.isKw(0, "SET") {
		keys = append(keys, strings.ToLower(p.peek(0).text))
		p.pos++
	}
	if !p.acceptKw("SET") {
		return l, false
	}
	setToks := p.untilWhen()
	if p.acceptKw("FROM") {
		p.from(sc)
	}
	for _, k := range keys {
		sc.aliases[k] = tgt
	}
	sc.order = append(sc.order, keys[0])
	for _, item := range splitTop(setToks) {
		eq := indexOf(item, "=")
		if eq <= 0 {
			continue
		}
		expr := item[eq+1:]
		var keep []SQLRef
		for _, r := range p.refs(expr, sc) {
			if !strings.EqualFold(r.Table, target) {
				keep = append(keep, r)
			}
		}
		if len(keep) == 0 {
			continue
		}
		e := ""
		if !(len(expr) == 1 || isDottedName(expr)) {
			e = rawText(expr)
		}
		l.Columns = append(l.Columns, SQLColumn{Name: item[eq-1].text, From: keep, Expr: e})
	}
	l.Sources = sortedSet(p.sources)
	return l, true
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
