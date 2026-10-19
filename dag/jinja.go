package dag

import (
	"regexp"
	"strings"
)

// Airflow templates use Jinja; dagsflow uses Go's text/template. To let SQL
// and bash files be copied from Airflow/Composer unchanged, the most common
// Jinja forms are translated before rendering:
//
//	{{ ds }}  {{ ds_nodash }}  {{ ts }}  {{ run_id }}      -> {{ .ds }} ...
//	{{ params.project_id }}                               -> {{ .params.project_id }}
//	{{ dag_run.conf.x }} / {{ dag_run.conf['x'] }}        -> {{ .conf.x }}
//	{{ var.value.region }}                                -> {{ var "region" }}
//	{{ macros.ds_add(ds, -1) }}                           -> {{ ds_add .ds -1 }}
//	{{ ti.xcom_pull(task_ids='a', key='k') }}             -> {{ xcom_pull "a" "k" }}
//
// Go-style templates are left untouched ({{ .ds }} keeps working), so both
// styles can be mixed. Statements ({% if %}, {% for %}, {% set %}, {% include %},
// {% raw %}), comments, operators and filters are translated in jinja_stmt.go.

var (
	actionRe  = regexp.MustCompile(`\{\{-?(.*?)-?\}\}`)
	dsAddRe   = regexp.MustCompile(`macros\.ds_add\(\s*(\.?[A-Za-z_][\w.]*)\s*,\s*(-?\d+)\s*\)`)
	xcomRe    = regexp.MustCompile(`(?:ti|task_instance)\.xcom_pull\(\s*(?:task_ids\s*=\s*)?['"]([^'"]+)['"]\s*(?:,\s*key\s*=\s*['"]([^'"]+)['"])?\s*\)`)
	varRe     = regexp.MustCompile(`\bvar\.(?:value|json)\.([A-Za-z_][\w-]*)`)
	confIdxRe = regexp.MustCompile(`dag_run\.conf\[\s*['"]([^'"]+)['"]\s*\]`)
	identRe   = regexp.MustCompile(`[A-Za-z_]\w*`)
)

// jinjaCompat translates a template (Jinja statements, filters and
// expressions; Go-style actions are kept) and falls back to the per-action
// rewrite if the template can't be translated. known is the set of top-level
// template variables.
func jinjaCompat(raw string, known map[string]bool) string {
	s, err := jinjaTranslate(raw, known)
	if err != nil {
		return legacyCompat(raw, known)
	}
	return s
}

// jinjaTranslate is jinjaCompat with errors (unclosed blocks, unsupported
// statements or filters), used when rendering.
func jinjaTranslate(raw string, known map[string]bool) (string, error) {
	if !strings.Contains(raw, "{{") && !strings.Contains(raw, "{%") && !strings.Contains(raw, "{#") {
		return raw, nil
	}
	return translateJinja(raw, known, func(action string) string { return legacyCompat(action, known) })
}

// legacyCompat rewrites Jinja-style variable references in every {{ }} action.
func legacyCompat(raw string, known map[string]bool) string {
	if !strings.Contains(raw, "{{") {
		return raw
	}
	return actionRe.ReplaceAllStringFunc(raw, func(action string) string {
		open, close := "{{", "}}"
		body := action[2 : len(action)-2]
		if strings.HasPrefix(body, "-") {
			open, body = "{{-", body[1:]
		}
		if strings.HasSuffix(body, "-") {
			close, body = "-}}", body[:len(body)-1]
		}
		body = confIdxRe.ReplaceAllString(body, "conf.$1")
		body = strings.ReplaceAll(body, "dag_run.conf", "conf")
		body = dsAddRe.ReplaceAllString(body, "ds_add $1 $2")
		body = xcomRe.ReplaceAllStringFunc(body, func(m string) string {
			sm := xcomRe.FindStringSubmatch(m)
			key := sm[2]
			if key == "" {
				key = ReturnKey
			}
			return `xcom_pull "` + sm[1] + `" "` + key + `"`
		})
		body = varRe.ReplaceAllString(body, `var "$1"`)
		return open + dotIdents(body, known) + close
	})
}

// dotIdents prefixes known bare identifiers with "." while leaving string
// literals, fields (.x), variables ($x) and function names alone.
func dotIdents(body string, known map[string]bool) string {
	var b strings.Builder
	for i := 0; i < len(body); {
		c := body[i]
		if c == '"' || c == '`' || c == '\'' { // copy literals verbatim
			j := i + 1
			for j < len(body) && body[j] != c {
				if body[j] == '\\' && c != '`' {
					j++
				}
				j++
			}
			if j < len(body) {
				j++
			}
			b.WriteString(body[i:j])
			i = j
			continue
		}
		if loc := identRe.FindStringIndex(body[i:]); loc != nil && loc[0] == 0 {
			word := body[i : i+loc[1]]
			prev := byte(' ')
			if i > 0 {
				prev = body[i-1]
			}
			if known[word] && prev != '.' && prev != '$' && !isWordByte(prev) {
				b.WriteByte('.')
			}
			b.WriteString(word)
			i += loc[1]
			continue
		}
		b.WriteByte(c)
		i++
	}
	return b.String()
}

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
