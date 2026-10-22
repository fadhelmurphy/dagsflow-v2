package dag

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Table expiration for the BigQuery nodes: a duration ("7d", "12h", "90m",
// "2w"; a bare number is days) or a date / timestamp ("2026-12-31",
// "2026-12-31T23:00:00+07:00"). It goes into the table OPTIONS
// (expiration_timestamp); a duration counts from the run that (re)creates the
// table, or for a load table from the last load, since every load renews it.

var expDurRe = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*(m|min|mins|minutes?|h|hours?|d|days?|w|weeks?)?$`)

// parseExpiration resolves the value to a moment, counting durations from now.
func parseExpiration(v string, now time.Time) (time.Time, error) {
	s := strings.ToLower(strings.TrimSpace(v))
	if m := expDurRe.FindStringSubmatch(s); m != nil {
		n, _ := strconv.ParseFloat(m[1], 64)
		unit := 24 * time.Hour // bare number = days
		switch {
		case m[2] == "":
		case m[2][0] == 'm':
			unit = time.Minute
		case m[2][0] == 'h':
			unit = time.Hour
		case m[2][0] == 'w':
			unit = 7 * 24 * time.Hour
		}
		d := time.Duration(n * float64(unit))
		if d < time.Minute {
			return time.Time{}, fmt.Errorf("expiration %q must be at least a minute", v)
		}
		return now.Add(d), nil
	}
	for _, f := range []string{time.RFC3339, "2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(f, strings.TrimSpace(v), time.Local); err == nil {
			if !t.After(now) {
				return time.Time{}, fmt.Errorf("expiration %q is in the past", v)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("expiration %q: use a duration (7d, 12h, 90m, 2w) or a date (2026-12-31)", v)
}

// expirationSQL returns the value for OPTIONS(expiration_timestamp = …):
// durations stay relative to the moment BigQuery runs the statement.
func expirationSQL(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", nil
	}
	now := time.Now()
	t, err := parseExpiration(v, now)
	if err != nil {
		return "", err
	}
	if expDurRe.MatchString(strings.ToLower(v)) {
		return fmt.Sprintf("TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL %d MINUTE)", int64(t.Sub(now).Round(time.Minute)/time.Minute)), nil
	}
	return "TIMESTAMP " + bqStr(t.UTC().Format("2006-01-02 15:04:05")+" UTC"), nil
}

func checkExpiration(d *DAG, id, v string) {
	if strings.Contains(v, "{{") {
		return
	}
	if _, err := expirationSQL(v); err != nil {
		d.buildErr(fmt.Errorf("task %s: %v", id, err))
	}
}

// renderExpiration renders templates in the value, then converts it.
func renderExpiration(ctx *TaskContext, v string) (string, error) {
	if strings.Contains(v, "{{") {
		r, err := ctx.Render(v, nil)
		if err != nil {
			return "", fmt.Errorf("expiration: %w", err)
		}
		v = r
	}
	return expirationSQL(v)
}
