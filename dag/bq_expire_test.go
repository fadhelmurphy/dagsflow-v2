package dag

import (
	"testing"
	"time"
)

func TestParseExpiration(t *testing.T) {
	now := time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)
	for in, want := range map[string]time.Duration{"7d": 7 * 24 * time.Hour, "30": 30 * 24 * time.Hour, "12h": 12 * time.Hour, "90m": 90 * time.Minute, "2w": 14 * 24 * time.Hour, " 3 D ": 3 * 24 * time.Hour} {
		got, err := parseExpiration(in, now)
		if err != nil || got.Sub(now) != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	if got, err := parseExpiration("2026-12-31T23:00:00+07:00", now); err != nil || got.UTC().Format(time.RFC3339) != "2026-12-31T16:00:00Z" {
		t.Errorf("rfc3339: %v %v", got, err)
	}
	if _, err := parseExpiration("2026-12-31", now); err != nil {
		t.Error(err)
	}
	for _, bad := range []string{"0d", "soon", "2025-01-01", "7y"} {
		if _, err := parseExpiration(bad, now); err == nil {
			t.Errorf("%q should fail", bad)
		}
	}
	dd, err := ParseYAML("a.yaml", []byte("dag_id: a\ntasks:\n  - {id: x, operator: bigquery_load_table, table: d.t, expiration: soon}\n"))
	if err == nil {
		err = dd[0].validate()
	}
	if err == nil {
		t.Fatal("bad expiration should fail")
	}
}
