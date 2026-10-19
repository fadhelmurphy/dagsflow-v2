package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Login lockout shared by every webserver replica: failed logins are kept in
// the metadata store (kv_table "login_failures", a row-locked transaction on
// Postgres), so a password guesser can't spread attempts over replicas.
const (
	LoginWindow   = 15 * time.Minute
	LoginMaxFails = 5
	loginMaxKeys  = 20000 // bound the table under a flood of usernames/IPs
)

const loginTable = "login_failures"

// Keys are hashed: the table holds no usernames or IP addresses.
func loginKey(k string) string {
	h := sha256.Sum256([]byte("dagsflow-login\x00" + k))
	return hex.EncodeToString(h[:12])
}

func pruneFails(ts []time.Time, now time.Time) []time.Time {
	kept := ts[:0]
	for _, t := range ts {
		if now.Sub(t) < LoginWindow {
			kept = append(kept, t)
		}
	}
	return kept
}

// LoginBlocked returns how long any of the keys (e.g. "u:alice", "ip:1.2.3.4")
// is locked out: LoginMaxFails failures within LoginWindow.
func (s *Store) LoginBlocked(keys ...string) (time.Duration, error) {
	m := map[string][]time.Time{}
	if err := s.readTable(loginTable, &m); err != nil {
		return 0, err
	}
	now := time.Now()
	var wait time.Duration
	for _, k := range keys {
		h := pruneFails(m[loginKey(k)], now)
		if len(h) >= LoginMaxFails {
			if w := LoginWindow - now.Sub(h[len(h)-LoginMaxFails]); w > wait {
				wait = w
			}
		}
	}
	return wait, nil
}

// LoginFailed records a failed login for every key.
func (s *Store) LoginFailed(keys ...string) error {
	m := map[string][]time.Time{}
	return s.updateTable(loginTable, &m, func() error {
		now := time.Now()
		for k, ts := range m {
			if ts = pruneFails(ts, now); len(ts) == 0 {
				delete(m, k)
			} else {
				m[k] = ts
			}
		}
		if len(m) > loginMaxKeys {
			for k := range m {
				delete(m, k)
			}
		}
		for _, k := range keys {
			hk := loginKey(k)
			m[hk] = append(m[hk], now)
		}
		return nil
	})
}

// LoginSucceeded clears the failures of the keys (skips the write when there
// is nothing to clear).
func (s *Store) LoginSucceeded(keys ...string) error {
	cur := map[string][]time.Time{}
	if err := s.readTable(loginTable, &cur); err != nil {
		return err
	}
	found := false
	for _, k := range keys {
		if _, ok := cur[loginKey(k)]; ok {
			found = true
		}
	}
	if !found {
		return nil
	}
	m := map[string][]time.Time{}
	return s.updateTable(loginTable, &m, func() error {
		for _, k := range keys {
			delete(m, loginKey(k))
		}
		return nil
	})
}
