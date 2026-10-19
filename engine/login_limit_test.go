package engine

import (
	"sync"
	"testing"
)

// Two webservers (two Store handles on the same metadata) see one lockout.
func testSharedLockout(t *testing.T, a, b *Store) {
	t.Helper()
	keys := []string{"u:alice", "ip:10.0.0.9"}
	for i := 0; i < LoginMaxFails-1; i++ {
		st := a
		if i%2 == 1 {
			st = b
		}
		if err := st.LoginFailed(keys...); err != nil {
			t.Fatal(err)
		}
	}
	if w, _ := b.LoginBlocked(keys...); w != 0 {
		t.Fatalf("blocked after %d failures", LoginMaxFails-1)
	}
	if err := b.LoginFailed("u:alice", "ip:10.0.0.10"); err != nil { // other IP, same user
		t.Fatal(err)
	}
	if w, _ := a.LoginBlocked("u:alice", "ip:10.0.0.11"); w <= 0 {
		t.Fatal("user should be locked out on the other replica")
	}
	if w, _ := a.LoginBlocked("u:bob", "ip:10.0.0.12"); w != 0 {
		t.Fatal("unrelated user/ip must not be blocked")
	}
	if err := a.LoginSucceeded("u:alice"); err != nil {
		t.Fatal(err)
	}
	if w, _ := b.LoginBlocked("u:alice"); w != 0 {
		t.Fatal("success should clear the user's failures")
	}
	m := map[string]any{}
	if err := a.readTable(loginTable, &m); err != nil {
		t.Fatal(err)
	}
	for k := range m {
		if k == "u:alice" || k == "ip:10.0.0.9" {
			t.Fatal("keys must be hashed")
		}
	}
}

func TestLoginLockoutSharedJSON(t *testing.T) {
	home := t.TempDir()
	a, err := OpenStore(home, "")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, _ := OpenStore(home, "")
	defer b.Close()
	testSharedLockout(t, a, b)
}

func TestLoginLockoutSharedPG(t *testing.T) {
	resetPG(t)
	a, b := pgStore(t), pgStore(t)
	testSharedLockout(t, a, b)

	// concurrent failures from many replicas are all counted (row lock)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := a
			if i%2 == 0 {
				st = b
			}
			_ = st.LoginFailed("u:carol")
		}(i)
	}
	wg.Wait()
	m := map[string][]any{}
	_ = a.readTable(loginTable, &m)
	if n := len(m[loginKey("u:carol")]); n != 10 {
		t.Fatalf("recorded %d failures, want 10", n)
	}
}
