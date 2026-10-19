package engine

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// With the JSON store a second scheduler waits as a standby and takes over
// when the active one stops.
func TestJSONSchedulerStandbyTakesOver(t *testing.T) {
	old := StandbyPoll
	StandbyPoll = 50 * time.Millisecond
	defer func() { StandbyPoll = old }()
	home := t.TempDir()
	open := func() *Store {
		st, err := OpenStore(home, "")
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	st1, st2 := open(), open()
	defer st1.Close()
	defer st2.Close()
	s1, s2 := NewScheduler(NewEngine(st1, 2)), NewScheduler(NewEngine(st2, 2))
	var log1, log2 syncBuf
	s1.Log, s2.Log = log.New(&log1, "", 0), log.New(&log2, "", 0)

	ctx1, stop1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- s1.Run(ctx1) }()
	waitFor(t, func() bool { return strings.Contains(log1.String(), "started") })

	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()
	done2 := make(chan error, 1)
	go func() { done2 <- s2.Run(ctx2) }()
	waitFor(t, func() bool { return strings.Contains(log2.String(), "standby: another scheduler is active") })
	time.Sleep(200 * time.Millisecond)
	if strings.Contains(log2.String(), "started") {
		t.Fatal("standby started while the first scheduler holds the lock")
	}

	stop1()
	if err := <-done1; err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return strings.Contains(log2.String(), "taking over") && strings.Contains(log2.String(), "started")
	})
	stop2()
	if err := <-done2; err != nil {
		t.Fatal(err)
	}
}

// A standby that is stopped before it gets the lock returns cleanly.
func TestJSONSchedulerStandbyStops(t *testing.T) {
	home := t.TempDir()
	st1, _ := OpenStore(home, "")
	st2, _ := OpenStore(home, "")
	defer st1.Close()
	defer st2.Close()
	s1, s2 := NewScheduler(NewEngine(st1, 1)), NewScheduler(NewEngine(st2, 1))
	var b1, b2 syncBuf
	s1.Log, s2.Log = log.New(&b1, "", 0), log.New(&b2, "", 0)
	ctx1, stop1 := context.WithCancel(context.Background())
	defer stop1()
	go func() { _ = s1.Run(ctx1) }()
	waitFor(t, func() bool { return strings.Contains(b1.String(), "started") })
	ctx2, stop2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop2()
	if err := s2.Run(ctx2); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b2.String(), "started") {
		t.Fatal("standby should not have started")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
