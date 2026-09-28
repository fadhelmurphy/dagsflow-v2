package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// slotManager hands out executor slots (global parallelism) and pool slots.
// Waiters are served by priority weight (desc), then FIFO. A waiter blocked by
// a full pool does not block waiters of other pools.
type slotManager struct {
	mu          sync.Mutex
	parallelism int
	used        int
	poolUsed    map[string]int
	waiters     []*waiter
	seq         int64
	poolSize    func(string) int // -1 = pool does not exist
}

type waiter struct {
	pool    string
	slots   int
	prio    int
	seq     int64
	ch      chan struct{}
	granted bool
}

func newSlotManager(parallelism int, poolSize func(string) int) *slotManager {
	return &slotManager{parallelism: parallelism, poolUsed: map[string]int{}, poolSize: poolSize}
}

func (m *slotManager) acquire(ctx context.Context, pool string, slots, prio int) error {
	if m.poolSize(pool) < 0 {
		return fmt.Errorf("pool %q does not exist (create it with `dagsflow-go pools set`)", pool)
	}
	m.mu.Lock()
	m.seq++
	w := &waiter{pool: pool, slots: slots, prio: prio, seq: m.seq, ch: make(chan struct{})}
	m.waiters = append(m.waiters, w)
	m.dispatchLocked()
	m.mu.Unlock()

	// re-dispatch periodically so pool size changes are picked up
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-w.ch:
			return nil
		case <-tick.C:
			m.mu.Lock()
			m.dispatchLocked()
			m.mu.Unlock()
		case <-ctx.Done():
			m.mu.Lock()
			if w.granted {
				m.releaseLocked(pool, slots)
			} else {
				for i, x := range m.waiters {
					if x == w {
						m.waiters = append(m.waiters[:i], m.waiters[i+1:]...)
						break
					}
				}
			}
			m.mu.Unlock()
			return ctx.Err()
		}
	}
}

func (m *slotManager) release(pool string, slots int) {
	m.mu.Lock()
	m.releaseLocked(pool, slots)
	m.mu.Unlock()
}

func (m *slotManager) releaseLocked(pool string, slots int) {
	m.used--
	m.poolUsed[pool] -= slots
	m.dispatchLocked()
}

func (m *slotManager) dispatchLocked() {
	sort.SliceStable(m.waiters, func(i, j int) bool {
		if m.waiters[i].prio != m.waiters[j].prio {
			return m.waiters[i].prio > m.waiters[j].prio
		}
		return m.waiters[i].seq < m.waiters[j].seq
	})
	rest := m.waiters[:0]
	for _, w := range m.waiters {
		if m.used < m.parallelism && m.poolUsed[w.pool]+w.slots <= m.poolSize(w.pool) {
			m.used++
			m.poolUsed[w.pool] += w.slots
			w.granted = true
			close(w.ch)
			continue
		}
		rest = append(rest, w)
	}
	m.waiters = rest
}
