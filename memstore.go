package dbbudget

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

// MemoryStore is a Store in this process: for tests, and for several budgets inside one program.
type MemoryStore struct {
	mu    sync.Mutex
	slots map[int]memSlot
	wants map[string]time.Time
	fail  error
	Now   func() time.Time
}

type memSlot struct {
	holder  string
	expires time.Time
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{slots: map[int]memSlot{}, wants: map[string]time.Time{}, Now: time.Now}
}

var _ Store = (*MemoryStore)(nil)

// Fail makes every call answer err (nil: work again); for testing what an instance does when the store is down.
func (m *MemoryStore) Fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = err
}

func (m *MemoryStore) Take(_ context.Context, holder string, max int, ttl time.Duration) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return -1, m.fail
	}
	now := m.Now()
	for _, i := range rand.Perm(max) {
		if cur, ok := m.slots[i]; !ok || !cur.expires.After(now) {
			m.slots[i] = memSlot{holder: holder, expires: now.Add(ttl)}
			delete(m.wants, holder)
			return i, nil
		}
	}
	return -1, nil
}

func (m *MemoryStore) Renew(_ context.Context, holder string, slot int, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	cur, ok := m.slots[slot]
	if !ok || cur.holder != holder {
		return ErrLost
	}
	m.slots[slot] = memSlot{holder: holder, expires: m.Now().Add(ttl)}
	return nil
}

func (m *MemoryStore) Give(_ context.Context, holder string, slot int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	if cur, ok := m.slots[slot]; ok && cur.holder == holder {
		delete(m.slots, slot)
	}
	return nil
}

func (m *MemoryStore) Want(_ context.Context, holder string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	m.wants[holder] = m.Now().Add(ttl)
	return nil
}

func (m *MemoryStore) Wanted(_ context.Context, holder string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return 0, m.fail
	}
	now, n := m.Now(), 0
	for h, exp := range m.wants {
		if !exp.After(now) {
			delete(m.wants, h)
			continue
		}
		if h != holder {
			n++
		}
	}
	return n, nil
}

// Held counts the slots held now (not expired).
func (m *MemoryStore) Held() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.slots {
		if s.expires.After(m.Now()) {
			n++
		}
	}
	return n
}
