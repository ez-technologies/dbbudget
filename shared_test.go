package dbbudget_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ez-technologies/dbbudget"
)

func newShared(t *testing.T, store dbbudget.Store, holder string, max int, grace time.Duration) *dbbudget.Shared {
	t.Helper()
	b, err := dbbudget.New(store, dbbudget.Options{Max: max, Holder: holder, TTL: 3 * time.Second, IdleGrace: grace,
		Poll: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return b
}

func acquireWithin(b dbbudget.Budget, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return b.Acquire(ctx)
}

func TestNewNeedsMaxAndHolder(t *testing.T) {
	if _, err := dbbudget.New(dbbudget.NewMemoryStore(), dbbudget.Options{Holder: "a"}); err == nil {
		t.Error("no Max accepted")
	}
	if _, err := dbbudget.New(dbbudget.NewMemoryStore(), dbbudget.Options{Max: 2}); err == nil {
		t.Error("no Holder accepted")
	}
}

func TestNothingIsReservedAndTheTotalStaysUnderMax(t *testing.T) {
	store := dbbudget.NewMemoryStore()
	a := newShared(t, store, "a", 3, time.Hour)
	b := newShared(t, store, "b", 3, time.Hour)
	if a.Share() != 0 || store.Held() != 0 {
		t.Fatal("something was reserved before any connection")
	}
	for i := 0; i < 3; i++ {
		if err := acquireWithin(a, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if a.Share() != 3 || store.Held() != 3 {
		t.Fatalf("share %d, held %d", a.Share(), store.Held())
	}
	if err := acquireWithin(b, 300*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("b got a fourth connection: %v", err)
	}
}

func TestAnIdleSlotGoesToWhoeverWaits(t *testing.T) {
	store := dbbudget.NewMemoryStore()
	a := newShared(t, store, "a", 2, time.Hour) // a would keep idle slots for an hour, but someone waits
	b := newShared(t, store, "b", 2, time.Hour)
	_ = acquireWithin(a, time.Second)
	_ = acquireWithin(a, time.Second)
	a.Release() // one connection of a closed: its slot is idle
	start := time.Now()
	if err := acquireWithin(b, 5*time.Second); err != nil {
		t.Fatalf("b never got the idle slot: %v", err)
	}
	if a.Share() != 1 || b.Share() != 1 {
		t.Fatalf("shares a %d b %d", a.Share(), b.Share())
	}
	t.Logf("handed over in %s", time.Since(start))
}

func TestIdleSlotsGoBackAfterTheGraceButOneStays(t *testing.T) {
	store := dbbudget.NewMemoryStore()
	a := newShared(t, store, "a", 5, 200*time.Millisecond)
	for i := 0; i < 3; i++ {
		_ = acquireWithin(a, time.Second)
	}
	for i := 0; i < 3; i++ {
		a.Release()
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && a.Share() != 1 {
		time.Sleep(20 * time.Millisecond)
	}
	if a.Share() != 1 || store.Held() != 1 {
		t.Fatalf("share %d held %d, want 1 kept", a.Share(), store.Held())
	}
	// The kept slot serves the next connection at once.
	if err := acquireWithin(a, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestPressureClosesIdleConnectionsForWhoeverWaits(t *testing.T) {
	store := dbbudget.NewMemoryStore()
	a := newShared(t, store, "a", 2, time.Hour)
	b := newShared(t, store, "b", 2, time.Hour)
	_ = acquireWithin(a, time.Second)
	_ = acquireWithin(a, time.Second)
	// Both of a's connections are open (idle in its pool): pressure makes it close one.
	pressed := make(chan struct{}, 10)
	a.SetOnPressure(func() {
		select {
		case pressed <- struct{}{}:
			a.Release() // the pool closed an idle connection
		default:
		}
	})
	if err := acquireWithin(b, 5*time.Second); err != nil {
		t.Fatalf("b waited in vain: %v", err)
	}
	if len(pressed) == 0 {
		t.Fatal("OnPressure was not called")
	}
}

func TestADeadInstancesSlotsComeBackAfterTheirTTL(t *testing.T) {
	store := dbbudget.NewMemoryStore()
	ctx := context.Background()
	// A process that died holding both slots (it took them and never renewed).
	_, _ = store.Take(ctx, "dead", 2, time.Second)
	_, _ = store.Take(ctx, "dead", 2, time.Second)
	b := newShared(t, store, "b", 2, time.Hour)
	if err := acquireWithin(b, 5*time.Second); err != nil {
		t.Fatalf("the dead instance's slot never came back: %v", err)
	}
}

func TestStoreDownKeepsAtMostOneConnection(t *testing.T) {
	store := dbbudget.NewMemoryStore()
	a := newShared(t, store, "a", 5, time.Hour)
	_ = acquireWithin(a, time.Second)
	_ = acquireWithin(a, time.Second)
	a.Release()
	a.Release()
	store.Fail(errors.New("store unreachable"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !a.Degraded() {
		time.Sleep(20 * time.Millisecond)
	}
	if !a.Degraded() || a.Share() != 1 {
		t.Fatalf("degraded %v share %d", a.Degraded(), a.Share())
	}
	if err := acquireWithin(a, time.Second); err != nil {
		t.Fatalf("one connection is allowed: %v", err)
	}
	if err := acquireWithin(a, 200*time.Millisecond); err == nil {
		t.Fatal("a second connection while the store is down")
	}
	store.Fail(nil)
	if err := acquireWithin(a, 3*time.Second); err != nil {
		t.Fatalf("after the store is back: %v", err)
	}
	if a.Degraded() {
		t.Fatal("still degraded")
	}
}

func TestCloseGivesEverythingBack(t *testing.T) {
	store := dbbudget.NewMemoryStore()
	a, _ := dbbudget.New(store, dbbudget.Options{Max: 3, Holder: "a"})
	_ = acquireWithin(a, time.Second)
	_ = acquireWithin(a, time.Second)
	if err := a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.Held() != 0 {
		t.Fatalf("held %d after close", store.Held())
	}
}

func TestFixed(t *testing.T) {
	f := dbbudget.Fixed(2)
	_ = acquireWithin(f, time.Second)
	_ = acquireWithin(f, time.Second)
	if err := acquireWithin(f, 100*time.Millisecond); err == nil {
		t.Fatal("a third permit")
	}
	f.Release()
	if err := acquireWithin(f, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if f.Share() != 2 || dbbudget.Fixed(0).Share() != 1 {
		t.Fatal("share")
	}
}

func TestDialHoldsAPermitPerOpenConnection(t *testing.T) {
	f := dbbudget.Fixed(1)
	dial := dbbudget.Dial(f, func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, _ := net.Pipe()
		return c, nil
	})
	c1, err := dial(context.Background(), "tcp", "db:5432")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := dial(ctx, "tcp", "db:5432"); err == nil {
		t.Fatal("a second connection over the budget")
	}
	_ = c1.Close()
	_ = c1.Close() // twice: one release
	c2, err := dial(context.Background(), "tcp", "db:5432")
	if err != nil {
		t.Fatal(err)
	}
	_ = c2.Close()
	// A failed dial gives the permit back.
	failing := dbbudget.Dial(f, func(context.Context, string, string) (net.Conn, error) { return nil, io.EOF })
	if _, err := failing(context.Background(), "tcp", "x"); err == nil {
		t.Fatal("expected the dial error")
	}
	if err := acquireWithin(f, 100*time.Millisecond); err != nil {
		t.Fatal("the failed dial kept its permit")
	}
}
