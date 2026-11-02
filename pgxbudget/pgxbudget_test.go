package pgxbudget_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ez-technologies/dbbudget"
	"github.com/ez-technologies/dbbudget/pgxbudget"
)

// Two pools, one budget of 2 connections: pool A's idle connections close for pool B when B waits.
func TestTwoPoolsShareTheBudget(t *testing.T) {
	url := os.Getenv("DBBUDGET_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("DBBUDGET_TEST_POSTGRES_URL is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	ctx := context.Background()
	store := dbbudget.NewMemoryStore()
	opts := func(h string) dbbudget.Options {
		return dbbudget.Options{Max: 2, Holder: h, IdleGrace: time.Hour, TTL: 3 * time.Second, Poll: 50 * time.Millisecond}
	}
	ba, _ := dbbudget.New(store, opts("a"))
	bb, _ := dbbudget.New(store, opts("b"))
	defer ba.Close(ctx)
	defer bb.Close(ctx)
	pa, err := pgxbudget.NewPool(ctx, url, ba, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer pa.Close()
	ba.SetOnPressure(func() { pgxbudget.Pressure(pa) })
	pb, err := pgxbudget.NewPool(ctx, url, bb, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer pb.Close()

	c1, err := pa.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := pa.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ba.InUse() != 2 {
		t.Fatalf("a has %d connections open", ba.InUse())
	}
	// A third connection of a waits: the budget is 2.
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	if c, err := pa.Acquire(short); err == nil {
		c.Release()
		t.Fatal("a opened a third connection")
	}
	cancel()
	c1.Release() // back in a's pool, idle but open
	c2.Release()
	wait, cancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel2()
	var one int
	if err := pb.QueryRow(wait, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("b never got a connection: %v", err)
	}
	if store.Held() > 2 {
		t.Fatalf("%d slots held", store.Held())
	}
}
