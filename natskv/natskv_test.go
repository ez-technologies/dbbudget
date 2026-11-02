package natskv_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/dbbudget"
	"github.com/ez-technologies/dbbudget/budgettest"
	"github.com/ez-technologies/dbbudget/natskv"
)

func newStore(t *testing.T) *natskv.Store {
	t.Helper()
	url := os.Getenv("DBBUDGET_TEST_NATS_URL")
	if url == "" {
		t.Skip("DBBUDGET_TEST_NATS_URL is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := jetstream.New(nc)
	bucket := fmt.Sprintf("budgettest_%d", time.Now().UnixNano())
	s, err := natskv.New(context.Background(), js, bucket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = js.DeleteKeyValue(context.Background(), bucket) })
	return s
}

func TestStore(t *testing.T) {
	budgettest.Store(t, func(t *testing.T) dbbudget.Store { return newStore(t) })
}

func TestAWaitingInstanceIsHeardAtOnce(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.WatchWants(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Want(ctx, "b", time.Minute); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no signal for a new want")
	}
}

func TestTwoBudgetsShareOneBucket(t *testing.T) {
	s := newStore(t)
	a, _ := dbbudget.New(s, dbbudget.Options{Max: 1, Holder: "a", IdleGrace: time.Hour, TTL: 3 * time.Second})
	b, _ := dbbudget.New(s, dbbudget.Options{Max: 1, Holder: "b", IdleGrace: time.Hour, TTL: 3 * time.Second, MinHold: 1})
	defer a.Close(context.Background())
	defer b.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	a.SetOnPressure(func() { a.Release() }) // a's one connection is idle: it closes it when b waits
	if err := b.Acquire(ctx); err != nil {
		t.Fatalf("b: %v", err)
	}
}
