// Command demo shows three app instances sharing a budget of 4 database connections: nothing reserved up front, a
// waiting instance getting an idle connection's slot, and a dead instance's slots coming back after their TTL.
//
//	go run ./examples/demo                              # the store in memory
//	go run ./examples/demo -nats nats://127.0.0.1:4222  # the store in a NATS KV bucket
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/dbbudget"
	"github.com/ez-technologies/dbbudget/natskv"
)

var start = time.Now()

func say(format string, args ...any) {
	fmt.Printf("%5.1fs  %s\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
}

func main() {
	natsURL := flag.String("nats", "", "NATS server URL (empty: the store in memory)")
	flag.Parse()
	ctx := context.Background()
	var store dbbudget.Store = dbbudget.NewMemoryStore()
	where := "in memory"
	if *natsURL != "" {
		nc, err := nats.Connect(*natsURL)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer nc.Close()
		js, _ := jetstream.New(nc)
		_ = js.DeleteKeyValue(ctx, "dbbudget_demo")
		if store, err = natskv.New(ctx, js, "dbbudget_demo"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		where = "in NATS KV"
	}
	quiet := slog.New(slog.DiscardHandler)
	mk := func(name string) *dbbudget.Shared {
		b, err := dbbudget.New(store, dbbudget.Options{Max: 4, Holder: name, TTL: 3 * time.Second,
			IdleGrace: time.Minute, Poll: 100 * time.Millisecond, Logger: quiet})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return b
	}
	a, b, c := mk("app-a"), mk("app-b"), mk("app-c")
	shares := func() string {
		return fmt.Sprintf("[a %d  b %d  c %d of 4]", a.Share(), b.Share(), c.Share())
	}
	open := func(name string, x *dbbudget.Shared, n int) {
		for range n {
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			t := time.Now()
			if err := x.Acquire(wctx); err != nil {
				say("%s could not open a connection: %v", name, err)
			} else if w := time.Since(t); w > 200*time.Millisecond {
				say("%s opened a connection after waiting %.1fs  %s", name, w.Seconds(), shares())
			} else {
				say("%s opened a connection  %s", name, shares())
			}
			cancel()
		}
	}
	say("budget of 4 connections shared by app-a, app-b, app-c; store %s; nothing reserved  %s", where, shares())
	open("app-a", a, 3)
	open("app-b", b, 1)
	say("the budget is full; app-c asks")
	go func() {
		time.Sleep(1500 * time.Millisecond)
		say("app-a: one connection closes (idle in its pool); its slot is kept for a minute, unless someone waits")
		a.Release()
	}()
	open("app-c", c, 1)
	say("app-b dies without giving its slot back (no more heartbeats)")
	_ = b.Close(ctx) // stops its heartbeat; in a real crash nothing is given back, the TTL does it
	_, _ = store.Take(ctx, "app-b-crashed", 4, 3*time.Second)
	open("app-c", c, 1)
	say("final  [a %d  c %d of 4]", a.Share(), c.Share())
	_ = a.Close(ctx)
	_ = c.Close(ctx)
}
