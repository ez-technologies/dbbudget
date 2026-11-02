// Package budgettest is the conformance suite every dbbudget Store passes. newStore must return a store over a fresh
// namespace (a new bucket, key prefix, table partition or collection) each time.
package budgettest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ez-technologies/dbbudget"
)

// Store checks taking until full, renewing, giving, expiry and wants.
func Store(t *testing.T, newStore func(t *testing.T) dbbudget.Store) {
	ctx := context.Background()

	t.Run("TakeUntilFullThenGiveBack", func(t *testing.T) {
		s := newStore(t)
		seen := map[int]bool{}
		for i := 0; i < 3; i++ {
			slot, err := s.Take(ctx, "a", 3, time.Minute)
			if err != nil || slot < 0 || slot > 2 || seen[slot] {
				t.Fatalf("take %d: slot %d, %v", i, slot, err)
			}
			seen[slot] = true
		}
		if slot, err := s.Take(ctx, "b", 3, time.Minute); err != nil || slot != -1 {
			t.Fatalf("a full budget gave slot %d, %v", slot, err)
		}
		if err := s.Give(ctx, "b", 1); err != nil {
			t.Fatalf("give by another holder is a no-op: %v", err)
		}
		if slot, _ := s.Take(ctx, "b", 3, time.Minute); slot != -1 {
			t.Fatal("another holder's give freed a slot")
		}
		if err := s.Give(ctx, "a", 1); err != nil {
			t.Fatal(err)
		}
		if slot, err := s.Take(ctx, "b", 3, time.Minute); err != nil || slot != 1 {
			t.Fatalf("b takes the given slot: %d, %v", slot, err)
		}
	})

	t.Run("RenewAndLost", func(t *testing.T) {
		s := newStore(t)
		slot, err := s.Take(ctx, "a", 1, time.Minute)
		if err != nil || slot != 0 {
			t.Fatalf("take: %d %v", slot, err)
		}
		if err := s.Renew(ctx, "a", 0, time.Minute); err != nil {
			t.Fatalf("renew: %v", err)
		}
		if err := s.Renew(ctx, "b", 0, time.Minute); !errors.Is(err, dbbudget.ErrLost) {
			t.Fatalf("renew by another holder: %v, want ErrLost", err)
		}
	})

	t.Run("ExpiredSlotsComeBack", func(t *testing.T) {
		s := newStore(t)
		for i := 0; i < 2; i++ {
			if slot, err := s.Take(ctx, "dead", 2, time.Second); err != nil || slot < 0 {
				t.Fatalf("take: %d %v", slot, err)
			}
		}
		if slot, _ := s.Take(ctx, "b", 2, time.Minute); slot != -1 {
			t.Fatal("full before expiry")
		}
		time.Sleep(2500 * time.Millisecond)
		slot, err := s.Take(ctx, "b", 2, time.Minute)
		if err != nil || slot < 0 {
			t.Fatalf("an expired slot was not taken: %d %v", slot, err)
		}
		if err := s.Renew(ctx, "dead", slot, time.Minute); !errors.Is(err, dbbudget.ErrLost) {
			t.Fatalf("the old holder renews a taken slot: %v, want ErrLost", err)
		}
	})

	t.Run("Wants", func(t *testing.T) {
		s := newStore(t)
		if n, err := s.Wanted(ctx, "a"); err != nil || n != 0 {
			t.Fatalf("no wants: %d %v", n, err)
		}
		if err := s.Want(ctx, "a", 2*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := s.Want(ctx, "b", 2*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := s.Want(ctx, "b", 2*time.Second); err != nil {
			t.Fatal(err)
		}
		if n, err := s.Wanted(ctx, "a"); err != nil || n != 1 {
			t.Fatalf("a sees %d others waiting (%v), want 1", n, err)
		}
		if n, _ := s.Wanted(ctx, "c"); n != 2 {
			t.Fatalf("c sees %d, want 2", n)
		}
		time.Sleep(3 * time.Second)
		if n, _ := s.Wanted(ctx, "c"); n != 0 {
			t.Fatalf("expired wants still count: %d", n)
		}
	})
}
