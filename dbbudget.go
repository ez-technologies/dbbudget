// Package dbbudget keeps the total number of database connections of many app instances under one limit, without
// reserving anything in advance.
//
// The budget is a table of Max slots in a shared Store (natskv, redisstore, dynamostore, firestorestore, or
// NewMemoryStore in one process). One slot is one open connection:
//
//   - An instance that needs a connection takes a free slot. It keeps the slots of the connections it has open, plus a
//     slot whose connection closed for IdleGrace, so a burst does not pay for a new slot each time.
//   - When the budget is full, the asker records that it wants a slot. An instance with an idle connection sees that
//     (on its next check, or at once with a store that can watch), closes the idle connection and gives the slot back;
//     the asker takes it.
//   - Slots carry a TTL that a heartbeat renews, so the slots of an instance that died come back by themselves.
//   - When the store cannot be reached, an instance keeps at most one connection open until it can again.
//
// The budget counts physical connections: put Dial in front of your driver's dialer (pgx: ConnConfig.DialFunc; see
// the pgxbudget package for pgxpool). With a real pooler in front of the database (PgBouncer, RDS Proxy, Cloud SQL
// managed pooling) use Fixed instead: the same interface, a fixed number per instance.
package dbbudget

import (
	"context"
	"errors"
	"time"
)

// Budget hands out permits for connections.
type Budget interface {
	// Acquire waits for a permit for one connection (or ctx). Call Release when that connection closes.
	Acquire(ctx context.Context) error
	// Release returns a permit.
	Release()
	// Share is how many connections this instance may have open now.
	Share() int
	// Close gives every slot back (at shutdown).
	Close(ctx context.Context) error
}

// Store is the shared slot table. Implementations must make each call atomic across instances.
type Store interface {
	// Take claims a free or expired slot in 0..max-1 for holder until ttl from now. It answers the slot, or -1 when
	// every slot is held. Taking a slot should also clear holder's want.
	Take(ctx context.Context, holder string, max int, ttl time.Duration) (int, error)
	// Renew extends holder's slot to ttl from now. ErrLost: the slot is no longer holder's (it expired and was taken).
	Renew(ctx context.Context, holder string, slot int, ttl time.Duration) error
	// Give frees holder's slot (no-op when it is not holder's).
	Give(ctx context.Context, holder string, slot int) error
	// Want records that holder waits for a slot, until ttl from now.
	Want(ctx context.Context, holder string, ttl time.Duration) error
	// Wanted counts the holders other than holder waiting now.
	Wanted(ctx context.Context, holder string) (int, error)
}

// Watcher is a Store that can tell at once when someone starts waiting (NATS KV). Others are polled.
type Watcher interface {
	// WatchWants sends on the channel when a holder records a want, until ctx ends.
	WatchWants(ctx context.Context) (<-chan struct{}, error)
}

// ErrLost is returned by Renew for a slot that is no longer the holder's.
var ErrLost = errors.New("dbbudget: slot lost")

// fixed is a semaphore of n.
type fixed struct{ sem chan struct{} }

// Fixed is a budget of n connections for this instance, for a database behind a real pooler.
func Fixed(n int) Budget {
	if n < 1 {
		n = 1
	}
	return &fixed{sem: make(chan struct{}, n)}
}

func (f *fixed) Acquire(ctx context.Context) error {
	select {
	case f.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fixed) Release() {
	select {
	case <-f.sem:
	default:
	}
}

func (f *fixed) Share() int                  { return cap(f.sem) }
func (f *fixed) Close(context.Context) error { return nil }
