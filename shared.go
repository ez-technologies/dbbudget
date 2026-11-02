package dbbudget

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Options of a shared budget.
type Options struct {
	// Max is the total number of connections across all instances (required): the database's max_connections minus
	// what admin tools and migrations need.
	Max int
	// Holder names this instance, unique among the instances (required; e.g. host name and process id).
	Holder string
	// TTL of a slot, renewed by a heartbeat every TTL/3 (default 15s). A dead instance's slots return after it.
	TTL time.Duration
	// IdleGrace is how long a slot whose connection closed is kept for the next one (default 30s).
	IdleGrace time.Duration
	// MinHold slots stay with the instance when idle (default 1), unless another instance waits for one.
	MinHold int
	// Poll is how often a waiting Acquire tries again and wants are checked (default 250ms).
	Poll time.Duration
	// OnPressure is called when other instances wait and every slot of this one has a connection open, and when
	// this process itself waits for a connection: close idle connections so their slots can go (SQLPressure,
	// pgxbudget.Pressure). Nil: slots go back only when connections close by themselves, and a pool allowed more
	// connections than the budget gives can wait on its own idle ones.
	OnPressure func()
	// Logger (default slog.Default()).
	Logger *slog.Logger
}

// Shared is a budget shared through a Store.
type Shared struct {
	store Store
	o     Options

	mu       sync.Mutex
	slots    []int       // held slot ids
	inUse    int         // permits out
	freeAt   []time.Time // when each free slot was freed (len = len(slots) - inUse when not degraded)
	degraded bool        // the store failed: at most one connection
	changed  chan struct{}

	stop context.CancelFunc
	done chan struct{}
}

var _ Budget = (*Shared)(nil)

// New starts a shared budget (its heartbeat runs until Close).
func New(store Store, o Options) (*Shared, error) {
	if o.Max < 1 {
		return nil, errors.New("dbbudget: Max must be at least 1")
	}
	if o.Holder == "" {
		return nil, errors.New("dbbudget: Holder is required")
	}
	if o.TTL <= 0 {
		o.TTL = 15 * time.Second
	}
	if o.IdleGrace <= 0 {
		o.IdleGrace = 30 * time.Second
	}
	if o.MinHold <= 0 {
		o.MinHold = 1
	}
	if o.Poll <= 0 {
		o.Poll = 250 * time.Millisecond
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Shared{store: store, o: o, changed: make(chan struct{}), stop: cancel, done: make(chan struct{})}
	go s.loop(ctx)
	return s, nil
}

func (s *Shared) signal() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// Share is how many connections this instance may have open now.
func (s *Shared) Share() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded {
		return 1
	}
	return len(s.slots)
}

// Degraded reports whether the store is unreachable (the instance keeps at most one connection).
func (s *Shared) Degraded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.degraded
}

// InUse is the number of permits out (connections open).
func (s *Shared) InUse() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inUse
}

func (s *Shared) setDegraded(err error) {
	s.mu.Lock()
	was := s.degraded
	s.degraded = err != nil
	if was != s.degraded {
		s.signal()
	}
	s.mu.Unlock()
	if err != nil && !was {
		s.o.Logger.Warn("dbbudget: store unreachable, keeping at most one connection", "error", err.Error())
	} else if err == nil && was {
		s.o.Logger.Info("dbbudget: store reachable again")
	}
}

// Acquire waits for a permit.
func (s *Shared) Acquire(ctx context.Context) error {
	wanted, pressed := time.Time{}, time.Time{}
	for {
		s.mu.Lock()
		if s.degraded {
			if s.inUse < 1 {
				s.inUse++
				s.mu.Unlock()
				return nil
			}
		} else if s.inUse < len(s.slots) {
			s.inUse++
			if n := len(s.freeAt); n > 0 {
				s.freeAt = s.freeAt[:n-1]
			}
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		degraded := s.degraded
		s.mu.Unlock()

		if !degraded {
			tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			slot, err := s.store.Take(tctx, s.o.Holder, s.o.Max, s.o.TTL)
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				s.setDegraded(err)
				continue
			}
			if slot >= 0 {
				s.mu.Lock()
				s.slots = append(s.slots, slot)
				s.inUse++
				s.mu.Unlock()
				return nil
			}
			// Full: say so (at most once per TTL/3), then wait for a slot to come back.
			if time.Since(wanted) > s.o.TTL/3 {
				if err := s.store.Want(ctx, s.o.Holder, s.o.TTL); err != nil && ctx.Err() == nil {
					s.setDegraded(err)
					continue
				}
				wanted = time.Now()
			}
		}
		if s.InUse() > 0 {
			// This process waits while it may hold idle connections itself: ask for them to be closed.
			if f := s.onPressure(); f != nil && time.Since(pressed) > s.o.Poll {
				pressed = time.Now()
				go f()
			}
		}
		t := time.NewTimer(s.o.Poll)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-changed:
		case <-t.C:
		}
		t.Stop()
	}
}

// Release returns a permit (its connection closed).
func (s *Shared) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inUse == 0 {
		return
	}
	s.inUse--
	if s.inUse < len(s.slots) && len(s.freeAt) < len(s.slots)-s.inUse {
		s.freeAt = append(s.freeAt, time.Now())
	}
	s.signal()
}

// Close stops the heartbeat and gives every slot back.
func (s *Shared) Close(ctx context.Context) error {
	s.stop()
	<-s.done
	s.mu.Lock()
	slots := s.slots
	s.slots, s.freeAt = nil, nil
	s.mu.Unlock()
	var errs []error
	for _, id := range slots {
		errs = append(errs, s.store.Give(ctx, s.o.Holder, id))
	}
	return errors.Join(errs...)
}

// loop is the heartbeat: renew slots, give idle ones back, react to wants.
func (s *Shared) loop(ctx context.Context) {
	defer close(s.done)
	wake := make(<-chan struct{})
	if w, ok := s.store.(Watcher); ok {
		if ch, err := w.WatchWants(ctx); err == nil {
			wake = ch
		}
	}
	tick := time.NewTicker(s.o.Poll)
	defer tick.Stop()
	lastRenew := time.Now()
	lastWants := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-wake:
			lastWants = time.Time{} // check wants now
		}
		if time.Since(lastRenew) >= s.o.TTL/3 {
			s.renew(ctx)
			lastRenew = time.Now()
		}
		// Wants are read at most every second unless the store pushed one.
		wanted := 0
		if time.Since(lastWants) >= time.Second {
			n, err := s.store.Wanted(ctx, s.o.Holder)
			if err == nil {
				wanted = n
			}
			lastWants = time.Now()
		}
		s.trim(ctx, wanted)
	}
}

func (s *Shared) renew(ctx context.Context) {
	s.mu.Lock()
	slots := append([]int(nil), s.slots...)
	s.mu.Unlock()
	var failed error
	for _, id := range slots {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.store.Renew(rctx, s.o.Holder, id, s.o.TTL)
		cancel()
		switch {
		case errors.Is(err, ErrLost):
			s.o.Logger.Warn("dbbudget: a slot expired and was taken; giving its permit up", "slot", id)
			s.drop(id)
		case err != nil:
			failed = err
		}
	}
	if ctx.Err() == nil {
		s.setDegraded(failed)
	}
}

func (s *Shared) drop(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, x := range s.slots {
		if x == id {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			break
		}
	}
	if free := len(s.slots) - s.inUse; free < len(s.freeAt) {
		if free < 0 {
			free = 0
		}
		s.freeAt = s.freeAt[len(s.freeAt)-free:]
	}
}

// trim gives back free slots idle for IdleGrace, or at once when others wait; and asks for idle connections to be
// closed when others wait and none is free.
func (s *Shared) trim(ctx context.Context, wanted int) {
	for {
		s.mu.Lock()
		// MinHold keeps slots from the idle grace only: a waiting instance gets even the last idle one.
		keep := s.o.MinHold
		if wanted > 0 {
			keep = 0
		}
		if s.degraded || len(s.slots) <= keep || len(s.freeAt) == 0 {
			pressure := wanted > 0 && !s.degraded && len(s.freeAt) == 0 && s.inUse > 0
			s.mu.Unlock()
			if pressure {
				if f := s.onPressure(); f != nil {
					f()
				}
			}
			return
		}
		oldest := s.freeAt[0]
		if wanted == 0 && time.Since(oldest) < s.o.IdleGrace {
			s.mu.Unlock()
			return
		}
		id := s.slots[len(s.slots)-1]
		s.slots = s.slots[:len(s.slots)-1]
		s.freeAt = s.freeAt[1:]
		s.mu.Unlock()
		gctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.store.Give(gctx, s.o.Holder, id)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.o.Logger.Warn("dbbudget: give back", "slot", id, "error", err.Error())
		}
		if wanted > 0 {
			wanted--
		}
	}
}

// DialFunc dials a network connection (the shape of net.Dialer.DialContext and pgconn.DialFunc).
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Dial puts the budget in front of a dialer: each connection waits for a permit and returns it when closed. Without
// next, a net.Dialer is used.
func Dial(b Budget, next DialFunc) DialFunc {
	if next == nil {
		next = (&net.Dialer{KeepAlive: 5 * time.Minute}).DialContext
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if err := b.Acquire(ctx); err != nil {
			return nil, err
		}
		c, err := next(ctx, network, addr)
		if err != nil {
			b.Release()
			return nil, err
		}
		return &conn{Conn: c, b: b}, nil
	}
}

type conn struct {
	net.Conn
	b    Budget
	once sync.Once
}

func (c *conn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.b.Release)
	return err
}

// SQLPressure is an OnPressure for database/sql: it closes the pool's idle connections (and keeps its idle limit).
//
//	b, _ := dbbudget.New(store, dbbudget.Options{..., OnPressure: func() { dbbudget.SQLPressure(db, 2) }})
func SQLPressure(db interface{ SetMaxIdleConns(int) }, maxIdle int) {
	db.SetMaxIdleConns(0)
	db.SetMaxIdleConns(maxIdle)
}

// SetOnPressure sets Options.OnPressure after New (the pool is usually made after the budget).
func (s *Shared) SetOnPressure(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.o.OnPressure = f
}

func (s *Shared) onPressure() func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.o.OnPressure
}
