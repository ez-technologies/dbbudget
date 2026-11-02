// Package natskv is a dbbudget Store on a NATS JetStream key-value bucket: keys "slot.<n>" and "want.<holder>", each
// changed by compare-and-set on its revision. It watches "want.>", so an idle instance hears of a waiting one at once.
// Expiry times are written by each instance's clock: keep clocks in sync (NTP) and the TTL well above the skew.
package natskv

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"regexp"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/dbbudget"
)

// Store is the KV store.
type Store struct {
	kv  jetstream.KeyValue
	now func() time.Time
}

var (
	_ dbbudget.Store   = (*Store)(nil)
	_ dbbudget.Watcher = (*Store)(nil)
)

// New makes (or opens) the bucket (default "dbbudget"). Use one bucket per budget.
func New(ctx context.Context, js jetstream.JetStream, bucket string) (*Store, error) {
	if bucket == "" {
		bucket = "dbbudget"
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, History: 1})
	if err != nil {
		return nil, err
	}
	return &Store{kv: kv, now: time.Now}, nil
}

type value struct {
	Holder  string `json:"holder,omitempty"`
	Expires int64  `json:"expires_ms"`
}

var unsafe = regexp.MustCompile(`[^A-Za-z0-9_-]`)

func slotKey(i int) string    { return "slot." + strconv.Itoa(i) }
func wantKey(h string) string { return "want." + unsafe.ReplaceAllString(h, "_") }
func (s *Store) ms() int64    { return s.now().UnixMilli() }
func enc(v value) []byte      { b, _ := json.Marshal(v); return b }
func live(e jetstream.KeyValueEntry) bool {
	return e != nil && e.Operation() == jetstream.KeyValuePut
}

func conflict(err error) bool {
	var apiErr *jetstream.APIError
	return errors.Is(err, jetstream.ErrKeyExists) ||
		(errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence)
}

func (s *Store) get(ctx context.Context, key string) (jetstream.KeyValueEntry, value, error) {
	e, err := s.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, value{}, nil
	}
	if err != nil {
		return nil, value{}, err
	}
	var v value
	if live(e) {
		_ = json.Unmarshal(e.Value(), &v)
	}
	return e, v, nil
}

// Take claims a free or expired slot.
func (s *Store) Take(ctx context.Context, holder string, max int, ttl time.Duration) (int, error) {
	val := enc(value{Holder: holder, Expires: s.now().Add(ttl).UnixMilli()})
	for _, i := range rand.Perm(max) {
		e, v, err := s.get(ctx, slotKey(i))
		if err != nil {
			return -1, err
		}
		switch {
		case e == nil:
			_, err = s.kv.Create(ctx, slotKey(i), val)
		case !live(e) || v.Expires < s.ms():
			_, err = s.kv.Update(ctx, slotKey(i), val, e.Revision())
		default:
			continue
		}
		if conflict(err) {
			continue
		}
		if err != nil {
			return -1, err
		}
		_ = s.kv.Delete(ctx, wantKey(holder))
		return i, nil
	}
	return -1, nil
}

// Renew extends holder's slot.
func (s *Store) Renew(ctx context.Context, holder string, slot int, ttl time.Duration) error {
	e, v, err := s.get(ctx, slotKey(slot))
	if err != nil {
		return err
	}
	if e == nil || !live(e) || v.Holder != holder {
		return dbbudget.ErrLost
	}
	_, err = s.kv.Update(ctx, slotKey(slot), enc(value{Holder: holder, Expires: s.now().Add(ttl).UnixMilli()}), e.Revision())
	if conflict(err) {
		return dbbudget.ErrLost
	}
	return err
}

// Give frees holder's slot.
func (s *Store) Give(ctx context.Context, holder string, slot int) error {
	e, v, err := s.get(ctx, slotKey(slot))
	if err != nil || e == nil || !live(e) || v.Holder != holder {
		return err
	}
	err = s.kv.Delete(ctx, slotKey(slot), jetstream.LastRevision(e.Revision()))
	if conflict(err) {
		return nil
	}
	return err
}

// Want records a waiting holder.
func (s *Store) Want(ctx context.Context, holder string, ttl time.Duration) error {
	_, err := s.kv.Put(ctx, wantKey(holder), enc(value{Holder: holder, Expires: s.now().Add(ttl).UnixMilli()}))
	return err
}

// Wanted counts the other holders waiting now.
func (s *Store) Wanted(ctx context.Context, holder string) (int, error) {
	l, err := s.kv.ListKeysFiltered(ctx, "want.>")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Stop() }()
	n, now, me := 0, s.ms(), wantKey(holder)
	for key := range l.Keys() {
		if key == me {
			continue
		}
		_, v, err := s.get(ctx, key)
		if err != nil {
			return 0, err
		}
		if v.Expires >= now {
			n++
		} else if v.Expires > 0 {
			_ = s.kv.Purge(ctx, key)
		}
	}
	return n, nil
}

// WatchWants signals each new want.
func (s *Store) WatchWants(ctx context.Context) (<-chan struct{}, error) {
	w, err := s.kv.Watch(ctx, "want.>", jetstream.UpdatesOnly(), jetstream.IgnoreDeletes())
	if err != nil {
		return nil, err
	}
	out := make(chan struct{}, 1)
	go func() {
		defer func() { _ = w.Stop() }()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-w.Updates():
				if !ok {
					return
				}
				if e != nil {
					select {
					case out <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return out, nil
}
