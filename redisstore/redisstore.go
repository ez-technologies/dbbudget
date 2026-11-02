// Package redisstore is a dbbudget Store on Redis or Valkey (ElastiCache, Memorystore, or your own): one key per
// slot with a TTL ("<prefix>:slot:<n>", set with NX), and a sorted set of waiting holders ("<prefix>:wants", scored by
// expiry). Renew and Give are compare-and-set scripts.
package redisstore

import (
	"context"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ez-technologies/dbbudget"
)

// Store is the Redis store.
type Store struct {
	c      redis.UniversalClient
	prefix string
	now    func() time.Time
}

var _ dbbudget.Store = (*Store)(nil)

// New returns a store with keys under prefix (default "dbbudget"). Use one prefix per budget.
func New(c redis.UniversalClient, prefix string) *Store {
	if prefix == "" {
		prefix = "dbbudget"
	}
	return &Store{c: c, prefix: prefix, now: time.Now}
}

func (s *Store) slot(i int) string { return s.prefix + ":slot:" + strconv.Itoa(i) }
func (s *Store) wants() string     { return s.prefix + ":wants" }

var renew = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`)

var give = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1]) else return 0 end`)

func (s *Store) Take(ctx context.Context, holder string, max int, ttl time.Duration) (int, error) {
	for _, i := range rand.Perm(max) {
		ok, err := s.c.SetNX(ctx, s.slot(i), holder, ttl).Result()
		if err != nil {
			return -1, err
		}
		if ok {
			s.c.ZRem(ctx, s.wants(), holder)
			return i, nil
		}
	}
	return -1, nil
}

func (s *Store) Renew(ctx context.Context, holder string, slot int, ttl time.Duration) error {
	n, err := renew.Run(ctx, s.c, []string{s.slot(slot)}, holder, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return dbbudget.ErrLost
	}
	return nil
}

func (s *Store) Give(ctx context.Context, holder string, slot int) error {
	return give.Run(ctx, s.c, []string{s.slot(slot)}, holder).Err()
}

func (s *Store) Want(ctx context.Context, holder string, ttl time.Duration) error {
	return s.c.ZAdd(ctx, s.wants(), redis.Z{Score: float64(s.now().Add(ttl).UnixMilli()), Member: holder}).Err()
}

func (s *Store) Wanted(ctx context.Context, holder string) (int, error) {
	now := strconv.FormatInt(s.now().UnixMilli(), 10)
	if err := s.c.ZRemRangeByScore(ctx, s.wants(), "-inf", "("+now).Err(); err != nil {
		return 0, err
	}
	n, err := s.c.ZCard(ctx, s.wants()).Result()
	if err != nil {
		return 0, err
	}
	if _, err := s.c.ZScore(ctx, s.wants(), holder).Result(); err == nil {
		n--
	}
	return int(n), nil
}
