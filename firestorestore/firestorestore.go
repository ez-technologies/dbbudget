// Package firestorestore is a dbbudget Store on Google Cloud Firestore: one document per slot ("<budget>-slot-<n>")
// and per waiting holder ("<budget>-want-<holder>") in a collection (default "dbbudget"), each changed in a
// transaction. A TTL policy on the field "ttl" is optional cleanup.
package firestorestore

import (
	"context"
	"math/rand/v2"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ez-technologies/dbbudget"
)

// Store is the Firestore store.
type Store struct {
	c    *firestore.Client
	col  string
	name string
	now  func() time.Time
}

var _ dbbudget.Store = (*Store)(nil)

// New returns the budget name in collection (default "dbbudget").
func New(c *firestore.Client, collection, name string) *Store {
	if collection == "" {
		collection = "dbbudget"
	}
	if name == "" {
		name = "default"
	}
	return &Store{c: c, col: collection, name: name, now: time.Now}
}

type doc struct {
	Budget  string    `firestore:"budget"`
	Kind    string    `firestore:"kind"`
	Holder  string    `firestore:"holder"`
	Expires time.Time `firestore:"expires"`
	TTL     time.Time `firestore:"ttl"`
}

var unsafe = regexp.MustCompile(`[^A-Za-z0-9_-]`)

func (s *Store) slot(i int) *firestore.DocumentRef {
	return s.c.Collection(s.col).Doc(s.name + "-slot-" + strconv.Itoa(i))
}

func (s *Store) want(h string) *firestore.DocumentRef {
	return s.c.Collection(s.col).Doc(s.name + "-want-" + unsafe.ReplaceAllString(h, "_"))
}

func (s *Store) doc(kind, holder string, exp time.Time) doc {
	return doc{Budget: s.name, Kind: kind, Holder: holder, Expires: exp, TTL: exp.Add(time.Hour)}
}

func read(tx *firestore.Transaction, ref *firestore.DocumentRef) (*doc, error) {
	snap, err := tx.Get(ref)
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var d doc
	if err := snap.DataTo(&d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) Take(ctx context.Context, holder string, max int, ttl time.Duration) (int, error) {
	for _, i := range rand.Perm(max) {
		got := false
		err := s.c.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			got = false
			cur, err := read(tx, s.slot(i))
			if err != nil {
				return err
			}
			now := s.now()
			if cur != nil && cur.Expires.After(now) {
				return nil
			}
			got = true
			return tx.Set(s.slot(i), s.doc("slot", holder, now.Add(ttl)))
		})
		if err != nil {
			return -1, err
		}
		if got {
			_, _ = s.want(holder).Delete(ctx)
			return i, nil
		}
	}
	return -1, nil
}

func (s *Store) Renew(ctx context.Context, holder string, slot int, ttl time.Duration) error {
	return s.c.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		cur, err := read(tx, s.slot(slot))
		if err != nil {
			return err
		}
		if cur == nil || cur.Holder != holder {
			return dbbudget.ErrLost
		}
		return tx.Set(s.slot(slot), s.doc("slot", holder, s.now().Add(ttl)))
	})
}

func (s *Store) Give(ctx context.Context, holder string, slot int) error {
	return s.c.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		cur, err := read(tx, s.slot(slot))
		if err != nil || cur == nil || cur.Holder != holder {
			return err
		}
		return tx.Delete(s.slot(slot))
	})
}

func (s *Store) Want(ctx context.Context, holder string, ttl time.Duration) error {
	_, err := s.want(holder).Set(ctx, s.doc("want", holder, s.now().Add(ttl)))
	return err
}

func (s *Store) Wanted(ctx context.Context, holder string) (int, error) {
	it := s.c.Collection(s.col).Where("budget", "==", s.name).Where("kind", "==", "want").Documents(ctx)
	defer it.Stop()
	n, now := 0, s.now()
	for {
		snap, err := it.Next()
		if err == iterator.Done {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
		var d doc
		if err := snap.DataTo(&d); err != nil {
			return 0, err
		}
		if d.Holder != holder && d.Expires.After(now) {
			n++
		}
	}
}
