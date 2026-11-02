package redisstore_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ez-technologies/dbbudget"
	"github.com/ez-technologies/dbbudget/budgettest"
	"github.com/ez-technologies/dbbudget/redisstore"
)

func TestStore(t *testing.T) {
	addr := os.Getenv("DBBUDGET_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("DBBUDGET_TEST_REDIS_ADDR is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	budgettest.Store(t, func(t *testing.T) dbbudget.Store {
		return redisstore.New(c, fmt.Sprintf("budgettest:%d", time.Now().UnixNano()))
	})
}
