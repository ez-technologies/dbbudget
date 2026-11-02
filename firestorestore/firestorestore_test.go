package firestorestore_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/ez-technologies/dbbudget"
	"github.com/ez-technologies/dbbudget/budgettest"
	"github.com/ez-technologies/dbbudget/firestorestore"
)

// TestStore runs against the Firestore emulator; never Google Cloud.
func TestStore(t *testing.T) {
	host := os.Getenv("DBBUDGET_TEST_FIRESTORE_EMULATOR")
	if host == "" {
		t.Skip("DBBUDGET_TEST_FIRESTORE_EMULATOR is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	t.Setenv("FIRESTORE_EMULATOR_HOST", host)
	c, err := firestore.NewClient(context.Background(), "dbbudget-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	budgettest.Store(t, func(t *testing.T) dbbudget.Store {
		return firestorestore.New(c, "budgettest", fmt.Sprintf("b%d", time.Now().UnixNano()))
	})
}
