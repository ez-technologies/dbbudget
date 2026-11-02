package dbbudget_test

import (
	"testing"

	"github.com/ez-technologies/dbbudget"
	"github.com/ez-technologies/dbbudget/budgettest"
)

func TestMemoryStore(t *testing.T) {
	budgettest.Store(t, func(t *testing.T) dbbudget.Store { return dbbudget.NewMemoryStore() })
}
