// Package pgxbudget puts a dbbudget.Budget in front of pgx: every physical connection (pgxpool, pgx, or
// database/sql through pgx's stdlib) waits for a permit and returns it when it closes.
package pgxbudget

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ez-technologies/dbbudget"
)

// Configure wraps the connection config's dialer with the budget. Call it on pgxpool.ParseConfig's ConnConfig, or on
// the *pgx.ConnConfig you give stdlib.OpenDB.
func Configure(cc *pgx.ConnConfig, b dbbudget.Budget) {
	var next dbbudget.DialFunc
	if cc.DialFunc != nil {
		next = dbbudget.DialFunc(cc.DialFunc)
	}
	cc.DialFunc = pgconn.DialFunc(dbbudget.Dial(b, next))
}

// NewPool opens a pgxpool whose connections go through the budget. MaxConns is a ceiling; the budget decides how
// many open. Idle connections close after idle (default 30s), so their slots can go back.
func NewPool(ctx context.Context, url string, b dbbudget.Budget, maxConns int32, idle time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	Configure(cfg.ConnConfig, b)
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MinConns = 0
	if idle <= 0 {
		idle = 30 * time.Second
	}
	cfg.MaxConnIdleTime = idle
	cfg.HealthCheckPeriod = idle / 2
	return pgxpool.NewWithConfig(ctx, cfg)
}

// Pressure closes the pool's idle connections: use it as Options.OnPressure, so their slots go to a waiting instance.
func Pressure(pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, c := range pool.AcquireAllIdle(ctx) {
		conn := c.Hijack() // out of the pool; closing it returns its permit
		_ = conn.Close(ctx)
	}
}
