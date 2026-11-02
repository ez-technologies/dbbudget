# AGENTS.md: rules for anyone (human or agent) changing code that uses dbbudget

dbbudget keeps the database connections of many instances under one total. The README has the details; these are the
rules and the traps.

## Rules
1. **Count physical connections.** Put the budget in the dialer (`Dial`, `pgxbudget.Configure`), never around
   queries: a pool reuses connections.
2. **Always set the pressure hook** (`SQLPressure`, `pgxbudget.Pressure`, through `SetOnPressure`): without it a
   waiting instance, or this instance itself, waits on idle connections nobody closes.
3. **Close idle connections soon** (`SetConnMaxIdleTime` / `MaxConnIdleTime` about the budget's `IdleGrace`), so
   slots go back.
4. **`Max` is the database's `max_connections` minus room** for migrations, backups, admin sessions and any pool that
   is not budgeted.
5. **Behind a real pooler use `Fixed`** (PgBouncer, RDS Proxy, Cloud SQL pooling): the pooler bounds the database.
6. **Test a new store with `budgettest`** on a local emulator; never against a cloud account.

## Traps
- **Clocks**: NATS KV, DynamoDB and Firestore stores compare expiry times written by the instances' clocks. Keep
  clocks in sync and the TTL well above the skew.
- **Store down**: an instance keeps at most one connection (new ones wait). Connections already open stay open.
- **Session state**: connections can be closed for other instances at any time when idle; never keep session state
  (temporary tables, `SET`, session advisory locks) across statements on a pooled connection. Under PgBouncer in
  transaction mode use the simple protocol (pgx `QueryExecModeSimpleProtocol`) or PgBouncer 1.21+ prepared statements.
- **MinHold**: each instance keeps one slot while idle; with more instances than `Max`, the last ones wait until
  someone wants and an idle one gives way.
- The library is generic: keep application names, data and secrets out of it.

## Checks before a pull request
`gofmt -l .` (empty), `go vet ./...`, `eval "$(scripts/test-env.sh up)" && go test -race ./...`.
