package pgx

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

// New creates a new LooselyTracedPool. See ParseConfig for information on connString format.
func New(ctx context.Context, connString string, opts ...Option) (*LooselyTracedPool, error) {
	poolCfg, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	return NewWithConfig(ctx, poolCfg, opts...)
}

// NewWithConfig creates a new LooselyTracedPool. config must have been created by ParseConfig.
func NewWithConfig(ctx context.Context, poolCfg *pgxpool.Config, opts ...Option) (*LooselyTracedPool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	cfg := new(config)
	defaults(cfg)
	for _, fn := range opts {
		fn(cfg)
	}
	return &LooselyTracedPool{
		Pool:        pool,
		traceParams: &traceParams{cfg: cfg},
	}, nil
}

// ParseConfig builds a Config from connString. It parses connString with the same behavior as pgx.ParseConfig with the
// addition of the following variables:
//
//   - pool_max_conns: integer greater than 0
//   - pool_min_conns: integer 0 or greater
//   - pool_max_conn_lifetime: duration string
//   - pool_max_conn_idle_time: duration string
//   - pool_health_check_period: duration string
//   - pool_max_conn_lifetime_jitter: duration string
//
// See Config for definitions of these arguments.
//
//	# Example DSN
//	user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
//	# Example URL
//	postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
func ParseConfig(connString string) (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(connString)
}
