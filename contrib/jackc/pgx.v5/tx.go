package pgx

import (
	"context"
	"fmt"
	"gopkg.in/DataDog/dd-trace-go.v1/internal"
	"runtime/trace"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// tracedTx is a traced version of sql.Tx
type tracedTx struct {
	pgx.Tx
	*traceParams
}

func noopTaskEnd() {}

// startTraceTask creates an execution trace task with the given name, and
// returns a context.Context associated with the task, and a function to end the
// task.
//
// This is intended for cases where a span would normally be created after an
// operation, where the operation may have been skipped and a span would be
// noisy. Execution trace tasks must cover the actual duration of the operation
// and can't be altered after the fact.
func startTraceTask(ctx context.Context, name string) (context.Context, func()) {
	if !trace.IsEnabled() {
		return ctx, noopTaskEnd
	}
	ctx, task := trace.NewTask(ctx, name)
	return internal.WithExecutionTraced(ctx), task.End
}

// Commit sends a span at the end of the transaction
func (t *tracedTx) Commit(ctx context.Context) (err error) {
	ctx, end := startTraceTask(ctx, QueryTypeCommit)
	defer end()

	start := time.Now()
	err = t.Tx.Commit(ctx)
	t.tryTrace(ctx, QueryTypeCommit, "", start, err)
	return err
}

// Rollback sends a span if the connection is aborted
func (t *tracedTx) Rollback(ctx context.Context) (err error) {
	ctx, end := startTraceTask(ctx, QueryTypeRollback)
	defer end()

	start := time.Now()
	err = t.Tx.Rollback(ctx)
	t.tryTrace(ctx, QueryTypeRollback, "", start, err)
	return err
}

func (t *tracedTx) Prepare(ctx context.Context, name, query string) (*pgconn.StatementDescription, error) {
	start := time.Now()
	mode := t.cfg.dbmPropagationMode
	if mode == tracer.DBMPropagationModeFull {
		// no context other than service in prepared statements
		mode = tracer.DBMPropagationModeService
	}
	cquery, spanID := t.injectComments(ctx, query, mode)
	ctx, end := startTraceTask(ctx, QueryTypePrepare)
	defer end()
	stmt, err := t.Tx.Prepare(ctx, name, cquery)
	t.tryTrace(ctx, QueryTypePrepare, query, start, err, append(withDBMTraceInjectedTag(mode), tracer.WithSpanID(spanID))...)
	return stmt, err
}

func (t *tracedTx) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	cquery, spanID := t.injectComments(ctx, query, t.cfg.dbmPropagationMode)
	ctx, end := startTraceTask(ctx, QueryTypeExec)
	defer end()
	r, err := t.Tx.Exec(ctx, cquery, args...)
	t.tryTrace(ctx, QueryTypeExec, query, start, err, append(withDBMTraceInjectedTag(t.cfg.dbmPropagationMode), tracer.WithSpanID(spanID))...)
	return r, err
}

func (t *tracedTx) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	start := time.Now()
	cquery, spanID := t.injectComments(ctx, query, t.cfg.dbmPropagationMode)
	ctx, end := startTraceTask(ctx, QueryTypeQuery)
	defer end()
	r, err := t.Tx.Query(ctx, cquery, args...)
	t.tryTrace(ctx, QueryTypeQuery, query, start, err, append(withDBMTraceInjectedTag(t.cfg.dbmPropagationMode), tracer.WithSpanID(spanID))...)
	return r, err
}

func (t *tracedTx) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	start := time.Now()
	cquery, spanID := t.injectComments(ctx, query, t.cfg.dbmPropagationMode)
	ctx, end := startTraceTask(ctx, QueryTypeQuery)
	defer end()
	r := t.Tx.QueryRow(ctx, cquery, args...)
	t.tryTrace(ctx, QueryTypeQuery, query, start, nil, append(withDBMTraceInjectedTag(t.cfg.dbmPropagationMode), tracer.WithSpanID(spanID))...)
	return r
}

func (t *tracedTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	start := time.Now()
	ctx, end := startTraceTask(ctx, QueryTypeCopyFrom)
	defer end()
	r, err := t.Tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
	t.tryTrace(ctx, QueryTypeCopyFrom, fmt.Sprintf("copy_from %s", tableName.Sanitize()), start, err)
	return r, err
}
