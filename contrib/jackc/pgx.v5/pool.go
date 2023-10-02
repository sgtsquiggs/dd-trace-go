package pgx

import (
	"context"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/telemetry"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const componentName = "jackc/pgx.v5"

func init() {
	telemetry.LoadIntegration(componentName)
	tracer.MarkIntegrationImported(componentName)
}

// QueryType represents the different available traced db queries.
type QueryType string

const (
	// QueryTypeConnect is used for Connect traces.
	QueryTypeConnect QueryType = "Connect"
	// QueryTypeQuery is used for Query traces.
	QueryTypeQuery = "Query"
	// QueryTypePing is used for Ping traces.
	QueryTypePing = "Ping"
	// QueryTypePrepare is used for Prepare traces.
	QueryTypePrepare = "Prepare"
	// QueryTypeExec is used for Exec traces.
	QueryTypeExec = "Exec"
	// QueryTypeBegin is used for Begin traces.
	QueryTypeBegin = "Begin"
	// QueryTypeClose is used for Close traces.
	QueryTypeClose = "Close"
	// QueryTypeCommit is used for Commit traces.
	QueryTypeCommit = "Commit"
	// QueryTypeRollback is used for Rollback traces.
	QueryTypeRollback = "Rollback"
	// QueryTypeCopyFrom is used for CopyFrom traces.
	QueryTypeCopyFrom = "CopyFrom"
)

const (
	keyDBMTraceInjected = "_dd.dbm_trace_injected"
)

// LooselyTracedPool holds a traced *pgxpool.Pool with tracing parameters.
type LooselyTracedPool struct {
	*pgxpool.Pool
	*traceParams
}

// WrappedPool returns the wrapped connection object.
func (tp *LooselyTracedPool) WrappedPool() *pgxpool.Pool {
	return tp.Pool
}

// BeginTx acquires a connection from the Pool and starts a transaction with pgx.TxOptions determining the transaction mode.
// Unlike database/sql, the context only affects the begin command. i.e. there is no auto-rollback on context cancellation.
// *pgxpool.Tx is returned, which implements the pgx.Tx interface.
// Commit or Rollback must be called on the returned transaction to finalize the transaction block.
func (tp *LooselyTracedPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	start := time.Now()
	ctx, end := startTraceTask(ctx, QueryTypeBegin)
	defer end()
	tx, err := tp.Pool.BeginTx(ctx, txOptions)
	tp.tryTrace(ctx, QueryTypeBegin, "", start, err)
	if err != nil {
		return nil, err
	}
	return &tracedTx{Tx: tx, traceParams: tp.traceParams}, nil
}

// traceParams stores all information related to tracing the driver.Conn
type traceParams struct {
	cfg  *config
	meta map[string]string
}

type contextKey int

const spanTagsKey contextKey = 0 // map[string]string

// WithSpanTags creates a new context containing the given set of tags. They will be added
// to any query created with the returned context.
func WithSpanTags(ctx context.Context, tags map[string]string) context.Context {
	return context.WithValue(ctx, spanTagsKey, tags)
}

// injectComments returns the query with SQL comments injected according to the comment injection mode along
// with a span ID injected into SQL comments. The returned span ID should be used when the SQL span is created
// following the traced database call.
func (tp *traceParams) injectComments(ctx context.Context, query string, mode tracer.DBMPropagationMode) (cquery string, spanID uint64) {
	// The sql span only gets created after the call to the database because we need to be able to skip spans
	// when a driver returns driver.ErrSkip. In order to work with those constraints, a new span id is generated and
	// used during SQL comment injection and returned for the sql span to be used later when/if the span
	// gets created.
	var spanCtx ddtrace.SpanContext
	if span, ok := tracer.SpanFromContext(ctx); ok {
		spanCtx = span.Context()
	}
	carrier := tracer.SQLCommentCarrier{Query: query, Mode: mode, DBServiceName: tp.cfg.serviceName}
	if err := carrier.Inject(spanCtx); err != nil {
		// this should never happen
		log.Warn("contrib/jackc/pgx.v5: failed to inject query comments: %v", err)
	}
	return carrier.Query, carrier.SpanID
}

func withDBMTraceInjectedTag(mode tracer.DBMPropagationMode) []tracer.StartSpanOption {
	if mode == tracer.DBMPropagationModeFull {
		return []tracer.StartSpanOption{tracer.Tag(keyDBMTraceInjected, true)}
	}
	return nil
}

// tryTrace will create a span using the given arguments, but will act as a no-op when err is driver.ErrSkip.
func (tp *traceParams) tryTrace(ctx context.Context, qtype QueryType, query string, startTime time.Time, err error, spanOpts ...ddtrace.StartSpanOption) {
	if tp.cfg.ignoreQueryTypes != nil {
		if _, ok := tp.cfg.ignoreQueryTypes[qtype]; ok {
			return
		}
	}
	if _, exists := tracer.SpanFromContext(ctx); tp.cfg.childSpansOnly && !exists {
		return
	}
	opts := append(spanOpts,
		tracer.ServiceName(tp.cfg.serviceName),
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.StartTime(startTime),
		tracer.Tag(ext.Component, "pgx/v5"),
		tracer.Tag(ext.SpanKind, ext.SpanKindClient),
		tracer.Tag(ext.DBSystem, "postgres"),
	)
	if tp.cfg.tags != nil {
		for key, tag := range tp.cfg.tags {
			opts = append(opts, tracer.Tag(key, tag))
		}
	}
	if !math.IsNaN(tp.cfg.analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, tp.cfg.analyticsRate))
	}
	span, _ := tracer.StartSpanFromContext(ctx, tp.cfg.spanName, opts...)
	resource := string(qtype)
	if query != "" {
		resource = query
	}
	span.SetTag("sql.query_type", string(qtype))
	span.SetTag(ext.ResourceName, resource)
	for k, v := range tp.meta {
		span.SetTag(k, v)
	}
	if meta, ok := ctx.Value(spanTagsKey).(map[string]string); ok {
		for k, v := range meta {
			span.SetTag(k, v)
		}
	}
	if err != nil && (tp.cfg.errCheck == nil || tp.cfg.errCheck(err)) {
		span.SetTag(ext.Error, err)
	}
	span.Finish()
}
