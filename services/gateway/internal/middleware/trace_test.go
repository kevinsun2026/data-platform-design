package middleware_test

import (
	"context"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/aidp/gateway/internal/middleware"
	"github.com/aidp/gateway/internal/testutil"
)

var traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// observedTrace captures the trace id the terminal handler saw, so
// tests can assert on the value that was actually attached to the
// request context (rather than just the response header, which
// could be set by a different code path).
type observedTrace struct {
	id      string
	method  string
	gotPath string
}

func newTraceRecorder() (*observedTrace, http.Handler) {
	obs := &observedTrace{}
	return obs, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obs.id = middleware.TraceIDFromContext(r.Context())
		obs.method = r.Method
		obs.gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
}

// TestTracePropagatesInboundTraceparent verifies the W3C contract:
// when a client sends a valid traceparent header, the gateway
// adopts the inbound trace id rather than minting a new one.
func TestTracePropagatesInboundTraceparent(t *testing.T) {
	middleware.Setup()
	tr := testutil.NewTracer()
	t.Cleanup(tr.Close)

	obs, terminal := newTraceRecorder()
	h := middleware.Trace()(terminal)

	const inboundTraceID = "0af7651916cd43dd8448eb211c80319c"
	const inboundSpanID = "b7ad6b7169203331"
	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("traceparent", "00-"+inboundTraceID+"-"+inboundSpanID+"-01"),
	)

	require.Equal(t, http.StatusOK, rec.Code, "terminal handler should not be short-circuited")
	assert.Equal(t, inboundTraceID, obs.id, "trace id must be propagated from inbound traceparent")
	assert.Equal(t, "GET", obs.method)
	assert.Equal(t, "/api/v1/iam/users", obs.gotPath)
	// The response header mirrors the resolved trace id so clients
	// without traceparent still get something to correlate against.
	assert.Equal(t, inboundTraceID, rec.Header().Get(middleware.HeaderTraceID))
}

// TestTraceSynthesisesNewIDWhenAbsent covers the cold-start path:
// a request with no traceparent header still gets a span and a
// response header carrying the generated trace id.
func TestTraceSynthesisesNewIDWhenAbsent(t *testing.T) {
	middleware.Setup()
	tr := testutil.NewTracer()
	t.Cleanup(tr.Close)

	obs, terminal := newTraceRecorder()
	h := middleware.Trace()(terminal)

	rec := testutil.DoRequest(h, http.MethodGet, "/healthz")

	require.Equal(t, http.StatusOK, rec.Code)
	require.NotEmpty(t, obs.id, "trace id must be minted for traceparent-less requests")
	assert.Regexp(t, traceIDPattern, obs.id)
	assert.Equal(t, obs.id, rec.Header().Get(middleware.HeaderTraceID))
}

// TestTraceToleratesMalformedTraceparent documents the fallback
// behaviour for an upstream that sends garbage: the gateway must
// not crash, and must produce a fresh trace id rather than
// propagating the broken value.
func TestTraceToleratesMalformedTraceparent(t *testing.T) {
	middleware.Setup()
	tr := testutil.NewTracer()
	t.Cleanup(tr.Close)

	obs, terminal := newTraceRecorder()
	h := middleware.Trace()(terminal)

	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/audit/events",
		testutil.WithHeader("traceparent", "this-is-not-a-valid-traceparent"),
	)

	require.Equal(t, http.StatusOK, rec.Code)
	require.NotEmpty(t, obs.id)
	assert.NotEqual(t, "this-is-not-a-valid-traceparent", obs.id)
	assert.Regexp(t, traceIDPattern, obs.id)
}

// TestTraceDecoratorRecords5xx guards the operator-visible behaviour:
// a downstream 5xx surfaces as an errored span so Jaeger / Tempo
// can light it up red. The recorder installed by testutil lets us
// inspect the recorded span directly.
func TestTraceDecoratorRecords5xx(t *testing.T) {
	middleware.Setup()
	tr := testutil.NewTracer()
	t.Cleanup(tr.Close)

	fail := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	h := middleware.Trace()(fail)

	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/login")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	spans := tr.Spans()
	require.NotEmpty(t, spans, "trace middleware must emit a span")
	// Find the server-side span (the one the trace middleware creates).
	var serverSpan *sdktrace.ReadOnlySpan
	for i := range spans {
		if spans[i].SpanKind() == trace.SpanKindServer {
			serverSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, serverSpan, "expected a server-kind span")
	status := (*serverSpan).Status()
	assert.Equal(t, codes.Error, status.Code, "5xx should mark span as errored")
}

// TestWithTraceIDHelper covers the test-side escape hatch: helpers
// outside the trace middleware (e.g. httperr tests) need a way to
// synthesise a span context without spinning up OTel.
func TestWithTraceIDHelper(t *testing.T) {
	const want = "0af7651916cd43dd8448eb211c80319c"
	ctx := middleware.WithTraceID(context.Background(), want)
	assert.Equal(t, want, middleware.TraceIDFromContext(ctx))
}

// TestNewTraceIDFormat pins the trace id format (32 lower-case hex
// chars) so callers that grep logs / URLs do not get bitten by an
// accidental capitalisation or longer-id change.
func TestNewTraceIDFormat(t *testing.T) {
	id := middleware.NewTraceID()
	assert.Len(t, id, 32)
	assert.Regexp(t, traceIDPattern, id)
	// Two consecutive calls must not collide.
	assert.NotEqual(t, id, middleware.NewTraceID())
}
