// Package testutil provides small helpers shared by the gateway's
// test packages. The goal is to keep the per-test boilerplate down
// without creating a behavioural dependency: every helper here is
// inert outside of *_test.go files.
package testutil

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Tracer is the in-process OTel tracer used by tests. It records
// every emitted span into an in-memory ring so tests can assert on
// span names, attributes, and errored-state without standing up
// Jaeger or running an OTLP collector.
type Tracer struct {
	provider *sdktrace.TracerProvider
	rec      *tracetest.SpanRecorder
}

// NewTracer constructs and registers a fresh in-process tracer.
// The tracer is also installed as the global OTel provider, so
// callers do not need to thread it through the production code
// path. Use Close to release the underlying goroutine.
//
// The integration is intentionally minimal: no exporters, no
// resource attributes. Tests that need finer control should
// build their own provider.
func NewTracer() *Tracer {
	rec := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(provider)
	return &Tracer{provider: provider, rec: rec}
}

// Close releases the tracer's resources. Safe to call from
// t.Cleanup. After Close, further span recording is undefined.
func (t *Tracer) Close() {
	_ = t.provider.Shutdown(context.Background())
}

// Spans returns a snapshot of every span recorded so far.
func (t *Tracer) Spans() []sdktrace.ReadOnlySpan {
	return t.rec.Ended()
}

// DoRequest is a convenience wrapper around httptest.NewRequest +
// httptest.NewRecorder + handler.ServeHTTP that returns the
// recorder ready for assertions. The handler is called with a
// context derived from ctx, which lets tests inject fake spans or
// cancellation without re-wiring each test.
func DoRequest(h http.Handler, method, target string, opts ...RequestOption) *httptest.ResponseRecorder {
	cfg := requestConfig{headers: http.Header{}, ctx: context.Background()}
	for _, o := range opts {
		o(&cfg)
	}
	req := httptest.NewRequest(method, target, bytes.NewReader(cfg.body))
	req = req.WithContext(cfg.ctx)
	for k, vs := range cfg.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// RequestOption customises a DoRequest call. Defined here (rather
// than in the test packages) so per-test variations do not
// require every test to grow a long signature.
type RequestOption func(*requestConfig)

type requestConfig struct {
	ctx     context.Context
	headers http.Header
	body    []byte
}

// WithHeader sets a single header on the outgoing request.
func WithHeader(key, value string) RequestOption {
	return func(c *requestConfig) {
		c.headers.Set(key, value)
	}
}

// WithContext attaches a custom request context. Tests that need
// to exercise a deadline / cancellation use this to inject one.
func WithContext(ctx context.Context) RequestOption {
	return func(c *requestConfig) {
		c.ctx = ctx
	}
}

// WithBody attaches a request body. The reader is buffered so the
// underlying handler can rewind if needed.
func WithBody(b []byte) RequestOption {
	return func(c *requestConfig) {
		c.body = b
	}
}

// NewTraceID returns a fresh 32-hex-char trace id, primarily so
// tests can mint a deterministic-looking id without depending on
// the production randomness source.
func NewTraceID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// MustNotExceed asserts the wrapped function returns within d. It
// is the test-side equivalent of the production timeout helper
// and exists so tests can flag hangs as failures rather than
// letting go test time out the whole suite.
func MustNotExceed(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("operation did not complete within %s", d)
	}
}

// drainBody is a tiny helper for tests that need to read the full
// response body and assert on it. Using io.ReadAll keeps callers
// independent of the testutil package's requestConfig layout.
func drainBody(r io.Reader) []byte {
	b, _ := io.ReadAll(r)
	return b
}

// ReadBody is the public form of drainBody, kept here so tests do
// not have to re-import io for a one-liner.
func ReadBody(r io.Reader) []byte { return drainBody(r) }
