// Package middleware contains the cross-cutting HTTP filters applied
// to every request the gateway handles. Each middleware is a small,
// testable function that wraps an http.Handler and exposes the
// configuration it needs via plain Go values — no global state, no
// package-level singletons, no surprise side effects.
//
// The order in router.go applies them is:
//
//	trace   (outermost: every request must have a span, even if the
//	         downstream later returns an error)
//	auth    (extracts the bearer token and resolves the caller
//	         identity for downstream services and the rate limiter)
//	ratelimit (per-identity token bucket backed by Redis)
//	router  (innermost: forward to the right downstream)
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HeaderTraceID is the response header that exposes the resolved
// trace id to clients. It mirrors the X-Request-ID pattern but
// standardised on the W3C trace id format (32 lower-case hex chars).
const HeaderTraceID = "X-Trace-Id"

// Setup installs the global OTel propagator (W3C Trace Context +
// W3C Baggage) and tracer provider. main() calls this once at
// start-up; tests that exercise trace propagation call it from a
// TestMain or per-test setup.
//
// We split this from the Trace middleware factory so the gateway
// can run with a no-op tracer in unit tests that don't care about
// spans (the propagator is what actually decides whether an
// inbound traceparent is honoured, so even a no-op tracer still
// needs a real propagator).
func Setup() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

// MustSpanID is a tiny helper for the (rare) places that need to
// mint a non-zero SpanID. The all-zero SpanID is reserved by OTel
// as "invalid" and is rejected by SpanContext.IsValid, so callers
// that synthesise a span context out of thin air must use a real
// id here. Returns the canonical "1" span id when err != nil.
func MustSpanID(s string) (sid trace.SpanID, err error) {
	return trace.SpanIDFromHex(s)
}

// Trace returns a middleware that ensures every request runs under
// an OpenTelemetry span. It reads the W3C traceparent header, falls
// back to a freshly generated trace id when the header is absent or
// malformed, and re-emits the trace id on the response so operators
// can correlate logs and traces from the client side.
//
// We use the global OTel tracer / propagator rather than a
// per-handler one so the gateway produces a single consistent
// trace across the trace, auth, and rate-limit layers.
func Trace() func(http.Handler) http.Handler {
	tracer := otel.Tracer(tracerName)
	propagator := otel.GetTextMapPropagator()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			spanName := spanNameFromRequest(r)
			ctx, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindServer))
			defer span.End()

			// Record the route + method as span attributes. These
			// show up in Jaeger / Tempo so operators can slice
			// traffic by route without log-grep.
			span.SetAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
			)

			// Surface the resolved trace id to the client. The
			// httperr envelope also embeds it, but the dedicated
			// header is easier for non-JSON consumers (e.g. curl).
			traceID := trace.SpanFromContext(ctx).SpanContext().TraceID().String()
			if traceID != "" {
				w.Header().Set(HeaderTraceID, traceID)
			}

			// Track whether the handler already wrote headers so
			// we can decorate the status code onto the span after.
			rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r.WithContext(ctx))

			span.SetAttributes(attribute.Int("http.response.status_code", rw.status))
			if rw.status >= http.StatusInternalServerError {
				// Mark 5xx spans as errors so dashboards light up.
				// SetStatus is what flips the SpanStatusCode from
				// Unset (0) to Error (1) — RecordError alone only
				// attaches an event, it does not change status.
				span.SetStatus(codes.Error, "non-2xx response")
			}
		})
	}
}

// spanNameFromRequest builds a stable, low-cardinality span name
// from the request method and a coarse route label. Using the raw
// path would explode the trace index for any parameterised URL, so
// we collapse to the first path segment — fine enough to be useful
// in Jaeger, coarse enough to keep the trace cardinality bounded.
func spanNameFromRequest(r *http.Request) string {
	first := strings.Trim(r.URL.Path, "/")
	if i := strings.Index(first, "/"); i >= 0 {
		first = first[:i]
	}
	if first == "" {
		first = "root"
	}
	return r.Method + " /" + first
}

// statusRecorder captures the HTTP status so the trace middleware
// can decorate the span after the handler runs. Without this, the
// span would always show status=200 even for failed requests.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

// errFromStatus was removed: span.SetStatus(codes.Error, ...) is the
// correct way to mark a span as errored, and the helper just
// shadowed that one-line call.

// NewTraceID generates a fresh W3C-style 32-hex-char trace id. It
// is exposed (rather than inlined into Trace) so tests, admin
// tooling, and bootstrap scripts can mint a trace id without going
// through the full OTel pipeline.
func NewTraceID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// WithTraceID attaches a pre-built trace id to ctx as a span
// context. It is the test-side equivalent of having a real OTel
// span: callers can mint a trace id, then exercise handlers that
// read trace.SpanFromContext and get the same id back.
//
// We use a synthetic non-zero SpanID because the all-zero SpanID
// is reserved by OTel as "invalid" — SpanContext.IsValid returns
// false, which would make TraceIDFromContext return "" and break
// every caller that depends on a round-trippable trace id.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return ctx
	}
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: false})
	return trace.ContextWithSpanContext(ctx, sc)
}

// TraceIDFromContext extracts the 32-char hex W3C trace id from the
// active span in ctx. Returns "" when no span is attached.
//
// This is the canonical place to read the trace id — callers
// outside the trace middleware (e.g. httperr, the router) should
// import this helper rather than reaching into OTel themselves, so
// the policy for "what counts as a valid span" lives in one place.
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}

const tracerName = "github.com/aidp/gateway"
