// Package httperr defines the canonical error envelope returned by the
// AIDP gateway and the helpers used by every handler / middleware to write
// it back to clients.
//
// The wire format is intentionally minimal — a stable three-field JSON
// object — so it matches the Python platform's error response and is
// trivial for clients to consume without a generated SDK.
//
//	{"code": "NOT_FOUND", "message": "user u-123 not found", "trace_id": "0af7651916cd43dd8448eb211c80319c"}
//
// "code" is a short stable token that clients can switch on (NOT_FOUND,
// UNAUTHORIZED, RATE_LIMITED, UPSTREAM_ERROR, INTERNAL, ...). "message"
// is human-readable and safe to surface to the user. "trace_id" is the
// 32-char hex W3C trace id of the originating request, set by the trace
// middleware so operators can correlate a client report with the
// gateway / upstream traces.
package httperr

import (
	"context"
	"encoding/json"
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

// ErrorCode is a stable, switch-friendly identifier for an error class.
//
// Mirrors aidp_common.errors.ErrorCode on the Python side so the same
// client error handling works for both stacks.
type ErrorCode string

// Canonical error codes returned by the gateway. Downstream services
// surface their own codes, which the gateway passes through unchanged.
const (
	CodeUnauthorized   ErrorCode = "UNAUTHORIZED"
	CodeForbidden      ErrorCode = "FORBIDDEN"
	CodeNotFound       ErrorCode = "NOT_FOUND"
	CodeValidation     ErrorCode = "VALIDATION"
	CodeRateLimited    ErrorCode = "RATE_LIMITED"
	CodeUpstreamError  ErrorCode = "UPSTREAM_ERROR"
	CodeInternal       ErrorCode = "INTERNAL"
	CodeServiceUnavail ErrorCode = "SERVICE_UNAVAILABLE"
)

// Error is the structured error the gateway returns to clients.
//
// It carries the wire-level fields (Code, Message) plus the HTTP status
// the gateway should reply with. Trace_id is resolved from the OTel
// context at write time so callers do not have to thread it through
// every layer.
type Error struct {
	// Code is the canonical, machine-readable identifier (see ErrorCode).
	Code ErrorCode `json:"code"`
	// Message is the human-readable description. Must be safe to surface
	// to the end user — no internal paths, no stack frames, no secrets.
	Message string `json:"message"`
	// Status is the HTTP status the gateway returns. Defaults are
	// derived from Code when callers use the New* constructors below.
	Status int `json:"-"`
	// Cause is the optional underlying error kept for server-side logs.
	// Never serialised into the response body.
	Cause error `json:"-"`
}

// Error implements the error interface so *Error can flow through
// normal Go error paths. The returned string is for logs only.
func (e *Error) Error() string {
	if e.Cause != nil {
		return string(e.Code) + ": " + e.Message + ": " + e.Cause.Error()
	}
	return string(e.Code) + ": " + e.Message
}

// Unwrap exposes the underlying cause to errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.Cause }

// Envelope is the wire shape returned to clients. It is exported so
// generated client SDKs can decode it without re-declaring the schema.
type Envelope struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	TraceID string    `json:"trace_id,omitempty"`
}

// Write serialises the error to w using the trace id resolved from ctx
// (falls back to an empty string if no span is attached). It also sets
// the Content-Type header so misbehaving clients see JSON, not HTML.
//
// Write is the only place that constructs the wire envelope, so
// changing the format requires a single edit and a single test.
func (e *Error) Write(ctx context.Context, w http.ResponseWriter) {
	body, _ := json.Marshal(Envelope{
		Code:    e.Code,
		Message: e.Message,
		TraceID: traceIDFromContext(ctx),
	})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.Status)
	// WriteHeader may have already been called by the caller; ignore
	// the returned error because the client will surface a connection
	// reset on its own.
	_, _ = w.Write(body)
}

// traceIDFromContext extracts the 32-char hex W3C trace id from the
// active span in ctx. Returns "" when no span is attached.
//
// This is intentionally a private duplicate of the same helper in
// the middleware package: importing middleware from httperr would
// create an import cycle (middleware uses httperr to build the
// "5xx = errored span" sentinel error). The two implementations
// share the same one-line OTel call; a regression test on either
// path would catch any drift.
func traceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}

// New builds an Error with an explicit HTTP status. Prefer the typed
// constructors (NewUnauthorized, NewRateLimited, ...) for the common
// cases so the status-code-to-code mapping stays in one place.
func New(code ErrorCode, message string, status int) *Error {
	return &Error{Code: code, Message: message, Status: status}
}

// Wrap attaches an underlying cause to a fresh Error. The cause is
// recorded for logs but never serialised into the response.
func Wrap(code ErrorCode, message string, status int, cause error) *Error {
	return &Error{Code: code, Message: message, Status: status, Cause: cause}
}

// NewUnauthorized reports that the caller did not present credentials
// the gateway could pass downstream (HTTP 401).
func NewUnauthorized(msg string) *Error {
	if msg == "" {
		msg = "unauthorized"
	}
	return New(CodeUnauthorized, msg, http.StatusUnauthorized)
}

// NewForbidden reports that the caller is authenticated but the request
// is not allowed (HTTP 403). Currently unused at the gateway level
// (the gateway does not enforce RBAC) but defined for symmetry with
// downstream services.
func NewForbidden(msg string) *Error {
	if msg == "" {
		msg = "forbidden"
	}
	return New(CodeForbidden, msg, http.StatusForbidden)
}

// NewNotFound is returned by the router when a request matches no
// downstream route prefix (HTTP 404).
func NewNotFound(msg string) *Error {
	if msg == "" {
		msg = "not found"
	}
	return New(CodeNotFound, msg, http.StatusNotFound)
}

// NewRateLimited is returned by the rate-limit middleware when a caller
// exceeds their token-bucket budget (HTTP 429).
func NewRateLimited(msg string) *Error {
	if msg == "" {
		msg = "rate limited"
	}
	return New(CodeRateLimited, msg, http.StatusTooManyRequests)
}

// NewUpstreamError wraps a downstream failure (HTTP 502).
func NewUpstreamError(msg string, cause error) *Error {
	if msg == "" {
		msg = "upstream error"
	}
	return Wrap(CodeUpstreamError, msg, http.StatusBadGateway, cause)
}

// NewServiceUnavailable is returned when a required dependency
// (typically Redis) cannot be reached (HTTP 503).
func NewServiceUnavailable(msg string, cause error) *Error {
	if msg == "" {
		msg = "service unavailable"
	}
	return Wrap(CodeServiceUnavail, msg, http.StatusServiceUnavailable, cause)
}

// NewInternal wraps an unexpected server-side failure (HTTP 500).
func NewInternal(msg string, cause error) *Error {
	if msg == "" {
		msg = "internal server error"
	}
	return Wrap(CodeInternal, msg, http.StatusInternalServerError, cause)
}
