package httperr_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/aidp/gateway/internal/httperr"
)

// TestEnvelopeWireFormat pins the JSON shape clients depend on. The
// shape is part of the public API contract — a missing field here
// would silently break every consumer that decodes the error body.
func TestEnvelopeWireFormat(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	httperr.NewNotFound("user u-123 not found").Write(context.Background(), rec)

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)

	var env httperr.Envelope
	require.NoError(t, json.Unmarshal(body, &env))

	assert.Equal(t, httperr.CodeNotFound, env.Code)
	assert.Equal(t, "user u-123 not found", env.Message)
	// No OTel span in the background context → trace_id is the empty
	// string and the json tag is omitempty, so the field is dropped.
	_, hasTrace := bodyContains(body, "trace_id")
	assert.False(t, hasTrace, "trace_id must be omitted when no span is attached")
}

// TestWriteAttachesTraceIDFromSpan verifies the trace middleware
// contract: an active OTel span's trace id must appear in the wire
// envelope so operators can correlate client reports with traces.
func TestWriteAttachesTraceIDFromSpan(t *testing.T) {
	t.Parallel()

	const expectedTraceID = "0af7651916cd43dd8448eb211c80319c"
	traceID, err := trace.TraceIDFromHex(expectedTraceID)
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("b7ad6b7169203331")
	require.NoError(t, err)
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	rec := httptest.NewRecorder()
	httperr.NewRateLimited("slow down").Write(ctx, rec)

	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	var env httperr.Envelope
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, httperr.CodeRateLimited, env.Code)
	assert.Equal(t, "slow down", env.Message)
	assert.Equal(t, expectedTraceID, env.TraceID)
}

// TestErrorImplementsErrorAndUnwrap covers the standard error
// interface contract so middleware can use errors.Is / errors.As to
// classify failures raised deep in the call stack.
func TestErrorImplementsErrorAndUnwrap(t *testing.T) {
	t.Parallel()

	cause := errors.New("redis: connection refused")
	e := httperr.NewUpstreamError("iam-service unreachable", cause)

	assert.True(t, errors.Is(e, cause), "errors.Is must walk the Unwrap chain")
	assert.Contains(t, e.Error(), "UPSTREAM_ERROR")
	assert.Contains(t, e.Error(), "iam-service unreachable")
	assert.Contains(t, e.Error(), "connection refused")
}

// TestConstructorsUseCanonicalStatuses guarantees the HTTP status
// mapping is stable — a status-code regression here would be a
// breaking change for clients that switch on the HTTP layer.
func TestConstructorsUseCanonicalStatuses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		err    *httperr.Error
		status int
		code   httperr.ErrorCode
	}{
		{"unauthorized", httperr.NewUnauthorized(""), http.StatusUnauthorized, httperr.CodeUnauthorized},
		{"forbidden", httperr.NewForbidden(""), http.StatusForbidden, httperr.CodeForbidden},
		{"not_found", httperr.NewNotFound(""), http.StatusNotFound, httperr.CodeNotFound},
		{"rate_limited", httperr.NewRateLimited(""), http.StatusTooManyRequests, httperr.CodeRateLimited},
		{"upstream", httperr.NewUpstreamError("", nil), http.StatusBadGateway, httperr.CodeUpstreamError},
		{"unavailable", httperr.NewServiceUnavailable("", nil), http.StatusServiceUnavailable, httperr.CodeServiceUnavail},
		{"internal", httperr.NewInternal("", nil), http.StatusInternalServerError, httperr.CodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.status, tc.err.Status)
			assert.Equal(t, tc.code, tc.err.Code)
			assert.NotEmpty(t, tc.err.Message, "constructor must supply a default message")
		})
	}
}

// bodyContains reports whether needle appears in body. It is a tiny
// helper rather than a json.Unmarshal because we want to assert the
// raw bytes — proving trace_id is truly absent, not present-but-empty.
func bodyContains(body []byte, needle string) (string, bool) {
	s := string(body)
	for i := 0; i+len(needle) <= len(s); i++ {
		if s[i:i+len(needle)] == needle {
			return needle, true
		}
	}
	return "", false
}
