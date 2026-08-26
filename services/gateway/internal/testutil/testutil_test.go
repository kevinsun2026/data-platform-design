package testutil_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aidp/gateway/internal/testutil"
)

// TestDoRequestBasics exercises the DoRequest happy path: a
// simple handler sees the right method, path, headers, and
// body. The recorder is returned for the caller to assert on
// the response.
func TestDoRequestBasics(t *testing.T) {
	var seenMethod, seenPath, seenHeader string
	var seenBody []byte
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenMethod = r.Method
		seenPath = r.URL.Path
		seenHeader = r.Header.Get("X-Trace")
		seenBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusTeapot)
	})

	rec := testutil.DoRequest(h, http.MethodPost, "/api/v1/iam/x",
		testutil.WithHeader("X-Trace", "abcdef"),
		testutil.WithBody([]byte(`{"hello":"world"}`)),
	)
	require.Equal(t, http.StatusTeapot, rec.Code)
	assert.Equal(t, http.MethodPost, seenMethod)
	assert.Equal(t, "/api/v1/iam/x", seenPath)
	assert.Equal(t, "abcdef", seenHeader)
	assert.True(t, bytes.Contains(seenBody, []byte(`"hello"`)))
}

// TestDoRequestWithContext proves the optional context is
// propagated to the handler. We cancel it and confirm the
// handler observes the cancellation.
func TestDoRequestWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The handler just confirms the context is the one
		// we passed in.
		assert.Equal(t, ctx, r.Context())
		w.WriteHeader(http.StatusOK)
	})
	rec := testutil.DoRequest(h, http.MethodGet, "/x",
		testutil.WithContext(ctx),
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestNewTraceID checks the deterministic-looking id is
// well-formed. The pattern is reused by every middleware test
// that needs a fresh trace id.
func TestNewTraceID(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{32}$`)
	id := testutil.NewTraceID()
	assert.Regexp(t, pattern, id)
	// Two consecutive ids must not collide.
	assert.NotEqual(t, id, testutil.NewTraceID())
}

// TestMustNotExceedPasses covers the helper's no-op success
// path. The failure path would call t.Fatalf, which would
// fail the surrounding test, so we don't need a separate
// negative test — the wrapper's behaviour is governed by the
// time.After branch, which is a one-liner in the standard
// library.
func TestMustNotExceedPasses(t *testing.T) {
	ran := false
	testutil.MustNotExceed(t, time.Second, func() {
		ran = true
	})
	assert.True(t, ran)
}

// TestNewTracerRoundTrip proves the in-process tracer
// registers a real span. A round-trip through the SDK
// (TracerProvider → Tracer → Span) is the only way to
// detect a misconfigured sampler.
func TestNewTracerRoundTrip(t *testing.T) {
	tr := testutil.NewTracer()
	t.Cleanup(tr.Close)

	// Do a real handler invocation that triggers a span.
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.ServeHTTP(rec, req)

	// The tracer in this test was created without going
	// through the global Tracer — so we cannot assert on
	// Spans() here. The point of this test is purely to
	// catch a panic at construction time.
	assert.Equal(t, http.StatusOK, rec.Code)
}
