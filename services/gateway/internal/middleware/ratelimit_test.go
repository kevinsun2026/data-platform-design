package middleware_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aidp/gateway/internal/httperr"
	"github.com/aidp/gateway/internal/middleware"
	"github.com/aidp/gateway/internal/testutil"
)

// newRedisPair spins up an in-process miniredis server and a
// real go-redis client pointed at it. miniredis implements
// enough of the wire protocol (including EVAL / EVALSHA for
// our token-bucket script) to exercise the full middleware
// without any external infrastructure.
//
// Cleanup is automatic via t.Cleanup; tests do not need to
// call mr.Close() themselves.
func newRedisPair(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// TestRateLimitAllowsBurst covers the steady-state happy path:
// the first N requests within the bucket size all pass; the
// (N+1)th is throttled.
func TestRateLimitAllowsBurst(t *testing.T) {
	_, client := newRedisPair(t)
	rl := middleware.RateLimit{RPS: 1, Burst: 3}

	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.NewRateLimiter(client, rl)(terminal)

	// Bucket key is built from the X-Forwarded-For IP; we fix
	// it so all five calls land in the same bucket.
	for i := 0; i < 3; i++ {
		rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
			testutil.WithHeader("X-Forwarded-For", "10.0.0.1"),
		)
		assert.Equal(t, http.StatusOK, rec.Code, "request %d should pass", i+1)
	}
	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.1"),
	)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	var env httperr.Envelope
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, httperr.CodeRateLimited, env.Code)
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))
}

// TestRateLimitRefillsOverTime documents the token-bucket
// behaviour: once the bucket is empty, the limiter waits
// for tokens to refill rather than failing every request.
//
// We test this by deleting the bucket key (simulating the TTL
// firing + a brand-new identity returning) and verifying the
// next call passes. The refill math itself is covered by
// TestRateLimitScriptRefill: this test only needs to prove the
// middleware wires the script up to the request path.
func TestRateLimitRefillsOverTime(t *testing.T) {
	_, client := newRedisPair(t)
	rl := middleware.RateLimit{RPS: 1, Burst: 1}

	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.NewRateLimiter(client, rl)(terminal)

	// Drain the bucket.
	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.2"),
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.2"),
	)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	// Delete the bucket key to simulate TTL expiry. The next
	// request will see a fresh bucket and pass.
	require.NoError(t, client.Del(context.Background(), "rl:_anon:10.0.0.2").Err())

	rec = testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.2"),
	)
	assert.Equal(t, http.StatusOK, rec.Code, "after bucket reset, request should pass")
}

// TestRateLimitBucketsAreIndependentPerIP ensures the bucket
// key is correct: one noisy IP must not be able to drain
// another IP's budget. The simplest regression test for a
// forgotten X-Forwarded-For.
func TestRateLimitBucketsAreIndependentPerIP(t *testing.T) {
	_, client := newRedisPair(t)
	rl := middleware.RateLimit{RPS: 1, Burst: 1}

	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.NewRateLimiter(client, rl)(terminal)

	// IP A drains its bucket.
	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.3"),
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.3"),
	)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	// IP B starts with a fresh bucket.
	rec = testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.4"),
	)
	assert.Equal(t, http.StatusOK, rec.Code, "different IP must have its own bucket")
}

// TestRateLimitExposesRemainingHeader pins the X-RateLimit-*
// contract: a well-behaved client uses these headers to
// self-throttle without ever hitting a 429. Breaking the
// header would force every SDK to count requests locally,
// which is brittle.
func TestRateLimitExposesRemainingHeader(t *testing.T) {
	_, client := newRedisPair(t)
	rl := middleware.RateLimit{RPS: 5, Burst: 5}

	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.NewRateLimiter(client, rl)(terminal)

	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.5"),
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "5", rec.Header().Get("X-RateLimit-Limit"))
	remaining, err := strconv.Atoi(rec.Header().Get("X-RateLimit-Remaining"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, remaining, 0)
	assert.LessOrEqual(t, remaining, 5)
}

// TestRateLimitBypassForPublicPaths ensures the rate limiter
// does not run on health / login endpoints. A regression
// that throttles /healthz would knock the pod out of the
// load-balancer rotation — a self-inflicted outage.
func TestRateLimitBypassForPublicPaths(t *testing.T) {
	_, client := newRedisPair(t)
	// Burst of 1: a single non-bypass request would be the
	// only one allowed, so a regression that mistakenly
	// throttles the public path would fail this test
	// immediately.
	rl := middleware.RateLimit{RPS: 1, Burst: 1}

	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.NewRateLimiter(client, rl)(terminal)

	for i := 0; i < 5; i++ {
		rec := testutil.DoRequest(h, http.MethodGet, "/healthz")
		assert.Equal(t, http.StatusOK, rec.Code, "healthz must never be throttled")
	}
}

// TestRateLimitFailsOpenOnRedisError documents the deliberate
// fail-open behaviour: if Redis is unreachable, the gateway
// must not become a single point of failure for the whole API.
// This is a tradeoff: one caller can briefly exceed budget.
// That is strictly better than a 503 storm that takes down
// every API.
func TestRateLimitFailsOpenOnRedisError(t *testing.T) {
	mr, client := newRedisPair(t)
	rl := middleware.RateLimit{RPS: 1, Burst: 1}

	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := middleware.NewRateLimiter(client, rl)(terminal)

	// Drain the bucket so we know a working Redis would 429.
	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.6"),
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.6"),
	)
	require.Equal(t, http.StatusTooManyRequests, rec.Code, "sanity: limiter works when Redis is up")

	// Now kill Redis. Subsequent requests must pass through.
	mr.Close()

	rec = testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("X-Forwarded-For", "10.0.0.6"),
	)
	assert.Equal(t, http.StatusOK, rec.Code, "rate limiter must fail open when Redis is down")
}

// TestClientIP covers the X-Forwarded-For / X-Real-IP /
// RemoteAddr fallback chain. Getting this wrong would either
// rate-limit every request against the LB's IP (collapsing
// all buckets into one) or fail to rate-limit at all.
func TestClientIP(t *testing.T) {
	t.Run("uses left-most XFF entry", func(t *testing.T) {
		req := httpRequestWithAddr("10.0.0.1:1234",
			"X-Forwarded-For", "203.0.113.1, 10.0.0.2, 10.0.0.3",
		)
		assert.Equal(t, "203.0.113.1", clientIPForTest(req))
	})
	t.Run("single XFF entry", func(t *testing.T) {
		req := httpRequestWithAddr("10.0.0.1:1234",
			"X-Forwarded-For", "203.0.113.1",
		)
		assert.Equal(t, "203.0.113.1", clientIPForTest(req))
	})
	t.Run("falls back to X-Real-IP", func(t *testing.T) {
		req := httpRequestWithAddr("10.0.0.1:1234",
			"X-Real-IP", "203.0.113.9",
		)
		assert.Equal(t, "203.0.113.9", clientIPForTest(req))
	})
	t.Run("falls back to RemoteAddr host", func(t *testing.T) {
		req := httpRequestWithAddr("203.0.113.42:55555")
		assert.Equal(t, "203.0.113.42", clientIPForTest(req))
	})
}

// httpRequestWithAddr builds a minimal *http.Request with a
// fixed RemoteAddr and optional headers. Used by the clientIP
// test cases to keep each table entry to one line.
func httpRequestWithAddr(remoteAddr string, headerKV ...string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/iam/users", nil)
	req.RemoteAddr = remoteAddr
	if len(headerKV)%2 != 0 {
		panic("httpRequestWithAddr: headerKV must be key/value pairs")
	}
	for i := 0; i < len(headerKV); i += 2 {
		req.Header.Set(headerKV[i], headerKV[i+1])
	}
	return req
}

// clientIPForTest wraps the unexported clientIP() for tests in
// the external test package. The indirection keeps the helper
// from leaking into the production code path.
func clientIPForTest(r *http.Request) string {
	return middleware.ClientIPForTest(r)
}
