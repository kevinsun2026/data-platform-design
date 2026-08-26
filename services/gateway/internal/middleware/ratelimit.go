package middleware

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aidp/gateway/internal/httperr"
)

// RateLimit is the public knob: requests per second, and the
// burst capacity. The token-bucket algorithm then derives a
// concrete budget per identity. We use a single pair (RPS,
// Burst) for the entire gateway today; per-route overrides would
// belong in a future config layer.
type RateLimit struct {
	// RPS is the steady-state refill rate per identity
	// (requests per second).
	RPS int
	// Burst is the maximum bucket depth, i.e. the largest
	// burst a single identity can sustain before throttling.
	Burst int
}

// tokenBucketScript is the Lua script executed atomically inside
// Redis. Atomicity matters: a GET-then-SET pair would race with
// concurrent requests from the same identity and let a caller
// blow past their budget by sending simultaneous requests.
//
// We compute the wall clock from Redis's own TIME command rather
// than passing it from the client. This serves two purposes:
//  1. The bucket is consistent across every gateway replica
//     (no per-replica clock skew).
//  2. Tests can advance time deterministically via
//     miniredis.FastForward without sleeping.
//
// KEYS[1]: bucket key
// ARGV[1]: rate (tokens / second, integer)
// ARGV[2]: burst (bucket capacity, integer)
// ARGV[3]: cost (tokens to consume, integer)
//
// Returns {allowed, remaining}: allowed is 1 or 0; remaining is
// the token balance after the (possibly denied) request.
const tokenBucketScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
  tokens = burst
  ts = now
end

-- Refill based on elapsed wall time. The floor at 0 protects
-- against clock skew on a Redis that briefly runs backwards.
local elapsed_ms = now - ts
if elapsed_ms < 0 then elapsed_ms = 0 end
local elapsed = elapsed_ms / 1000.0
tokens = math.min(burst, tokens + elapsed * rate)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

-- TTL is sized to (burst / rate) + a 1-second grace so an
-- identity that stops calling for a while has their bucket
-- garbage-collected rather than lingering forever.
local ttl = math.ceil(burst / rate) + 1
redis.call("HMSET", key, "tokens", tokens, "ts", now)
redis.call("EXPIRE", key, ttl)
return {allowed, tokens}
`

// NewRateLimiter returns a configured middleware. The caller
// (typically main.go) injects a real go-redis client; tests
// inject a miniredis-backed client so they can run without a
// real Redis instance.
func NewRateLimiter(client redis.Cmdable, rl RateLimit) func(http.Handler) http.Handler {
	// script.Load returns a *redis.Script that uses EVALSHA
	// with EVAL fallback. We load it once at start-up so every
	// subsequent call is a single round-trip.
	script := redis.NewScript(tokenBucketScript)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Bypass for unauthenticated public paths. The
			// auth middleware runs *after* us in the chain, so
			// a request that is missing a token will still
			// reach here — we let it through and let auth
			// produce the 401. Throttling an already-failing
			// request would only mask the real problem in
			// dashboards.
			if isPublicPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			bucket, ok := bucketKey(r)
			if !ok {
				// We genuinely could not derive a key (no
				// tenant, no IP). Fail open: denying a
				// request we cannot rate-limit would
				// amount to a global outage, while
				// allowing it through costs us nothing
				// measurable on the request rate.
				next.ServeHTTP(w, r)
				return
			}

			ctx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
			defer cancel()
			res, err := script.Run(ctx, client,
				[]string{bucket},
				rl.RPS, rl.Burst, 1,
			).Result()
			if err != nil {
				// Redis down → fail open. Same reasoning as
				// above: a hard 503 here would be a single
				// point of failure for the entire API
				// surface, which is much worse than letting
				// one caller briefly exceed their budget.
				next.ServeHTTP(w, r)
				return
			}

			arr, ok := res.([]any)
			if !ok || len(arr) != 2 {
				next.ServeHTTP(w, r)
				return
			}
			allowedRaw, _ := arr[0].(int64)
			remaining, _ := arr[1].(int64)
			// Surface the standard X-RateLimit-* headers so
			// well-behaved clients can self-throttle.
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(rl.Burst))
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))

			if allowedRaw == 0 {
				retryAfter := time.Duration(float64(time.Second) / float64(rl.RPS))
				w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds()+0.999)))
				httperr.NewRateLimited("rate limit exceeded").Write(r.Context(), w)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// bucketKey derives the Redis key used for this request's
// rate-limit bucket. The key format is `rl:<tenant>:<id>` where
// `<id>` is the user subject if authenticated, or the client IP
// otherwise. The tenant dimension is intentional: a noisy user
// in tenant A must not be able to deplete tenant B's budget.
//
// Returning ok=false signals "we have no usable identity" —
// the caller is expected to fail open in that case.
func bucketKey(r *http.Request) (string, bool) {
	if id, ok := IdentityFromContext(r.Context()); ok && id.TenantID != "" {
		subject := id.Subject
		if subject == "" {
			subject = clientIP(r)
		}
		return fmt.Sprintf("rl:%s:%s", id.TenantID, subject), true
	}
	ip := clientIP(r)
	if ip == "" {
		return "", false
	}
	return "rl:_anon:" + ip, true
}

// clientIP returns the originating client address, honouring
// X-Forwarded-For when present (the typical case behind an
// ingress / load balancer). Returns "" when no address can
// be derived — the caller should fail open in that case.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the left-most entry: the original client.
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if rip := r.Header.Get("X-Real-IP"); rip != "" {
		return strings.TrimSpace(rip)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ClientIPForTest is the public alias for clientIP, exposed only
// so the external test package can exercise the XFF / X-Real-IP /
// RemoteAddr chain without poking at internals. It must not be
// used by production code.
func ClientIPForTest(r *http.Request) string { return clientIP(r) }
