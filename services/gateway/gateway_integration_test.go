package main

// Integration test: spin up a real hertz server on an ephemeral
// port, wire it through the full middleware stack, and exercise
// the trace → auth → ratelimit → router chain against a
// in-process mock downstream.
//
// The test lives in package main (not main_test) so it can call
// the internal buildRoutes / newServer directly. This is the
// "black-box-via-orchestrator" pattern: a real Hertz binary is
// the system under test, and we hit it over a real TCP socket.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aidp/gateway/internal/config"
	"github.com/aidp/gateway/internal/middleware"
)

// integrationFixture is the in-process stack we exercise.
// Each test gets its own: a fresh miniredis, a real hertz
// server, and a fake downstream. The struct holds the
// listening address of every component so individual tests
// can target them with http requests.
type integrationFixture struct {
	listenAddr string
	downstream *httptest.Server
	seen       *seenDownstream
	mr         *miniredis.Miniredis
}

type seenDownstream struct {
	method    string
	path      string
	authHdr   string
	userHdr   string
	tenantHdr string
	traceHdr  string
	body      []byte
}

// newIntegrationFixture boots the full stack on ephemeral ports
// and returns it. Cleanup is automatic via t.Cleanup.
func newIntegrationFixture(t *testing.T) *integrationFixture {
	t.Helper()
	middleware.Setup()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	seen := &seenDownstream{}
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		seen.method = r.Method
		seen.path = r.URL.Path
		seen.authHdr = r.Header.Get("Authorization")
		seen.userHdr = r.Header.Get("X-User-Id")
		seen.tenantHdr = r.Header.Get("X-Tenant-Id")
		seen.traceHdr = r.Header.Get("traceparent")
		seen.body = buf
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(down.Close)

	downURL, err := url.Parse(down.URL)
	require.NoError(t, err)

	// Pick a free port for hertz so we never collide with
	// other test runs on the same host.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	cfg := &config.Config{
		ServiceName:    "gateway-test",
		Env:            "test",
		ListenAddr:     addr,
		RateLimitRPS:   100,
		RateLimitBurst: 100,
		HealthTimeout:  2 * time.Second,
		DownstreamURLs: map[string]*url.URL{
			"iam":         downURL,
			"audit":       mustParse(t, "http://audit:80"),
			"notify":      mustParse(t, "http://notify:80"),
			"agent":       mustParse(t, "http://agent:80"),
			"datasources": mustParse(t, "http://ds:80"),
		},
	}
	hz, err := newServer(cfg, rdb)
	require.NoError(t, err)

	go func() {
		_ = hz.Run()
	}()

	// Wait for the listener to come up. The hertz server is
	// initialised asynchronously after New(), so a test that
	// fires immediately after newServer would hit a
	// connection-refused on the first probe.
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "hertz should start listening")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = hz.Shutdown(ctx)
	})

	return &integrationFixture{
		listenAddr: addr,
		downstream: down,
		seen:       seen,
		mr:         mr,
	}
}

// httpClient returns a standard http.Client with a short
// timeout, sized for the integration test.
func (f *integrationFixture) httpClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second}
}

// makeToken builds a signed-looking (but unsigned) JWT with the
// given sub/tenant claims. The gateway does not verify the
// signature, so any value works as the third segment.
func makeToken(t *testing.T, sub, tenant string) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"sub": sub, "tenant_id": tenant})
	require.NoError(t, err)
	body := base64.RawURLEncoding.EncodeToString(claims)
	return header + "." + body + ".sig"
}

func TestIntegrationHealthz(t *testing.T) {
	f := newIntegrationFixture(t)

	resp, err := f.httpClient().Get("http://" + f.listenAddr + "/healthz")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestIntegrationForwardsAuthenticatedRequest(t *testing.T) {
	f := newIntegrationFixture(t)

	req, err := http.NewRequest(http.MethodGet, "http://"+f.listenAddr+"/api/v1/iam/users/42", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+makeToken(t, "u-1", "t-1"))

	resp, err := f.httpClient().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, `{"ok":true}`, readAll(t, resp.Body))

	// Downstream saw the request with the prefix stripped and
	// identity headers populated.
	assert.Equal(t, http.MethodGet, f.seen.method)
	assert.Equal(t, "/users/42", f.seen.path)
	assert.NotEmpty(t, f.seen.authHdr)
	assert.True(t, len(f.seen.authHdr) > 7, "Bearer header must be non-trivial")
	assert.Equal(t, "u-1", f.seen.userHdr)
	assert.Equal(t, "t-1", f.seen.tenantHdr)
}

func TestIntegrationRejectsUnauthenticated(t *testing.T) {
	f := newIntegrationFixture(t)

	resp, err := f.httpClient().Get("http://" + f.listenAddr + "/api/v1/iam/users")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestIntegrationPropagatesTraceparent(t *testing.T) {
	f := newIntegrationFixture(t)

	const inboundTraceID = "0af7651916cd43dd8448eb211c80319c"
	const inboundSpanID = "b7ad6b7169203331"
	req, err := http.NewRequest(http.MethodGet, "http://"+f.listenAddr+"/api/v1/iam/x", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+makeToken(t, "u-1", "t-1"))
	req.Header.Set("traceparent", "00-"+inboundTraceID+"-"+inboundSpanID+"-01")

	resp, err := f.httpClient().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	// The response carries the resolved trace id (X-Trace-Id)
	// so the client can correlate even without parsing the body.
	assert.Equal(t, inboundTraceID, resp.Header.Get(middleware.HeaderTraceID))

	// The downstream received a traceparent header carrying the
	// inbound trace id.
	assert.NotEmpty(t, f.seen.traceHdr, "downstream must see a traceparent header")
	assert.Contains(t, f.seen.traceHdr, inboundTraceID,
		"downstream traceparent must carry the inbound trace id")
}

func TestIntegrationRateLimiterThrottlesExcess(t *testing.T) {
	f := newIntegrationFixture(t)

	// Drain the bucket by firing 100 requests in a row from
	// the same IP; the 101st should be 429.
	token := makeToken(t, "u-1", "t-1")
	hit429 := false
	for i := 0; i < 105; i++ {
		req, _ := http.NewRequest(http.MethodGet,
			"http://"+f.listenAddr+"/api/v1/iam/x", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := f.httpClient().Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			hit429 = true
			break
		}
	}
	assert.True(t, hit429, "rate limiter should eventually return 429")
}

func TestIntegrationReadyz(t *testing.T) {
	f := newIntegrationFixture(t)

	resp, err := f.httpClient().Get("http://" + f.listenAddr + "/readyz")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Kill Redis → not ready. The next request is what we
	// assert on, not the in-flight one.
	f.mr.Close()
	time.Sleep(50 * time.Millisecond)
	resp, err = f.httpClient().Get("http://" + f.listenAddr + "/readyz")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestBuildRoutesExposesAllFivePrefixes(t *testing.T) {
	cfg := &config.Config{
		DownstreamURLs: map[string]*url.URL{
			"iam":         mustParse(t, "http://iam:80"),
			"audit":       mustParse(t, "http://audit:80"),
			"notify":      mustParse(t, "http://notify:80"),
			"agent":       mustParse(t, "http://agent:80"),
			"datasources": mustParse(t, "http://ds:80"),
		},
	}
	rs, err := buildRoutes(cfg)
	require.NoError(t, err)
	require.Len(t, rs, 5)

	prefixes := make(map[string]bool, len(rs))
	for _, r := range rs {
		prefixes[r.Prefix] = true
	}
	for _, want := range []string{"iam", "audit", "notify", "agent", "datasources"} {
		assert.True(t, prefixes[want], "buildRoutes must include prefix %q", want)
	}
}

func TestBuildRoutesMissingDownstreamURL(t *testing.T) {
	cfg := &config.Config{
		DownstreamURLs: map[string]*url.URL{
			"iam": mustParse(t, "http://iam:80"),
		},
	}
	_, err := buildRoutes(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "audit")
}

// TestRunWithSignalsSuccessPath exercises the full runWithSignals
// happy path so coverage on the main package reaches the 80%
// gate. We use SIGUSR1 (the same one TestServeUntilSignalShutsDownOnSignal
// uses) and a free port for the listener so the test never
// collides with other suites on the same host.
func TestRunWithSignalsSuccessPath(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	mr := miniredis.RunT(t)
	t.Setenv("AIDP_LISTEN_ADDR", addr)
	t.Setenv("AIDP_REDIS_ADDR", mr.Addr())
	t.Setenv("AIDP_RATE_LIMIT_RPS", "100")
	t.Setenv("AIDP_RATE_LIMIT_BURST", "100")
	t.Setenv("AIDP_IAM_URL", "http://iam:80")
	t.Setenv("AIDP_AUDIT_URL", "http://audit:80")
	t.Setenv("AIDP_NOTIFY_URL", "http://notify:80")
	t.Setenv("AIDP_AGENT_URL", "http://agent:80")
	t.Setenv("AIDP_DATASOURCES_URL", "http://ds:80")

	done := make(chan error, 1)
	go func() {
		done <- runWithSignals(syscall.SIGUSR1)
	}()

	// Give the goroutine a beat to bind the listener and
	// register the signal handler.
	time.Sleep(150 * time.Millisecond)
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGUSR1))

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runWithSignals did not return after SIGUSR1")
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	// Set a rate-limit config that violates the validation
	// invariant (burst < rps). The gateway must refuse to
	// boot, not panic at first request.
	t.Setenv("AIDP_RATE_LIMIT_RPS", "100")
	t.Setenv("AIDP_RATE_LIMIT_BURST", "10")
	err := run()
	require.Error(t, err)
}

func TestServeUntilSignalShutsDownOnSignal(t *testing.T) {
	// Build a minimal hertz server, drive serveUntilSignal in
	// a goroutine, and assert that the function returns after
	// we send a signal to ourselves. SIGUSR1 is the right
	// choice: it is never sent by `go test` itself, and Go's
	// signal package lets us register a custom handler for it
	// without disturbing the test runner.
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	cfg := &config.Config{
		ListenAddr:     addr,
		RateLimitRPS:   100,
		RateLimitBurst: 100,
		HealthTimeout:  1 * time.Second,
		DownstreamURLs: map[string]*url.URL{
			"iam":         mustParse(t, "http://iam:80"),
			"audit":       mustParse(t, "http://audit:80"),
			"notify":      mustParse(t, "http://notify:80"),
			"agent":       mustParse(t, "http://agent:80"),
			"datasources": mustParse(t, "http://ds:80"),
		},
	}
	hz, err := newServer(cfg, rdb)
	require.NoError(t, err)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- serveUntilSignal(hz, addr, syscall.SIGUSR1)
	}()

	// Give the goroutine a moment to register the signal
	// handler, then send SIGUSR1 to ourselves.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGUSR1))

	select {
	case err := <-doneCh:
		// serveUntilSignal returns nil after a clean
		// shutdown; any other return is a failure.
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("serveUntilSignal did not return after SIGUSR1")
	}
}

// signalHasRun is a test-only helper kept here so a future
// refactor of the signal-handling code has an obvious place
// to drop a deterministic test. It is currently unused; the
// TestServeUntilSignalShutsDownOnSignal test exercises the
// real signal path via syscall.Kill(SIGUSR1).
func signalHasRun(c <-chan os.Signal) error {
	select {
	case sig := <-c:
		_ = sig
		return nil
	case <-time.After(100 * time.Millisecond):
		return errors.New("no signal received")
	}
}

var _ = signalHasRun

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(b)
}
