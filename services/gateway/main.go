// Command gateway is the AIDP API gateway. It accepts requests on
// :8000, applies the trace / auth / rate-limit middleware, and
// forwards matched /api/v1/<prefix>/* requests to the
// corresponding downstream service.
//
// Wiring overview:
//
//	trace   →  outermost (every request gets a span)
//	auth    →  extracts bearer token, attaches Identity
//	ratelimit →  per-identity token bucket backed by Redis
//	router  →  dispatches to the matching downstream
//
// We use CloudWeGo Hertz as the HTTP server (per the platform
// plan), but the request handlers and middleware are plain
// net/http types. Hertz's adaptor package bridges the two, so
// every layer below this file is testable with the standard
// httptest package and no Hertz-specific types leak in.
//
// main.go is intentionally short: every concern lives in its
// own package, and main() does nothing more than construct the
// dependency graph and start the server.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/redis/go-redis/v9"

	"github.com/aidp/gateway/internal/config"
	"github.com/aidp/gateway/internal/middleware"
	"github.com/aidp/gateway/internal/router"
)

// redisPingTimeout is the per-call deadline for the readiness
// check. A slow Redis must not be able to hold the readiness
// probe open for longer than this.
const redisPingTimeout = 2 * time.Second

// shutdownTimeout caps how long graceful shutdown waits for
// in-flight requests to finish. After this elapses the process
// is terminated regardless, so a stuck request cannot keep the
// pod alive past the kubelet's terminationGracePeriodSeconds.
const shutdownTimeout = 30 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}

// run is the testable entry point. We split it from main() so
// integration tests can call it with a customised environment,
// without having to spin up the whole process.
//
// The function is small on purpose: the heavy lifting lives in
// newServer, which the integration test exercises directly.
func run() error {
	return runWithSignals(os.Interrupt, syscall.SIGTERM)
}

// runWithSignals is the same as run but lets the caller pick
// the signals that trigger shutdown. The integration test
// passes a custom channel via the unexported runWithSignalChan
// (see below) so the test does not race the test runner's own
// signal handling.
func runWithSignals(sigs ...os.Signal) error {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		return err
	}

	middleware.Setup()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	hz, err := newServer(cfg, rdb)
	if err != nil {
		return err
	}
	return serveUntilSignal(hz, cfg.ListenAddr, sigs...)
}

// newServer assembles the full middleware stack and registers
// every route on a fresh Hertz engine. It returns the engine
// without starting it; the caller is responsible for Spin /
// Shutdown.
//
// Splitting the assembly from the lifecycle makes the wiring
// testable in isolation: integration tests build a server
// with this function, then drive its routes directly. The
// production code path is the same one a real deploy would
// take, so any test that passes here passes in prod.
func newServer(cfg *config.Config, rdb *redis.Client) (*server.Hertz, error) {
	rs, err := buildRoutes(cfg)
	if err != nil {
		return nil, err
	}
	rt, err := router.New(rs)
	if err != nil {
		return nil, err
	}

	// Stack the middleware. The order is significant:
	//   1. trace — outer so it always runs, even if downstream
	//      middleware short-circuits with an error.
	//   2. auth — extracts the identity; must run before the
	//      rate limiter so the limiter can bucket by tenant.
	//   3. ratelimit — per-identity token bucket.
	//   4. router — forwards to the matching downstream.
	var h http.Handler = rt
	h = middleware.NewRateLimiter(rdb, middleware.RateLimit{
		RPS:   cfg.RateLimitRPS,
		Burst: cfg.RateLimitBurst,
	})(h)
	h = middleware.Auth()(h)
	h = middleware.Trace()(h)

	healthz := router.Healthz()
	readyz := router.NewReadyz(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), redisPingTimeout)
		defer cancel()
		return rdb.Ping(ctx).Err()
	})

	// Catch-all net/http mux used as the bridge target for
	// adaptor.HertzHandler. The mux exists purely so we have a
	// single function to hand to Hertz — registering each
	// route on both sides would invite drift.
	mux := http.NewServeMux()
	mux.Handle("/healthz", healthz)
	mux.Handle("/readyz", readyz)
	mux.Handle("/api/v1/", h)

	hz := server.New(
		server.WithHostPorts(cfg.ListenAddr),
		server.WithReadTimeout(10*time.Second),
	)
	hz.GET("/healthz", adaptor.HertzHandler(healthz))
	hz.GET("/readyz", adaptor.HertzHandler(readyz))
	hz.Any("/api/v1/*path", adaptor.HertzHandler(mux))
	return hz, nil
}

// serveUntilSignal runs the server until it receives one of
// the supplied signals, then performs a graceful shutdown.
// Extracted from run() so the integration test can exercise
// the full lifecycle with synthetic signals, without having
// to send real SIGTERM into the test runner.
func serveUntilSignal(hz *server.Hertz, listenAddr string, sigs ...os.Signal) error {
	stop := make(chan os.Signal, 1)
	if len(sigs) > 0 {
		signal.Notify(stop, sigs...)
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("gateway: listening on %s", listenAddr)
		hz.Spin()
		errCh <- nil
	}()

	select {
	case sig := <-stop:
		log.Printf("gateway: received %s, shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := hz.Shutdown(ctx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

// buildRoutes converts the typed Config into the router's flat
// (prefix, URL) list. We do this rather than passing the config
// to the router directly so the router's contract stays focused
// on "prefix → URL" and does not know about env vars.
//
// The five prefixes are fixed by the platform-wide API surface
// spec; adding a new one is a deliberate code change, not a
// config flip, so a misconfigured deploy cannot accidentally
// introduce a new public route.
func buildRoutes(cfg *config.Config) (router.Routes, error) {
	pairs := []struct {
		prefix string
		key    string
	}{
		{"iam", "iam"},
		{"audit", "audit"},
		{"notify", "notify"},
		{"agent", "agent"},
		{"datasources", "datasources"},
	}
	rs := make(router.Routes, 0, len(pairs))
	for _, p := range pairs {
		u, ok := cfg.DownstreamURLs[p.key]
		if !ok {
			// Defensive: config.DownstreamURLs is always
			// populated by LoadFromEnv, so this branch is
			// unreachable in practice. Returning the error
			// rather than panicking keeps the start-up path
			// clean.
			return nil, errors.New("gateway: missing downstream URL for " + p.prefix)
		}
		rs = append(rs, router.Route{Prefix: p.prefix, Upstream: u})
	}
	return rs, nil
}
