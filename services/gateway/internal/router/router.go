// Package router wires the gateway's reverse-proxy dispatch to
// the per-downstream handlers. It is intentionally thin — the
// real complexity lives in the middleware and in
// httputil.ReverseProxy. This package's job is to:
//
//  1. Match an incoming request to a downstream route prefix.
//  2. Strip the /api/v1/<prefix>/… portion so the upstream sees
//     the path it actually serves.
//  3. Forward the request, propagating trace context and the
//     identity headers the auth middleware injected.
//
// The router is configured with a map keyed by the path-prefix
// tail ("iam", "audit", …); the public canonical URL paths are
// hard-coded so an operator who changes the path layout has to
// touch the code, not the config, to keep them in sync.
package router

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aidp/gateway/internal/httperr"
)

// Route is a single (path-prefix → upstream) entry. The prefix
// is the segment immediately under /api/v1/, e.g. "iam" matches
// /api/v1/iam/* and /api/v1/iam.
type Route struct {
	// Prefix is the path segment under /api/v1/ (e.g. "iam").
	Prefix string
	// Upstream is the base URL of the downstream service. The
	// router appends the path-after-prefix to this URL, so the
	// upstream's own route layout is preserved end-to-end.
	Upstream *url.URL
}

// Routes is the ordered list of routes the router will try in
// turn. Order matters only for the disambiguation log: requests
// always match a single (longest prefix wins) entry.
type Routes []Route

// Router is the HTTP handler the gateway installs for /api/v1/*
// requests. The zero value is invalid; use New to build one.
type Router struct {
	routes  []Route
	proxies map[string]*httputil.ReverseProxy
	mu      sync.RWMutex
}

// New constructs a Router with one ReverseProxy per route. The
// proxies are pre-built (one per upstream) so the per-request
// cost is a single map lookup + the proxy's own hop, not a URL
// parse or a new struct allocation.
func New(routes Routes) (*Router, error) {
	if len(routes) == 0 {
		return nil, errEmptyRoutes
	}
	proxies := make(map[string]*httputil.ReverseProxy, len(routes))
	for _, r := range routes {
		if r.Prefix == "" {
			return nil, errEmptyPrefix
		}
		if r.Upstream == nil {
			return nil, errNilUpstream
		}
		if _, dup := proxies[r.Prefix]; dup {
			return nil, errDuplicateRoute
		}
		proxies[r.Prefix] = newReverseProxy(r.Upstream)
	}
	// Defensive copy + sort so the public Routes slice is
	// never mutated by the router.
	cp := make([]Route, len(routes))
	copy(cp, routes)
	sort.SliceStable(cp, func(i, j int) bool {
		return cp[i].Prefix < cp[j].Prefix
	})
	return &Router{routes: cp, proxies: proxies}, nil
}

// newReverseProxy builds the *httputil.ReverseProxy the router
// uses for a single upstream. We construct it here (rather than
// inline in ServeHTTP) so the Director closure is allocated
// once per upstream rather than per request — httputil's own
// Director is allowed to be cheap, but the cost of one map
// lookup per request is what we are optimising for at the
// gateway's request volume.
//
// The Director:
//  1. Rewrites the request URL to the upstream's scheme / host.
//  2. Strips the /api/v1/<prefix>/ prefix and replaces it with
//     the upstream's own path (or root if the upstream has
//     none).
//  3. Preserves the original Host header as X-Forwarded-Host
//     so upstreams can produce correct Location headers on
//     redirects. The actual Host header is set to the
//     upstream's host so HTTP/1.1 virtual hosting works.
func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			orig := r.In.URL.Path
			stripped := stripAPIPrefix(orig)
			r.Out.URL.Scheme = target.Scheme
			r.Out.URL.Host = target.Host
			r.Out.Host = target.Host
			// If the upstream has a base path (e.g. /iam),
			// preserve it; otherwise root the path there.
			base := strings.TrimRight(target.Path, "/")
			r.Out.URL.Path = base + stripped
			if base == "" && stripped == "" {
				r.Out.URL.Path = "/"
			}
			r.Out.Header.Set("X-Forwarded-Host", orig)
		},
		// Limit how long we wait for an upstream. A stuck
		// downstream must not be able to hold a gateway
		// connection open indefinitely — the LB will recycle
		// us if we don't respond within its own timeout, but
		// a tighter bound here keeps resource use bounded.
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       60 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			httperr.NewUpstreamError("upstream request failed", err).Write(r.Context(), w)
		},
	}
	return proxy
}

// stripAPIPrefix removes the /api/v1/<prefix> portion of the
// path. It is exported (lowercase) for the test package to
// assert on; production callers go through ServeHTTP.
func stripAPIPrefix(p string) string {
	// Fast path: no /api/v1/ prefix → nothing to do.
	const apiV1 = "/api/v1/"
	if !strings.HasPrefix(p, apiV1) {
		return p
	}
	rest := p[len(apiV1):]
	// rest is now "<prefix>/..." or "<prefix>".
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[i:]
	}
	// Bare /api/v1/<prefix> with no trailing path. Send to
	// the upstream's root.
	return "/"
}

// ServeHTTP implements http.Handler. It looks up the route by
// the first segment after /api/v1/ and dispatches to the
// corresponding ReverseProxy. Unmatched paths produce a 404 in
// the canonical error envelope — a request that names a
// well-formed prefix the router has no route for is a 404 from
// the client's perspective, not a 500.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	prefix, ok := matchPrefix(req.URL.Path)
	if !ok {
		httperr.NewNotFound("no route matches "+req.URL.Path).Write(req.Context(), w)
		return
	}
	r.mu.RLock()
	proxy := r.proxies[prefix]
	r.mu.RUnlock()
	if proxy == nil {
		httperr.NewNotFound("no route registered for prefix "+prefix).Write(req.Context(), w)
		return
	}
	proxy.ServeHTTP(w, req)
}

// matchPrefix extracts the route prefix from a /api/v1/<x>/...
// path. Returns ok=false for paths that don't begin with
// /api/v1/. The returned prefix is the first segment after
// /api/v1/ — the router does not care about the *content* of
// the prefix, only that it can find a matching proxy.
func matchPrefix(p string) (string, bool) {
	const apiV1 = "/api/v1/"
	if !strings.HasPrefix(p, apiV1) {
		return "", false
	}
	rest := p[len(apiV1):]
	if rest == "" {
		return "", false
	}
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		rest = rest[:i]
	}
	if rest == "" {
		return "", false
	}
	return rest, true
}

// Healthz is the liveness handler. It returns 200 unconditionally
// when the process is up; the kubelet only needs to know the
// gateway is alive, not whether its dependencies are. Use
// Readyz for dependency-aware readiness.
func Healthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
}

// NewReadyz returns a readiness handler that pings Redis
// (when configured) and returns 503 if the dependency is
// unreachable. The check is intentionally cheap — readiness
// probes run every few seconds and a slow check would thrash
// the pod in and out of the LB rotation.
func NewReadyz(ping func() error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := ping(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready","reason":"` + err.Error() + `"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
}

// Errors raised by New. Kept unexported because the only
// consumer is the gateway's main(), and exposing the literal
// strings as exported vars would be a wider API than the
// function itself.
var (
	errEmptyRoutes    = routerError("router: at least one route is required")
	errEmptyPrefix    = routerError("router: route prefix must not be empty")
	errNilUpstream    = routerError("router: route upstream must not be nil")
	errDuplicateRoute = routerError("router: duplicate route prefix")
)

type routerError string

func (e routerError) Error() string { return string(e) }
