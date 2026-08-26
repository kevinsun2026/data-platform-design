package router_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aidp/gateway/internal/httperr"
	"github.com/aidp/gateway/internal/router"
	"github.com/aidp/gateway/internal/testutil"
)

// downstreamRecorder is the upstream-facing side of an
// integration test. It returns canned responses and records
// what the gateway sent so assertions can verify the
// prefix-strip + header-propagation behaviour.
type downstreamRecorder struct {
	server *httptest.Server
	seen   *seenRequest
}

type seenRequest struct {
	method    string
	path      string
	host      string
	authHdr   string
	userHdr   string
	tenantHdr string
	body      []byte
}

func newDownstream(t *testing.T, status int, body string) *downstreamRecorder {
	t.Helper()
	rec := &downstreamRecorder{seen: &seenRequest{}}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		rec.seen = &seenRequest{
			method:    r.Method,
			path:      r.URL.Path,
			host:      r.Host,
			authHdr:   r.Header.Get("Authorization"),
			userHdr:   r.Header.Get("X-User-Id"),
			tenantHdr: r.Header.Get("X-Tenant-Id"),
			body:      buf,
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

// routesFor builds a Routes value from a prefix → recorder map.
// Each recorder contributes one route, so a single test can
// cover prefix dispatch and prefix-strip in one go.
func routesFor(recs map[string]*downstreamRecorder) (router.Routes, error) {
	rs := make(router.Routes, 0, len(recs))
	for prefix, rec := range recs {
		u, err := url.Parse(rec.server.URL)
		if err != nil {
			return nil, err
		}
		rs = append(rs, router.Route{Prefix: prefix, Upstream: u})
	}
	return rs, nil
}

func TestRouterDispatchByPrefix(t *testing.T) {
	iam := newDownstream(t, http.StatusOK, `{"id":"u-1"}`)
	audit := newDownstream(t, http.StatusOK, `{"events":[]}`)

	routes, err := routesFor(map[string]*downstreamRecorder{
		"iam":   iam,
		"audit": audit,
	})
	require.NoError(t, err)
	r, err := router.New(routes)
	require.NoError(t, err)

	rec := testutil.DoRequest(r, http.MethodGet, "/api/v1/iam/users/42")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "GET", iam.seen.method)
	assert.Equal(t, "/users/42", iam.seen.path, "/api/v1/iam prefix must be stripped")
	// Host check: the request landed on the iam test server, not
	// the audit one.
	assert.Equal(t, iam.server.Listener.Addr().String(), iam.seen.host,
		"request must be sent to the iam upstream")
	assert.Equal(t, `{"id":"u-1"}`, rec.Body.String())

	// The audit upstream must not have been touched.
	assert.Empty(t, audit.seen.method)
}

func TestRouterDispatchesBarePrefixedPath(t *testing.T) {
	iam := newDownstream(t, http.StatusOK, `{}`)
	routes, err := routesFor(map[string]*downstreamRecorder{"iam": iam})
	require.NoError(t, err)
	r, err := router.New(routes)
	require.NoError(t, err)

	// /api/v1/iam (no trailing slash) must still reach iam, on
	// the upstream's root path.
	rec := testutil.DoRequest(r, http.MethodGet, "/api/v1/iam")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "/", iam.seen.path, "bare /api/v1/iam must be sent to upstream root")
}

func TestRouterReturnsNotFoundForUnknownPrefix(t *testing.T) {
	iam := newDownstream(t, http.StatusOK, `{}`)
	routes, err := routesFor(map[string]*downstreamRecorder{"iam": iam})
	require.NoError(t, err)
	r, err := router.New(routes)
	require.NoError(t, err)

	rec := testutil.DoRequest(r, http.MethodGet, "/api/v1/unknown/things")
	require.Equal(t, http.StatusNotFound, rec.Code)
	var env httperr.Envelope
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, httperr.CodeNotFound, env.Code)
	assert.Contains(t, env.Message, "no route")
}

func TestRouterReturnsNotFoundForNonAPIPath(t *testing.T) {
	routes, err := routesFor(map[string]*downstreamRecorder{"iam": newDownstream(t, http.StatusOK, `{}`)})
	require.NoError(t, err)
	r, err := router.New(routes)
	require.NoError(t, err)

	rec := testutil.DoRequest(r, http.MethodGet, "/admin")
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRouterPreservesAuthAndIdentityHeaders(t *testing.T) {
	iam := newDownstream(t, http.StatusOK, `{}`)
	routes, err := routesFor(map[string]*downstreamRecorder{"iam": iam})
	require.NoError(t, err)
	r, err := router.New(routes)
	require.NoError(t, err)

	rec := testutil.DoRequest(r, http.MethodGet, "/api/v1/iam/me",
		testutil.WithHeader("Authorization", "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdCI6InUtMSIsInQiOiJ0LTEifQ.sig"),
		testutil.WithHeader("X-User-Id", "u-1"),
		testutil.WithHeader("X-Tenant-Id", "t-1"),
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, strings.HasPrefix(iam.seen.authHdr, "Bearer "),
		"downstream must see the original Authorization header")
	assert.Equal(t, "u-1", iam.seen.userHdr)
	assert.Equal(t, "t-1", iam.seen.tenantHdr)
}

func TestRouterForwardsRequestBody(t *testing.T) {
	iam := newDownstream(t, http.StatusCreated, `{"id":"new"}`)
	routes, err := routesFor(map[string]*downstreamRecorder{"iam": iam})
	require.NoError(t, err)
	r, err := router.New(routes)
	require.NoError(t, err)

	rec := testutil.DoRequest(r, http.MethodPost, "/api/v1/iam/users",
		testutil.WithBody([]byte(`{"name":"alice"}`)),
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "POST", iam.seen.method)
	assert.Equal(t, "/users", iam.seen.path)
	assert.JSONEq(t, `{"name":"alice"}`, string(iam.seen.body))
}

func TestRouterWrapsUpstreamError(t *testing.T) {
	// Downstream that always closes the connection without
	// responding. httputil.ReverseProxy converts this into a
	// 502 (Bad Gateway) on the gateway side.
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		_ = conn.Close()
	}))
	t.Cleanup(down.Close)

	u, err := url.Parse(down.URL)
	require.NoError(t, err)
	r, err := router.New(router.Routes{{Prefix: "iam", Upstream: u}})
	require.NoError(t, err)

	rec := testutil.DoRequest(r, http.MethodGet, "/api/v1/iam/anything")
	assert.Equal(t, http.StatusBadGateway, rec.Code)
}

func TestNewValidatesRoutes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		_, err := router.New(nil)
		require.Error(t, err)
	})
	t.Run("empty prefix", func(t *testing.T) {
		_, err := router.New(router.Routes{{Prefix: "", Upstream: mustURL(t, "http://x")}})
		require.Error(t, err)
	})
	t.Run("nil upstream", func(t *testing.T) {
		_, err := router.New(router.Routes{{Prefix: "iam", Upstream: nil}})
		require.Error(t, err)
	})
	t.Run("duplicate prefix", func(t *testing.T) {
		u := mustURL(t, "http://x")
		_, err := router.New(router.Routes{
			{Prefix: "iam", Upstream: u},
			{Prefix: "iam", Upstream: u},
		})
		require.Error(t, err)
	})
}

func TestHealthzHandler(t *testing.T) {
	h := router.Healthz()
	rec := testutil.DoRequest(h, http.MethodGet, "/healthz")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ok")
}

func TestReadyzHandler(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		h := router.NewReadyz(func() error { return nil })
		rec := testutil.DoRequest(h, http.MethodGet, "/readyz")
		require.Equal(t, http.StatusOK, rec.Code)
	})
	t.Run("not ready", func(t *testing.T) {
		h := router.NewReadyz(func() error { return errSentinel("redis down") })
		rec := testutil.DoRequest(h, http.MethodGet, "/readyz")
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Contains(t, rec.Body.String(), "redis down")
	})
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
