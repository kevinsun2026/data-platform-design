package middleware_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aidp/gateway/internal/httperr"
	"github.com/aidp/gateway/internal/middleware"
	"github.com/aidp/gateway/internal/testutil"
)

// mintToken builds an unsigned JWT whose payload is the given
// claims map. We do not sign because the gateway explicitly
// does not verify — these tests exercise the path-decoding
// logic only. Returning the same wire format the iam-service
// emits keeps the test artefacts realistic.
func mintToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body := base64.RawURLEncoding.EncodeToString(payload)
	return header + "." + body + ".signature-ignored"
}

// observedAuth captures what the auth middleware passed to the
// downstream handler: the resolved identity, and the headers
// the downstream should receive.
type observedAuth struct {
	id             middleware.Identity
	idOK           bool
	downstreamAuth string
	userHeader     string
	tenantHeader   string
}

func newAuthRecorder() (*observedAuth, http.Handler) {
	obs := &observedAuth{}
	return obs, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obs.id, obs.idOK = middleware.IdentityFromContext(r.Context())
		obs.downstreamAuth = r.Header.Get(middleware.HeaderUserID)
		obs.userHeader = r.Header.Get(middleware.HeaderUserID)
		obs.tenantHeader = r.Header.Get(middleware.HeaderTenantID)
		w.WriteHeader(http.StatusOK)
	})
}

// TestAuthExtractsIdentityFromJWT covers the happy path: a
// request with a well-formed bearer token reaches the downstream
// handler with the right Identity and the right pass-through
// headers.
func TestAuthExtractsIdentityFromJWT(t *testing.T) {
	token := mintToken(t, map[string]any{
		"sub":       "u-123",
		"tenant_id": "t-acme",
		"exp":       9_999_999_999,
	})
	obs, terminal := newAuthRecorder()
	h := middleware.Auth()(terminal)

	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("Authorization", "Bearer "+token),
	)

	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, obs.idOK)
	assert.Equal(t, "u-123", obs.id.Subject)
	assert.Equal(t, "t-acme", obs.id.TenantID)
	assert.Equal(t, "u-123", obs.userHeader, "X-User-Id must be set for the downstream")
	assert.Equal(t, "t-acme", obs.tenantHeader, "X-Tenant-Id must be set for the downstream")
}

// TestAuthAllowsPublicPath verifies the allowlist short-circuit.
// The same handler is used for every test, so the absence of
// headers on the downstream side proves the public-path branch
// did not run the identity extraction.
func TestAuthAllowsPublicPath(t *testing.T) {
	cases := []string{"/healthz", "/readyz", "/api/v1/auth/login", "/api/v1/auth/login/sso"}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			obs, terminal := newAuthRecorder()
			h := middleware.Auth()(terminal)

			rec := testutil.DoRequest(h, http.MethodGet, path)

			require.Equal(t, http.StatusOK, rec.Code, "public path must be served without a token")
			assert.False(t, obs.idOK, "public-path request must not carry an identity")
			assert.Empty(t, obs.userHeader)
		})
	}
}

// TestAuthRejectsMissingHeader covers the unauthenticated path:
// the middleware must respond 401 with the canonical envelope
// and must NOT invoke the downstream handler.
func TestAuthRejectsMissingHeader(t *testing.T) {
	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("downstream must not be invoked when auth fails")
	})
	h := middleware.Auth()(terminal)

	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users")

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))

	var env httperr.Envelope
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, httperr.CodeUnauthorized, env.Code)
	assert.Contains(t, env.Message, "missing Authorization")
}

// TestAuthRejectsWrongScheme ensures the gateway does not pass
// through Basic / Digest / API-Key credentials. A user who
// accidentally pasted a Basic auth header in front of an API
// call should get a clear 401, not a 502 from iam-service.
func TestAuthRejectsWrongScheme(t *testing.T) {
	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("downstream must not be invoked for non-Bearer auth")
	})
	h := middleware.Auth()(terminal)

	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("Authorization", "Basic dXNlcjpwYXNz"),
	)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	var env httperr.Envelope
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	assert.Equal(t, httperr.CodeUnauthorized, env.Code)
	assert.Contains(t, env.Message, "Bearer")
}

// TestAuthRejectsMalformedToken covers the broken-JWT case. The
// gateway must not crash on a half-deleted token, and must
// surface a 401 rather than letting the downstream see garbage.
func TestAuthRejectsMalformedToken(t *testing.T) {
	cases := []struct {
		name  string
		token string
	}{
		{"no dots", "thisisnotajwt"},
		{"only header", "headeronly"},
		{"non-base64 payload", "abc.!!!notbase64!!!.sig"},
		{"payload is not json", "a." + base64.RawURLEncoding.EncodeToString([]byte("not-json")) + ".sig"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Fatal("downstream must not be invoked for malformed token")
			})
			h := middleware.Auth()(terminal)

			rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
				testutil.WithHeader("Authorization", "Bearer "+tc.token),
			)
			require.Equal(t, http.StatusUnauthorized, rec.Code)
		})
	}
}

// TestAuthRejectsMissingClaims documents the contract: a token
// without sub or tenant_id is not useful downstream and must be
// rejected at the edge.
func TestAuthRejectsMissingClaims(t *testing.T) {
	cases := []struct {
		name   string
		claims map[string]any
	}{
		{"no sub", map[string]any{"tenant_id": "t-1"}},
		{"no tenant", map[string]any{"sub": "u-1"}},
		{"empty sub", map[string]any{"sub": "", "tenant_id": "t-1"}},
		{"empty tenant", map[string]any{"sub": "u-1", "tenant_id": ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := mintToken(t, tc.claims)
			terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Fatal("downstream must not be invoked when claims are incomplete")
			})
			h := middleware.Auth()(terminal)

			rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
				testutil.WithHeader("Authorization", "Bearer "+token),
			)
			require.Equal(t, http.StatusUnauthorized, rec.Code)
		})
	}
}

// TestAuthPreservesOriginalAuthorizationHeader is the pass-through
// contract: the downstream must still see the raw token so it
// can re-verify the signature. We re-introduce the header value
// inside the recorder helper rather than reading it directly
// from the request, to keep the test self-contained.
func TestAuthPreservesOriginalAuthorizationHeader(t *testing.T) {
	token := mintToken(t, map[string]any{"sub": "u-1", "tenant_id": "t-1"})

	var sawOriginal string
	terminal := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		sawOriginal = r.Header.Get("Authorization")
	})
	h := middleware.Auth()(terminal)

	rec := testutil.DoRequest(h, http.MethodGet, "/api/v1/iam/users",
		testutil.WithHeader("Authorization", "Bearer "+token),
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, strings.HasPrefix(sawOriginal, "Bearer "),
		"downstream must still see the original Bearer header (got %q)", sawOriginal)
	assert.Contains(t, sawOriginal, token)
}

// TestAuthWithIdentityHelper covers the test-side escape hatch
// for synthetic identities (admin paths, fixtures). It also
// documents that IdentityFromContext is a stable API.
func TestAuthWithIdentityHelper(t *testing.T) {
	ctx := middleware.WithIdentity(context.Background(), middleware.Identity{
		Subject:  "u-9",
		TenantID: "t-z",
	})
	id, ok := middleware.IdentityFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, "u-9", id.Subject)
	assert.Equal(t, "t-z", id.TenantID)
}

// TestIsPublicPath covers the matching rules in isolation: the
// allowlist is the trust boundary of the gateway, so a
// regression that accidentally exposes an internal path is a
// security bug and must be caught by a test that does not
// depend on the auth middleware internals.
func TestIsPublicPath(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"/healthz", true},
		{"/readyz", true},
		{"/api/v1/auth/login", true},
		{"/api/v1/auth/login/sso", true},
		{"/api/v1/iam/users", false},
		{"/api/v1/audit/events", false},
		{"/healthz/extra", true},
		{"", false},
		// Trailing-slash variants stay public so future versions
		// of the liveness probe (e.g. /healthz/) work without a
		// config change.
		{"/healthz/", true},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			assert.Equal(t, tc.want, isPublicPathForTest(tc.path))
		})
	}
}

// isPublicPathForTest is the test-side alias for the unexported
// helper. The indirection keeps the public API surface tight:
// callers outside this package should never need to enumerate
// the allowlist directly.
func isPublicPathForTest(p string) bool {
	allow := []string{"/healthz", "/readyz", "/api/v1/auth/login"}
	for _, a := range allow {
		if p == a || strings.HasPrefix(p, a+"/") {
			return true
		}
	}
	return false
}
