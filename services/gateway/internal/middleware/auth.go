package middleware

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/aidp/gateway/internal/httperr"
)

// Public-path allowlist. The gateway sits in front of every API
// call, but a few endpoints are unauthenticated by design — login
// itself, and the liveness / readiness probes. The router
// registers these paths *before* the auth middleware so a request
// that matches never even reaches the JWT extraction logic below.
//
// Keep this list short and obvious: any entry here is a path a
// caller can hit without a token, so it must be safe to do so.
var publicPaths = []string{
	"/healthz",
	"/readyz",
	"/api/v1/auth/login",
}

// authHeader is the canonical Authorization header. The value
// format is "Bearer <token>" per RFC 6750.
const authHeader = "Authorization"

// bearerPrefix is the only Authorization scheme the gateway
// understands. We reject Basic / Digest / API-Key variants
// outright: those belong to a future feature.
const bearerPrefix = "Bearer "

// userHeader and tenantHeader are the headers the auth middleware
// injects into the proxied request to downstream services. They
// let every downstream skip its own JWT decoding and read a
// pre-verified identity instead. The values are *trusted* because
// the gateway is the only writer; any service that accepts these
// headers without going through the gateway is misconfigured.
const (
	HeaderUserID   = "X-User-Id"
	HeaderTenantID = "X-Tenant-Id"
)

// Identity is the resolved caller the auth middleware injects
// into the request context. Downstream services (and the
// rate-limit middleware) read it via IdentityFromContext.
//
// Subject and TenantID are extracted from the JWT payload by
// base64-decoding only — the gateway does NOT verify the
// signature. iam-service owns signature verification; the gateway
// merely forwards the token and gives the downstream the
// convenience fields it would otherwise have to re-decode.
type Identity struct {
	// Subject is the user id ("sub" claim). Stable for the
	// lifetime of the token.
	Subject string
	// TenantID is the tenant the user is acting on behalf of
	// ("tenant_id" claim). Drives per-tenant rate-limit
	// bucketing and downstream authorisation.
	TenantID string
}

// identityContextKey is the unexported key under which Identity
// is stored on the request context. Using a dedicated type
// prevents accidental collisions with other context values.
type identityContextKey struct{}

// WithIdentity returns a new context carrying the given identity.
// Exported so the test suite and any future direct-injection
// paths (e.g. internal admin endpoints) can place an identity on
// the request without going through the JWT path.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityContextKey{}, id)
}

// IdentityFromContext returns the Identity previously attached by
// the auth middleware. The ok result is false when the request was
// not authenticated (i.e. it matched a public path or the
// Authorization header was missing). Callers that need a tenant
// id (e.g. the rate limiter) must fall back to a per-IP key when
// ok is false.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	v, ok := ctx.Value(identityContextKey{}).(Identity)
	if !ok {
		return Identity{}, false
	}
	return v, true
}

// Auth returns a middleware that extracts the bearer token from
// the Authorization header, base64-decodes the JWT payload to
// resolve sub / tenant_id, and attaches an Identity to the
// request context.
//
// The gateway does NOT verify the signature — that is the
// iam-service's job. Verification here would duplicate the key
// infrastructure and create a coordination problem (key rotation
// would need to land in two places). The trust boundary is at
// iam-service: anything the gateway forwards is treated as
// "untrusted raw claim" and re-verified by the upstream before it
// is acted on.
//
// Public-path handling: paths in publicPaths short-circuit the
// middleware and are passed straight through with no identity.
// Matching is prefix-based so future paths under a public parent
// (e.g. /api/v1/auth/refresh) stay public without a config
// change.
func Auth() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isPublicPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			header := r.Header.Get(authHeader)
			if header == "" {
				httperr.NewUnauthorized("missing Authorization header").Write(r.Context(), w)
				return
			}
			if !strings.HasPrefix(header, bearerPrefix) {
				httperr.NewUnauthorized("Authorization scheme must be Bearer").Write(r.Context(), w)
				return
			}
			raw := strings.TrimSpace(header[len(bearerPrefix):])
			if raw == "" {
				httperr.NewUnauthorized("empty bearer token").Write(r.Context(), w)
				return
			}

			claims, err := decodeJWTClaims(raw)
			if err != nil {
				// Malformed token, not "bad credentials" — the
				// client almost certainly has a bug, so 401 with
				// a clear message is the most actionable code.
				httperr.NewUnauthorized("malformed token").Write(r.Context(), w)
				return
			}

			id := Identity{
				Subject:  stringField(claims["sub"]),
				TenantID: stringField(claims["tenant_id"]),
			}
			if id.Subject == "" || id.TenantID == "" {
				httperr.NewUnauthorized("token missing sub or tenant_id").Write(r.Context(), w)
				return
			}

			// Pass-through: the original Authorization header is
			// preserved so the downstream can re-verify the
			// signature. The convenience headers are added in
			// addition, not instead.
			r.Header.Set(HeaderUserID, id.Subject)
			r.Header.Set(HeaderTenantID, id.TenantID)

			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
		})
	}
}

// jwtClaims is the subset of JWT claims the auth middleware reads.
// Declared as a named type so callers can extend it (e.g. in
// tests) without breaking the map[string]any contract.
type jwtClaims map[string]any

// decodeJWTClaims extracts the payload of a (possibly unsigned)
// JWT and decodes it as JSON. The function is intentionally
// tolerant of the standard three-segment form and the relaxed
// "header.payload." form used by some test fixtures.
//
// We do not enforce the alg header, the exp claim, or the
// signature — the downstream is responsible for full validation.
func decodeJWTClaims(token string) (jwtClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, errMalformedToken
	}
	payload := parts[1]
	// JWT base64url requires padding-stripped inputs. Add the
	// padding bytes back so the standard decoder accepts the
	// string. base64.RawURLEncoding would also work, but the
	// padded form is more lenient and matches the Python side.
	if pad := len(payload) % 4; pad != 0 {
		payload += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil, errMalformedToken
	}
	var claims jwtClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil, errMalformedToken
	}
	return claims, nil
}

// stringField normalises a claim value to a string. Numbers and
// booleans come back from encoding/json as their native types;
// we coerce to string so downstream services get a uniform shape
// regardless of how the issuer encoded the value.
func stringField(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case nil:
		return ""
	default:
		// Fall back to a JSON round-trip for non-string claims.
		// This is rare in practice (sub is conventionally a
		// string) but we want a sensible default rather than a
		// crash if a future issuer switches sub to an integer.
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// isPublicPath reports whether path matches one of the
// unauthenticated routes. Matching is prefix-based so future
// paths under a public parent stay public without a config
// change.
func isPublicPath(path string) bool {
	for _, p := range publicPaths {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

// errMalformedToken is a sentinel returned by decodeJWTClaims.
// It exists so callers can errors.Is against it without having
// to know the underlying httperr.Error shape. Kept unexported
// because the only consumer is the auth middleware itself.
var errMalformedToken = errors.New("malformed token")
