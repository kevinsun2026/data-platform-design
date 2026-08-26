// Package config loads the gateway's runtime configuration from
// environment variables. We intentionally avoid viper / cobra here:
// the gateway has a small, flat config surface and a one-screen
// loader is easier to test and audit than a 100KB dependency tree.
//
// All settings live under the AIDP_ prefix so the same deploy
// manifests can be reused across every service in the platform.
//
// Required:
//
//	AIDP_LISTEN_ADDR        (default ":8000")
//	AIDP_REDIS_ADDR         (default "localhost:6379")
//	AIDP_IAM_URL            (default "http://localhost:8001")
//	AIDP_AUDIT_URL          (default "http://localhost:8002")
//	AIDP_NOTIFY_URL         (default "http://localhost:8003")
//	AIDP_AGENT_URL          (default "http://localhost:8004")
//	AIDP_DATASOURCE_URL     (default "http://localhost:8005")
//
// Optional:
//
//	AIDP_SERVICE_NAME       (default "gateway")
//	AIDP_ENV                (default "dev")
//	AIDP_OTLP_ENDPOINT      (empty = no exporter)
//	AIDP_RATE_LIMIT_RPS     (default 50, per identity, token bucket)
//	AIDP_RATE_LIMIT_BURST   (default 100, bucket capacity)
//	AIDP_HEALTH_TIMEOUT_MS  (default 2000)
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config is the validated, in-memory configuration the gateway boots
// with. It is constructed once at start-up and treated as immutable
// thereafter; tests that need to vary values construct their own copy
// via LoadFromEnv with a custom environment.
type Config struct {
	// ServiceName is the logical name attached to OTel resource
	// attributes and logs.
	ServiceName string
	// Env is the deployment environment label (dev / staging / prod).
	Env string
	// ListenAddr is the bind address for the gateway's HTTP server.
	ListenAddr string
	// RedisAddr is the host:port of the Redis instance used by the
	// rate-limit middleware.
	RedisAddr string
	// OTLPEndpoint, when non-empty, enables OTLP gRPC trace export.
	// Format: "host:4317" (no scheme).
	OTLPEndpoint string
	// RateLimitRPS is the steady-state refill rate of the token bucket
	// per identity (requests per second).
	RateLimitRPS int
	// RateLimitBurst is the maximum bucket depth, i.e. the largest
	// burst a caller can sustain before throttling kicks in.
	RateLimitBurst int
	// HealthTimeout is the per-dependency timeout for the readiness
	// probe (Redis ping).
	HealthTimeout time.Duration
	// DownstreamURLs maps a route prefix to the upstream base URL.
	// Keys are the canonical path prefixes (e.g. "iam", "audit") and
	// values are parsed URLs (scheme + host + optional base path).
	DownstreamURLs map[string]*url.URL
}

// LoadFromEnv reads the process environment and returns a fully
// validated Config. It is the single entry point for production code;
// tests can call it with a custom env map by mutating the os.Environ
// round-trip, or — more commonly — by constructing a Config directly
// from a struct literal.
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		ServiceName:    getEnv("AIDP_SERVICE_NAME", "gateway"),
		Env:            getEnv("AIDP_ENV", "dev"),
		ListenAddr:     getEnv("AIDP_LISTEN_ADDR", ":8000"),
		RedisAddr:      getEnv("AIDP_REDIS_ADDR", "localhost:6379"),
		OTLPEndpoint:   os.Getenv("AIDP_OTLP_ENDPOINT"),
		RateLimitRPS:   getEnvInt("AIDP_RATE_LIMIT_RPS", 50),
		RateLimitBurst: getEnvInt("AIDP_RATE_LIMIT_BURST", 100),
		HealthTimeout:  time.Duration(getEnvInt("AIDP_HEALTH_TIMEOUT_MS", 2000)) * time.Millisecond,
	}

	urls, err := loadDownstreamURLs()
	if err != nil {
		return nil, err
	}
	cfg.DownstreamURLs = urls

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// loadDownstreamURLs parses the per-service base URL env vars into a
// map keyed by route prefix. Returning a map keyed by canonical name
// (rather than scattering five URL fields on the struct) keeps the
// router code path-agnostic: the router only needs to know "this
// prefix → that URL".
func loadDownstreamURLs() (map[string]*url.URL, error) {
	raw := map[string]string{
		"iam":         getEnv("AIDP_IAM_URL", "http://localhost:8001"),
		"audit":       getEnv("AIDP_AUDIT_URL", "http://localhost:8002"),
		"notify":      getEnv("AIDP_NOTIFY_URL", "http://localhost:8003"),
		"agent":       getEnv("AIDP_AGENT_URL", "http://localhost:8004"),
		"datasources": getEnv("AIDP_DATASOURCE_URL", "http://localhost:8005"),
	}
	out := make(map[string]*url.URL, len(raw))
	for name, s := range raw {
		u, err := url.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("config: invalid AIDP_%s_URL %q: %w", envName(name), s, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("config: AIDP_%s_URL must include scheme and host, got %q", envName(name), s)
		}
		out[name] = u
	}
	return out, nil
}

// envName maps a route prefix to the AIDP_*_URL env-var suffix.
// The mapping is co-located with loadDownstreamURLs so renaming a
// downstream is a one-line change.
func envName(prefix string) string {
	switch prefix {
	case "iam":
		return "IAM"
	case "audit":
		return "AUDIT"
	case "notify":
		return "NOTIFY"
	case "agent":
		return "AGENT"
	case "datasources":
		return "DATASOURCE"
	default:
		return prefix
	}
}

// validate enforces the invariants the running gateway depends on.
// Returning an error here is preferable to panicking at first request
// — start-up fails loudly instead of degrading mid-flight.
func (c *Config) validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("config: AIDP_LISTEN_ADDR must not be empty")
	}
	if c.RateLimitRPS <= 0 {
		return fmt.Errorf("config: AIDP_RATE_LIMIT_RPS must be positive, got %d", c.RateLimitRPS)
	}
	if c.RateLimitBurst <= 0 {
		return fmt.Errorf("config: AIDP_RATE_LIMIT_BURST must be positive, got %d", c.RateLimitBurst)
	}
	if c.RateLimitBurst < c.RateLimitRPS {
		// A burst smaller than the refill rate is a config bug: the
		// bucket would never have more tokens than it can spend per
		// second, so steady-state traffic would be throttled.
		return fmt.Errorf(
			"config: AIDP_RATE_LIMIT_BURST (%d) must be >= AIDP_RATE_LIMIT_RPS (%d)",
			c.RateLimitBurst, c.RateLimitRPS,
		)
	}
	if c.HealthTimeout <= 0 {
		return fmt.Errorf("config: AIDP_HEALTH_TIMEOUT_MS must be positive, got %s", c.HealthTimeout)
	}
	return nil
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
