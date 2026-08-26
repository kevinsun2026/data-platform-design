package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withEnv swaps the process environment for the duration of a test,
// restoring the original values on cleanup. We can't use t.Setenv
// because some envs need to be unset (LookupEnv fallback path).
func withEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

// TestLoadFromEnvDefaults exercises the contract every platform
// service relies on: AIDP_* env vars override defaults, and the
// resulting Config is fully populated and valid.
func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("AIDP_LISTEN_ADDR", "")
	t.Setenv("AIDP_REDIS_ADDR", "")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)

	assert.Equal(t, "gateway", cfg.ServiceName)
	assert.Equal(t, ":8000", cfg.ListenAddr)
	assert.Equal(t, "localhost:6379", cfg.RedisAddr)
	assert.Equal(t, 50, cfg.RateLimitRPS)
	assert.Equal(t, 100, cfg.RateLimitBurst)
}

// TestLoadFromEnvOverrides verifies env-var wiring for every value
// the operator is likely to tune in a Helm chart.
func TestLoadFromEnvOverrides(t *testing.T) {
	withEnv(t, map[string]string{
		"AIDP_SERVICE_NAME":     "gateway-eu",
		"AIDP_ENV":              "staging",
		"AIDP_LISTEN_ADDR":      ":9000",
		"AIDP_REDIS_ADDR":       "redis.internal:6379",
		"AIDP_OTLP_ENDPOINT":    "otel-collector:4317",
		"AIDP_RATE_LIMIT_RPS":   "200",
		"AIDP_RATE_LIMIT_BURST": "500",
		"AIDP_IAM_URL":          "http://iam:80",
		"AIDP_AUDIT_URL":        "http://audit:80",
		"AIDP_NOTIFY_URL":       "http://notify:80",
		"AIDP_AGENT_URL":        "http://agent:80",
		"AIDP_DATASOURCE_URL":   "http://datasource:80",
	})

	cfg, err := LoadFromEnv()
	require.NoError(t, err)

	assert.Equal(t, "gateway-eu", cfg.ServiceName)
	assert.Equal(t, "staging", cfg.Env)
	assert.Equal(t, ":9000", cfg.ListenAddr)
	assert.Equal(t, "redis.internal:6379", cfg.RedisAddr)
	assert.Equal(t, "otel-collector:4317", cfg.OTLPEndpoint)
	assert.Equal(t, 200, cfg.RateLimitRPS)
	assert.Equal(t, 500, cfg.RateLimitBurst)
	assert.Equal(t, "http://iam:80", cfg.DownstreamURLs["iam"].String())
	assert.Equal(t, "http://audit:80", cfg.DownstreamURLs["audit"].String())
	assert.Equal(t, "http://notify:80", cfg.DownstreamURLs["notify"].String())
	assert.Equal(t, "http://agent:80", cfg.DownstreamURLs["agent"].String())
	assert.Equal(t, "http://datasource:80", cfg.DownstreamURLs["datasources"].String())
}

// TestValidateRejectsBadRateLimit pins the bucket invariants. A
// regression that lets RPS > BURST through would silently throttle
// every legitimate caller, so we surface it as a start-up error.
func TestValidateRejectsBadRateLimit(t *testing.T) {
	cases := []struct {
		name    string
		rps     int
		burst   int
		wantErr bool
	}{
		{"valid equal", 10, 10, false},
		{"valid burst larger", 10, 50, false},
		{"burst smaller than rps", 50, 10, true},
		{"zero rps", 0, 10, true},
		{"negative burst", 10, -1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				ListenAddr:     ":8000",
				RateLimitRPS:   tc.rps,
				RateLimitBurst: tc.burst,
				HealthTimeout:  1,
			}
			err := cfg.validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDownstreamURLMustHaveScheme guards against a class of Helm
// chart bug where a downstream URL is set to "iam-service:80"
// (no scheme) and the reverse proxy silently fails every request.
func TestDownstreamURLMustHaveScheme(t *testing.T) {
	t.Setenv("AIDP_IAM_URL", "iam-service:80")
	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scheme")
}
