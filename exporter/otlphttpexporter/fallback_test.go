// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlphttpexporter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter/internal/metadata"
)

type fallbackRoundTripper func(*http.Request) (*http.Response, error)

func (f fallbackRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	defer req.Body.Close()
	return f(req)
}

func fallbackTestResponse() *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
}

func newFallbackTestExporter(primary, fallback fallbackRoundTripper) *baseExporter {
	e := &baseExporter{
		config: &Config{Encoding: EncodingProto},
		client: &http.Client{Transport: primary},
		logger: zap.NewNop(),
	}
	if fallback != nil {
		e.fallbackClient = &http.Client{Transport: fallback}
	}
	return e
}

func exportFallbackTestRequest(e *baseExporter, ctx context.Context) error {
	return e.export(ctx, "http://localhost/v1/traces", []byte("payload"), e.tracesPartialSuccessHandler)
}

func TestFallbackPrimaryRequestNotCloned(t *testing.T) {
	t.Parallel()
	for _, probe := range []bool{false, true} {
		name := "normal"
		if probe {
			name = "probe"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var received *http.Request
			e := newFallbackTestExporter(func(req *http.Request) (*http.Response, error) {
				received = req
				return fallbackTestResponse(), nil
			}, func(*http.Request) (*http.Response, error) {
				t.Error("unexpected fallback request")
				return fallbackTestResponse(), nil
			})
			if probe {
				e.fallbackDelay = time.Minute
			}
			const url = "http://localhost/v1/traces"
			payload := []byte("payload")
			req, err := e.newRequest(t.Context(), url, payload)
			require.NoError(t, err)
			resp, err := e.doRequest(req, url, payload)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Same(t, req, received)
		})
	}
}

func TestFallbackBackoffAndRecovery(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var primaryCalls, fallbackCalls int
		primaryTimeout := true
		e := newFallbackTestExporter(func(req *http.Request) (*http.Response, error) {
			primaryCalls++
			require.Equal(t, "http://localhost/v1/traces", req.URL.String())
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, "payload", string(body))
			// Model a transport that consumes the body and changes headers.
			req.Header.Set("Content-Encoding", "gzip")
			req.Header.Set("Authorization", "primary")
			req.Host = "primary.local"
			req.URL.Host = "primary.local"
			if primaryTimeout {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			return fallbackTestResponse(), nil
		}, func(req *http.Request) (*http.Response, error) {
			fallbackCalls++
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, "payload", string(body))
			require.Equal(t, int64(len(body)), req.ContentLength)
			require.Equal(t, protobufContentType, req.Header.Get("Content-Type"))
			require.Empty(t, req.Header.Get("Content-Encoding"))
			require.Empty(t, req.Header.Get("Authorization"))
			require.Equal(t, "backup.local:4318", req.Host)
			require.Equal(t, "http://backup.local:4318/tenant/v1/traces", req.URL.String())
			return fallbackTestResponse(), nil
		})
		e.client.Timeout = time.Second
		var err error
		e.fallbackURL, err = url.Parse("http://backup.local:4318/tenant/v1/traces")
		require.NoError(t, err)

		for i, delay := range []time.Duration{time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute} {
			// The current request is resent immediately after the primary times out.
			require.NoError(t, exportFallbackTestRequest(e, t.Context()))
			require.Equal(t, i+1, primaryCalls)
			require.Equal(t, i*3+1, fallbackCalls)
			require.Equal(t, delay, e.fallbackDelay)
			require.Equal(t, time.Now().Add(delay), e.fallbackUntil)

			require.NoError(t, exportFallbackTestRequest(e, t.Context()))
			time.Sleep(delay - time.Nanosecond)
			require.NoError(t, exportFallbackTestRequest(e, t.Context()))
			require.Equal(t, i+1, primaryCalls)
			require.Equal(t, i*3+3, fallbackCalls)
			time.Sleep(time.Nanosecond)
		}

		primaryTimeout = false
		require.NoError(t, exportFallbackTestRequest(e, t.Context()))
		require.Zero(t, e.fallbackDelay)
		require.True(t, e.fallbackUntil.IsZero())
		require.NoError(t, exportFallbackTestRequest(e, t.Context()))
		require.Equal(t, 6, primaryCalls)
		require.Equal(t, 12, fallbackCalls)

		primaryTimeout = true
		require.NoError(t, exportFallbackTestRequest(e, t.Context()))
		require.Equal(t, time.Minute, e.fallbackDelay)
	})
}

func TestFallbackErrorHandling(t *testing.T) {
	t.Parallel()
	transportErr := errors.New("connection refused")
	for _, tc := range []struct {
		name         string
		primaryErr   error
		fallbackErr  error
		noFallback   bool
		cancel       bool
		deadline     bool
		status       int
		wantErr      error
		wantFallback bool
	}{
		{name: "non-timeout", primaryErr: transportErr, wantErr: transportErr},
		{name: "no fallback", primaryErr: context.DeadlineExceeded, noFallback: true, wantErr: context.DeadlineExceeded},
		{name: "canceled", primaryErr: context.Canceled, cancel: true, wantErr: context.Canceled},
		{name: "caller deadline", primaryErr: context.DeadlineExceeded, deadline: true, wantErr: context.DeadlineExceeded},
		{name: "HTTP gateway timeout", status: http.StatusGatewayTimeout},
		{name: "fallback error", primaryErr: context.DeadlineExceeded, fallbackErr: transportErr, wantErr: transportErr, wantFallback: true},
		{name: "fallback timeout", primaryErr: context.DeadlineExceeded, fallbackErr: context.DeadlineExceeded, wantErr: context.DeadlineExceeded, wantFallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fallbackCalls := 0
			e := newFallbackTestExporter(func(*http.Request) (*http.Response, error) {
				if tc.primaryErr != nil {
					return nil, tc.primaryErr
				}
				resp := fallbackTestResponse()
				resp.StatusCode = tc.status
				return resp, nil
			}, func(*http.Request) (*http.Response, error) {
				fallbackCalls++
				return nil, tc.fallbackErr
			})
			if tc.noFallback {
				e.fallbackClient = nil
			}
			ctx := t.Context()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if tc.deadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			err := exportFallbackTestRequest(e, ctx)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.ErrorContains(t, err, "HTTP Status Code 504")
			}
			if tc.wantFallback {
				require.Equal(t, 1, fallbackCalls)
				require.Equal(t, time.Minute, e.fallbackDelay)
			} else {
				require.Zero(t, fallbackCalls)
				require.Zero(t, e.fallbackDelay)
			}
		})
	}
}

func TestFallbackSingleConcurrentProbe(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var primaryCalls, fallbackCalls atomic.Int32
		releaseProbe := make(chan struct{})
		e := newFallbackTestExporter(func(*http.Request) (*http.Response, error) {
			if primaryCalls.Add(1) == 1 {
				return nil, context.DeadlineExceeded
			}
			<-releaseProbe
			return fallbackTestResponse(), nil
		}, func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "http://backup.local/v1/traces", req.URL.String())
			// A transport must not be able to modify the shared fallback URL.
			req.URL.Host = "changed.local"
			req.URL.Path = "/changed"
			fallbackCalls.Add(1)
			return fallbackTestResponse(), nil
		})
		var err error
		e.fallbackURL, err = url.Parse("http://backup.local/v1/traces")
		require.NoError(t, err)
		require.NoError(t, exportFallbackTestRequest(e, t.Context()))
		time.Sleep(time.Minute)

		// All requests compete to probe when the cooldown expires.
		var wg sync.WaitGroup
		for range 21 {
			wg.Go(func() { assert.NoError(t, exportFallbackTestRequest(e, t.Context())) })
		}
		synctest.Wait()
		require.Equal(t, int32(2), primaryCalls.Load())
		require.Equal(t, int32(21), fallbackCalls.Load())
		close(releaseProbe)
		wg.Wait()
		require.Zero(t, e.fallbackDelay)
		require.Equal(t, "http://backup.local/v1/traces", e.fallbackURL.String())
	})
}

func TestFallbackSignalURL(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		endpoint, signal, version, want string
	}{
		{"https://backup.local", "traces", "v1", "https://backup.local/v1/traces"},
		{"https://backup.local/tenant/", "metrics", "v1", "https://backup.local/tenant/v1/metrics"},
		{"https://backup.local/tenant", "logs", "v1", "https://backup.local/tenant/v1/logs"},
		{"https://backup.local", "profiles", "v1development", "https://backup.local/v1development/profiles"},
		{"https://backup.local/base?tenant=1", "logs", "v1", "https://backup.local/base/v1/logs?tenant=1"},
		{"", "traces", "v1", ""},
	} {
		t.Run(tc.endpoint+tc.signal, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{
				FallbackClient:   &confighttp.ClientConfig{Endpoint: tc.endpoint},
				TracesEndpoint:   "http://primary.local/custom/traces",
				MetricsEndpoint:  "http://primary.local/custom/metrics",
				LogsEndpoint:     "http://primary.local/custom/logs",
				ProfilesEndpoint: "http://primary.local/custom/profiles",
			}
			got, err := composeFallbackSignalURL(cfg, tc.signal, tc.version)
			require.NoError(t, err)
			if tc.want == "" {
				require.Nil(t, got)
			} else {
				require.Equal(t, tc.want, got.String())
			}
		})
	}
	got, err := composeFallbackSignalURL(&Config{}, "logs", "v1")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestFallbackInitialization(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{"https://backup.local/base", "", "://bad", "backup.local", "ftp://backup.local", "https://backup.local/#fragment"} {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			cfg := createDefaultConfig().(*Config)
			cfg.ClientConfig.Endpoint = "http://primary.local"
			cfg.FallbackClient = &confighttp.ClientConfig{Endpoint: endpoint}
			err := cfg.Validate()
			_, createErr := newExporter(cfg, exportertest.NewNopSettings(metadata.Type))
			if endpoint == "https://backup.local/base" || endpoint == "" {
				require.NoError(t, err)
				require.NoError(t, createErr)
			} else {
				require.ErrorContains(t, err, "fallback_client.endpoint")
				require.ErrorContains(t, createErr, "fallback_client.endpoint")
			}
		})
	}
	cfg := createDefaultConfig().(*Config)
	cfg.ClientConfig.Endpoint = "http://primary.local"
	cfg.FallbackClient = &confighttp.ClientConfig{Endpoint: "https://backup.local"}
	cfg.FallbackClient.TLS.CAFile = "testdata/nonexistent-fallback-ca.pem"
	e, err := newExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	require.NoError(t, err)
	require.ErrorContains(t, e.start(t.Context(), componenttest.NewNopHost()), "failed to create fallback HTTP client")
}

func TestFallbackIgnoresStalePrimaryResults(t *testing.T) {
	t.Parallel()
	for _, lateTimeout := range []bool{false, true} {
		name := "success"
		if lateTimeout {
			name = "timeout"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var primaryCalls atomic.Int32
				releaseOld := make(chan struct{})
				e := newFallbackTestExporter(func(*http.Request) (*http.Response, error) {
					if primaryCalls.Add(1) == 1 {
						<-releaseOld
						if !lateTimeout {
							return fallbackTestResponse(), nil
						}
					}
					return nil, context.DeadlineExceeded
				}, func(*http.Request) (*http.Response, error) { return fallbackTestResponse(), nil })
				oldDone := make(chan error, 1)
				go func() { oldDone <- exportFallbackTestRequest(e, t.Context()) }()
				synctest.Wait()
				require.NoError(t, exportFallbackTestRequest(e, t.Context()))
				until := e.fallbackUntil
				close(releaseOld)
				require.NoError(t, <-oldDone)
				require.Equal(t, time.Minute, e.fallbackDelay)
				require.Equal(t, until, e.fallbackUntil)
			})
		})
	}
}

func TestFallbackProbeNonTimeoutError(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		primaryErr := error(context.DeadlineExceeded)
		e := newFallbackTestExporter(func(*http.Request) (*http.Response, error) {
			return nil, primaryErr
		}, func(*http.Request) (*http.Response, error) { return fallbackTestResponse(), nil })
		require.NoError(t, exportFallbackTestRequest(e, t.Context()))
		time.Sleep(time.Minute)
		primaryErr = errors.New("connection refused")
		require.ErrorIs(t, exportFallbackTestRequest(e, t.Context()), primaryErr)
		require.Equal(t, time.Minute, e.fallbackDelay)
		require.Equal(t, time.Now().Add(time.Minute), e.fallbackUntil)
		require.False(t, e.clientProbe)
		require.NoError(t, exportFallbackTestRequest(e, t.Context()))
	})
}
