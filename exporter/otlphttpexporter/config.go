// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlphttpexporter // import "go.opentelemetry.io/collector/exporter/otlphttpexporter"

import (
	"encoding"
	"errors"
	"fmt"
	"net/url"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// EncodingType defines the type for content encoding
type EncodingType string

const (
	EncodingProto EncodingType = "proto"
	EncodingJSON  EncodingType = "json"
)

var _ encoding.TextUnmarshaler = (*EncodingType)(nil)

// UnmarshalText unmarshalls text to an EncodingType.
func (e *EncodingType) UnmarshalText(text []byte) error {
	if e == nil {
		return errors.New("cannot unmarshal to a nil *EncodingType")
	}

	str := string(text)
	switch str {
	case string(EncodingProto):
		*e = EncodingProto
	case string(EncodingJSON):
		*e = EncodingJSON
	default:
		return fmt.Errorf("invalid encoding type: %s", str)
	}

	return nil
}

// Config defines configuration for OTLP/HTTP exporter.
type Config struct {
	ClientConfig confighttp.ClientConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	// FallbackClient is used after a timeout. Its endpoint is a base URL with
	// signal paths appended independently of the primary signal endpoints.
	// An empty fallback endpoint keeps the primary request URL.
	FallbackClient *confighttp.ClientConfig                                 `mapstructure:"fallback_client"`
	QueueConfig    configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	RetryConfig    configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`

	// The URL to send traces to. If omitted the Endpoint + "/v1/traces" will be used.
	TracesEndpoint string `mapstructure:"traces_endpoint"`

	// The URL to send metrics to. If omitted the Endpoint + "/v1/metrics" will be used.
	MetricsEndpoint string `mapstructure:"metrics_endpoint"`

	// The URL to send logs to. If omitted the Endpoint + "/v1/logs" will be used.
	LogsEndpoint string `mapstructure:"logs_endpoint"`

	// The URL to send profiles to. If omitted the Endpoint + "/v1development/profiles" will be used.
	ProfilesEndpoint string `mapstructure:"profiles_endpoint"`

	// The encoding to export telemetry (default: "proto")
	Encoding EncodingType `mapstructure:"encoding"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if cfg.ClientConfig.Endpoint == "" && cfg.TracesEndpoint == "" && cfg.MetricsEndpoint == "" && cfg.LogsEndpoint == "" && cfg.ProfilesEndpoint == "" {
		return errors.New("at least one endpoint must be specified")
	}
	return cfg.validateFallback()
}

func (cfg *Config) validateFallback() error {
	if cfg.FallbackClient == nil {
		return nil
	}
	if err := cfg.FallbackClient.Validate(); err != nil {
		return fmt.Errorf("fallback_client: %w", err)
	}
	if cfg.FallbackClient.Endpoint != "" {
		u, err := url.Parse(cfg.FallbackClient.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Fragment != "" {
			return errors.New("fallback_client.endpoint must be an absolute HTTP or HTTPS URL without a fragment")
		}
	}
	return nil
}
