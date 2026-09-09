// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/experr"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/sender"
)

// TODO: Clean this by forcing all exporters to return an internal error type that always include the information about retries.
type throttleRetry struct {
	err   error
	delay time.Duration
}

func (t throttleRetry) Error() string {
	return "Throttle (" + t.delay.String() + "), error: " + t.err.Error()
}

func (t throttleRetry) Unwrap() error {
	return t.err
}

// NewThrottleRetry creates a new throttle retry error.
func NewThrottleRetry(err error, delay time.Duration) error {
	return throttleRetry{
		err:   err,
		delay: delay,
	}
}

type retrySender struct {
	component.StartFunc
	cfg      configretry.BackOffConfig
	stopCtx  context.Context
	stopFunc context.CancelFunc
	stopOnce sync.Once
	logger   *zap.Logger
	next     sender.Sender[request.Request]
}

func newRetrySender(config configretry.BackOffConfig, set exporter.Settings, next sender.Sender[request.Request]) *retrySender {
	stopCtx, stopFunc := context.WithCancel(context.Background())
	return &retrySender{
		cfg:      config,
		stopCtx:  stopCtx,
		stopFunc: stopFunc,
		logger:   set.Logger,
		next:     next,
	}
}

func (rs *retrySender) Shutdown(context.Context) error {
	rs.stopOnce.Do(func() {
		rs.stopFunc()
	})
	return nil
}

// Send implements the requestSender interface
func (rs *retrySender) Send(ctx context.Context, req request.Request) error {
	sendCtx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(rs.stopCtx, cancel)
	defer func() {
		stopCancel()
		cancel()
	}()

	// Do not use NewExponentialBackOff since it calls Reset and the code here must
	// call Reset after changing the InitialInterval (this saves an unnecessary call to Now).
	expBackoff := backoff.ExponentialBackOff{
		InitialInterval:     rs.cfg.InitialInterval,
		RandomizationFactor: rs.cfg.RandomizationFactor,
		Multiplier:          rs.cfg.Multiplier,
		MaxInterval:         rs.cfg.MaxInterval,
	}
	span := trace.SpanFromContext(ctx)
	retryNum := int64(0)
	var maxElapsedTime time.Time
	if rs.cfg.MaxElapsedTime > 0 {
		maxElapsedTime = time.Now().Add(rs.cfg.MaxElapsedTime)
	}
	for {
		if rs.stopCtx.Err() != nil {
			return experr.NewShutdownErr(errors.New("retry sender is shutting down"))
		}
		span.AddEvent(
			"Sending request.",
			trace.WithAttributes(attribute.Int64("retry_num", retryNum)))

		err := rs.next.Send(sendCtx, req)
		if err == nil {
			return nil
		}
		if rs.stopCtx.Err() != nil {
			return experr.NewShutdownErr(err)
		}

		// Immediately drop data on permanent errors.
		if consumererror.IsPermanent(err) {
			return fmt.Errorf("not retryable error: %w", err)
		}

		if errReq, ok := req.(request.ErrorHandler); ok {
			req = errReq.OnError(err)
		}

		backoffDelay := expBackoff.NextBackOff()
		if backoffDelay == backoff.Stop {
			return fmt.Errorf("no more retries left: %w", err)
		}

		throttleErr := throttleRetry{}
		if errors.As(err, &throttleErr) {
			backoffDelay = max(backoffDelay, throttleErr.delay)
		}

		nextRetryTime := time.Now().Add(backoffDelay)
		if !maxElapsedTime.IsZero() && maxElapsedTime.Before(nextRetryTime) {
			// The delay is longer than the maxElapsedTime.
			return fmt.Errorf("no more retries left: %w", err)
		}

		if deadline, has := ctx.Deadline(); has && deadline.Before(nextRetryTime) {
			// The delay is longer than the deadline.  There is no point in
			// waiting for cancelation.
			return fmt.Errorf("request will be cancelled before next retry: %w", err)
		}

		backoffDelayStr := backoffDelay.String()
		span.AddEvent(
			"Exporting failed. Will retry the request after interval.",
			trace.WithAttributes(
				attribute.String("interval", backoffDelayStr),
				attribute.String("error", err.Error())))
		rs.logger.Info(
			"Exporting failed. Will retry the request after interval.",
			zap.Error(err),
			zap.String("interval", backoffDelayStr),
		)
		retryNum++

		// back-off, but get interrupted when shutting down or request is cancelled or timed out.
		select {
		case <-sendCtx.Done():
			if rs.stopCtx.Err() != nil {
				return experr.NewShutdownErr(err)
			}
			return fmt.Errorf("request is cancelled or timed out: %w", err)
		case <-rs.stopCtx.Done():
			return experr.NewShutdownErr(err)
		case <-time.After(backoffDelay):
		}
	}
}
