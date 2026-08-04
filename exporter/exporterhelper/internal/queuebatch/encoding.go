// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuebatch // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queuebatch"

import "context"

// encoding defines the encoding used for shutdown persistence and startup recovery.
// Duplicate definition with exporterhelper.QueueBatchEncoding since aliasing generics is not supported by default.
type encoding[T any] interface {
	// Marshal is a function that can marshal a request and its context into bytes.
	Marshal(context.Context, T) ([]byte, error)

	// Unmarshal is a function that can unmarshal bytes into a request and its context.
	Unmarshal([]byte) (context.Context, T, error)
}
