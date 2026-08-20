// Package logging is structured, leveled, request-ID propagating logging.
package logging

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey struct{}

// WithRequestID stores the request ID on ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// RequestID returns the request ID from ctx, or "".
func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKey{}).(string)
	return v
}

// New returns a JSON slog logger to stderr.
func New() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
