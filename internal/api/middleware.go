package api

import (
    "context"
    "net/http"

    "github.com/google/uuid"
    "event-pipeline/pkg/logger"
)

type ctxKey string

const requestIDKey ctxKey = "request_id"

func RequestIDMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        rid := r.Header.Get("X-Request-ID")
        if rid == "" {
            rid = uuid.New().String()
        }
        ctx := context.WithValue(r.Context(), requestIDKey, rid)
        logger.Get().Infow("incoming request",
            "method", r.Method,
            "url", r.URL.String(),
            "request_id", rid,
        )
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}

func GetRequestID(ctx context.Context) string {
    if v, ok := ctx.Value(requestIDKey).(string); ok {
        return v
    }
    return ""
}
