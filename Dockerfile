# Build stage
FROM golang:1.21-alpine AS builder
WORKDIR /app

# Install build tools
RUN apk add --no-cache git

# Dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy full source
COPY . .

# Build the service (entrypoint = cmd/main.go)
RUN go build -o event-pipeline cmd/main.go

# Runtime stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/event-pipeline .

EXPOSE 8080
CMD ["./event-pipeline"]
