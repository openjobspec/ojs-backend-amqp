FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ojs-server ./cmd/ojs-server/

FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -S ojs && adduser -S ojs -G ojs

COPY --from=builder /ojs-server /usr/local/bin/ojs-server

USER ojs

EXPOSE 8080 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/ojs/v1/health || exit 1

ENTRYPOINT ["ojs-server"]
