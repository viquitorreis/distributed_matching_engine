FROM golang:1.26.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -ldflags="-s -w" -o /engine ./cmd/node

FROM alpine:3.21
WORKDIR /app
COPY --from=builder /engine /app/engine
EXPOSE 8080
ENTRYPOINT ["/app/engine"]