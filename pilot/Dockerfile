FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git curl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(go env GOARCH)/kubectl" \
    && chmod +x kubectl && mv kubectl /usr/local/bin/
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o regret-pilot .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl
COPY --from=builder /usr/local/bin/kubectl /usr/local/bin/kubectl
COPY --from=builder /build/regret-pilot /usr/local/bin/regret-pilot
EXPOSE 8080 9090
CMD ["regret-pilot"]
