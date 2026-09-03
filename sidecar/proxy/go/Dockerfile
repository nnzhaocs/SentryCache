FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY proxy.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/proxy proxy.go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/proxy /proxy
USER nonroot
ENTRYPOINT ["/proxy"]
