FROM golang:1.24-alpine AS builder

WORKDIR /app


RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download


COPY . .


RUN CGO_ENABLED=0 GOOS=linux go build -o hatokuse main.go


FROM alpine:3.19

WORKDIR /root/


RUN apk add --no-cache ca-certificates


COPY --from=builder /app/hatokuse .
COPY --from=builder /app/config.yaml .
COPY --from=builder /app/tolerance.conf .


RUN mkdir -p storage_files


EXPOSE 50051 50052 6000

ENTRYPOINT ["./hatokuse"]
