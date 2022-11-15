FROM golang:1.19-alpine AS builder

WORKDIR /app

RUN apk -U add ca-certificates
RUN apk update && apk upgrade && apk add pkgconf git bash build-base sudo

COPY . .

RUN go build -o transfer .

FROM alpine:3.16 AS runner

COPY --from=builder /app/transfer /
