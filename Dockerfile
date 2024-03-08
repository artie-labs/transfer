FROM --platform=linux/amd64 golang:1.22-alpine AS builder

WORKDIR /app

RUN apk -U add ca-certificates
RUN apk update && apk upgrade && apk add pkgconf git bash build-base sudo

COPY . .

RUN go build -o transfer .

FROM --platform=linux/amd64 alpine:3.19 AS runner

COPY --from=builder /app/transfer /
