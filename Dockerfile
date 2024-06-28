FROM --platform=linux/amd64 golang:1.22-alpine3.20 AS builder

WORKDIR /app

RUN apk -U add ca-certificates && apk update && apk upgrade && apk add git

COPY . .

RUN go build -o transfer .

FROM --platform=linux/amd64 alpine:3.20 AS runner

COPY --from=builder /app/transfer /
