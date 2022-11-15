FROM golang:1.19-alpine AS builder

ENV PATH="/go/bin:${PATH}"
ENV GO111MODULE=on
ENV CGO_ENABLED=1

### Commenting this out for now, see: https://github.com/confluentinc/confluent-kafka-go/issues/898
# ENV GOOS=linux
# ENV GOARCH=amd64

WORKDIR /go/src

COPY go.mod .
COPY go.sum .
RUN go mod download

RUN apk -U add ca-certificates
RUN apk update && apk upgrade && apk add pkgconf git bash build-base sudo
RUN git clone https://github.com/edenhill/librdkafka.git && cd librdkafka && ./configure --disable-sasl --prefix /usr && make && make install

COPY . .

# RUN go build -tags dynamic --ldflags "-extldflags -static" -o transfer .

# FROM alpine:3.16 AS runner

# COPY --from=builder /go/src/transfer /
