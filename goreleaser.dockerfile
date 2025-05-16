FROM --platform=linux/amd64 alpine:3.21
RUN apk add --no-cache tzdata
COPY transfer /transfer
ENTRYPOINT ["/transfer"]
