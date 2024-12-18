FROM --platform=linux/amd64 alpine:3.21
COPY transfer /transfer
ENTRYPOINT ["/transfer"]
