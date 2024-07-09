FROM --platform=linux/amd64 alpine:3.20
COPY transfer /transfer
ENTRYPOINT ["/transfer"]
