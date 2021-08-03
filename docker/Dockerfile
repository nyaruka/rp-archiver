FROM golang:1.15.14-alpine3.14

WORKDIR /app

RUN apk update \
  && apk add --virtual build-deps gcc git \
  && rm -rf /var/cache/apk/*

RUN addgroup -S golang \
  && adduser -S -G golang golang

COPY . .

RUN go install -v ./cmd/...
RUN chown -R golang /app

USER golang

EXPOSE 8080
ENTRYPOINT ["rp-archiver"]
