FROM golang:1.17.9-buster as build
WORKDIR /go/src/provider

COPY go.mod go.sum ./
RUN go get -d -v ./...

ADD . /go/src/provider
RUN go build -o /go/bin/provider

# TODO consider auto initialization flag as part of `daemon` command
ARG INIT_PROVIDER='true'
RUN if test "${INIT_PROVIDER}" = 'false'; then /go/bin/provider init; else echo 'skipping provider initialization.'; fi

FROM gcr.io/distroless/static
COPY --from=build /go/bin/provider /usr/local/
COPY --from=build /root/.index-provider* /root/.index-provider
ENTRYPOINT ["/usr/local/provider"]
CMD ["daemon"]
