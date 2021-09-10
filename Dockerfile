FROM golang:1.16-buster as build

WORKDIR /go/src/provider

COPY go.mod go.sum ./
RUN go get -d -v ./...

ADD . /go/src/provider
RUN go build -o /go/bin/provider

# TODO consider auto initialization flag as part of `daemon` command
ARG INIT_PROVIDER='true'
RUN if test "${INIT_PROVIDER}" = 'false'; then /go/bin/provider init; else echo 'skipping provider initialization.'; fi

FROM gcr.io/distroless/base-debian11
COPY --from=build /go/bin/provider /
COPY --from=build /root/.reference-provider* /root/.reference-provider
ENTRYPOINT ["/provider"]
CMD ["daemon"]
