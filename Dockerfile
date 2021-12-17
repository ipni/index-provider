FROM golang:1.16-buster as build
WORKDIR /go/src/provider
ARG INIT_PROVIDER='true'

# Add the source code
ADD . /go/src/provider

# Build the provider
RUN go get -d -v ./...
RUN cd cmd/provider && go build -o /go/bin/provider

# Manage initialization
RUN if test "${INIT_PROVIDER}" = 'true'; then echo 'Initializing the provider ...'; /go/bin/provider init; else echo 'Skipped provider initialization.'; fi

# Build the final image
FROM gcr.io/distroless/base-debian11
COPY --from=build /go/bin/provider /
COPY --from=build /root/.index-provider* /root/.index-provider
ENTRYPOINT ["/provider"]
CMD ["daemon"]
