FROM golang:1.17.9-buster as build
WORKDIR /go/src/provider

COPY go.mod go.sum ./
RUN go get -d -v ./...

ADD . /go/src/provider
RUN CGO_ENABLED=0 go build -o /go/bin/provider ./cmd/provider

FROM gcr.io/distroless/static
COPY --from=build /go/bin/provider /usr/local/
ENTRYPOINT ["/usr/local/provider"]
CMD ["daemon"]
