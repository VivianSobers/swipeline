FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /gateway ./cmd/gateway
FROM scratch
COPY --from=build /gateway /gateway
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/gateway"]
CMD ["serve"]
