FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/swipeline ./cmd/replayer
RUN mkdir -p /data
FROM scratch
COPY --from=build /out/swipeline /swipeline
COPY --from=build --chown=65532:65532 /data /data
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/swipeline"]
CMD ["serve", "--listen", ":8080", "--data", "/data"]
