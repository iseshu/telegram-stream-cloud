FROM golang:1.27.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY web/ ./web/
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/filestreambot .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home app && mkdir /data && chown app:app /data
COPY --from=build /out/filestreambot /usr/local/bin/filestreambot
USER 10001:10001
WORKDIR /app
ENV DATA_DIR=/data PORT=8080 BIND_ADDRESS=0.0.0.0
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/filestreambot"]
