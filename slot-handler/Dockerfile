FROM golang:1.24.4 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/slot-handler .

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin slot-handler \
    && mkdir -p /app \
    && chown slot-handler:slot-handler /app

WORKDIR /app

COPY --from=build /out/slot-handler /app/slot-handler

RUN chown slot-handler:slot-handler /app/slot-handler

USER slot-handler

EXPOSE 8080

ENTRYPOINT ["/app/slot-handler"]
CMD ["-c", "/app/config.json"]
