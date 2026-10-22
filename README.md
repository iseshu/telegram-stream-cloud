# Telegram Stream Cloud

A Go Telegram-backed HTTP streaming origin with a primary command bot, persistent MTProto worker sessions, virtual multipart files, and bounded-memory cloud leeching.

## Run

Requires Go 1.27 or the container build. Put the supplied configuration in `.env` and give every bot access to the storage channel. Bots need post permission; channel manifest captions or pins additionally require edit/pin permission. The configured owner is the only user whose private messages are processed.

```sh
go build -buildvcs=false -trimpath -o bin/filestreambot .
./bin/filestreambot --check
./bin/filestreambot
```

`--check` authenticates workers and verifies primary bot/channel permissions without polling commands or posting messages. Sessions and the link signing key are persisted even in check mode. `.env` is parsed as data and never executed; process environment overrides it. No secret values are logged. Keep `.env` and the entire data directory private.

Alternatively:

```sh
docker compose up --build -d
```

Set `BASE_URL` to the publicly reachable HTTPS origin. Configure DNS and your HTTPS reverse proxy separately; the program does not provision hosting or a CDN. `deploy/nginx.conf` contains the streaming proxy settings. Compose binds port 8080 to localhost for the proxy. The native binary uses `BIND_ADDRESS` from your configuration.

## Commands

- Send a Telegram document, video, or audio file: copy it into the storage channel and receive links.
- `/leech https://example.com/file`: inspect and upload a remote object.
- `/status`: worker readiness and active job.
- `/cancel`: cancel the current leech job.
- `/job JOB_ID`: inspect saved progress.
- `/resume JOB_ID`: resume a job after verifying its source and slice configuration.
- `/link FILE_ID`: retrieve an existing file's links.
- `/restore MANIFEST_MESSAGE_ID`: recover a leech manifest stored in the channel.

One leech job runs at a time, using all configured ready workers when the source supports ranges and has a usable validator. Bot updates are acknowledged locally before handling; interrupted transfers remain visible as jobs and can be resumed explicitly. The bot uses the HTTP Bot API for commands and server-side copying; worker data transfers use MTProto.

## HTTP behavior

`/watch/FILE_IDHASH/filename.ext` is the player, `/inline/FILE_IDHASH/filename.ext` streams, and `/dl/FILE_IDHASH/filename.ext` downloads. The FILE_IDHASH segment combines the file identifier and its verification hash; filenames are URL-encoded, with no query string required. Links are signed bearer capabilities with no expiry: anyone with the link can access the file. Preserve `data/link.key` to keep links valid. Deleting a local manifest revokes origin access; an enabled CDN may retain content until its TTL expires.

GET and HEAD, ETags, If-None-Match, If-Range, single-byte probes, suffix/open ranges, and 416 responses are supported. Multiple requested ranges are ignored and receive a full 200 response. Files are split into raw bytes and stitched without remuxing. Browser playback depends on codec support; VLC handles a wider range of formats. There is no transcoding or adaptive bitrate output.

`/health` reports anonymous worker counts, cache usage, and active streams; it never includes credentials, URLs, channel identifiers, or sessions. `/ready` returns 204 when at least one worker is ready, otherwise 503. Restrict these endpoints at the reverse proxy if operational statistics should be private.

## CDN integration

This service is a single origin, not a globally distributed CDN. An external CDN must support your media traffic, object sizes, and byte ranges. Default responses use `private` caching. To opt into shared edge caching, set `CDN_CACHE_SECONDS` to an appropriate TTL and configure the CDN to:

1. Include the **entire signed URL path, including FILE_IDHASH**, in its cache key. Older query-signed links also require the signature query parameter in the cache key.
2. Cache only successful media GET/HEAD responses; bypass `/watch/`, `/health`, and `/ready`.
3. Respect range semantics and origin cache headers. Disable content transformation/compression for media.
4. Retain the exact public path and query when forwarding to the origin.

Never configure a cache rule that drops or normalizes away the verification hash without edge authorization. Nginx response buffering and CDN object caching are separate settings. `X-Accel-Buffering: no` assists Nginx; it cannot override every CDN product's buffering behavior.

## Resources and recovery

- `LEECH_PART_SIZE_MB=1600` means **1,600,000,000 bytes**, not MiB.
- Upload payload buffers are 512 KiB per active worker, retained until Telegram acknowledges each chunk. Protocol, TLS, and Go allocations add overhead; 2 MB per worker is a design target, not an RSS guarantee.
- The byte-budgeted LRU stores the first/last 8 MiB of parts, up to `CACHE_SIZE_MB`. It does not parse MP4 moov atoms or MKV cue tables, so instant seeking is not guaranteed.
- Stream prefetch is bounded by `PREFETCH_CHUNKS`. Admission uses `MAX_STREAMS` (default 32); the configuration rejects a buffer budget exceeding `MEMORY_LIMIT_MB`.
- Normal media transfers use no local payload files. Small sessions, state, manifests and signing keys are written to disk.
- On a resumable source failure, fallback stages at most one slice globally, checks free space with 128 MiB headroom, then deletes it on exit. Startup removes abandoned slice files under the service lock.
- Sources with unknown lengths are rejected. Sources without stable ETag/Last-Modified use a sequential stream and cannot be safely resumed. Range support alone does not guarantee a stable object.
- Jobs preserve completed message IDs. Already uploaded parts remain in the channel after cancellation/failure to support resume. No destructive cleanup of channel files is automatic.
- Completed leech manifests are also uploaded as JSON. The first part's caption receives the manifest or a pointer; if caption editing is denied, pinning is attempted. If both are denied the JSON backup remains in the channel and its message ID is logged.
- Files copied directly from Telegram have local manifests only. Back up the data volume to preserve those mappings.

Mount persistent storage for `data/`. A container memory limit does not enforce a disk quota: configure a volume quota on your hosting platform. A 1.6 GB fallback slice leaves limited room in a 2 GB filesystem; session/state growth and free-space checks still matter. Compilation needs more memory than the running service; use the cloud build workflow for small deployment containers.

## Verification and releases

```sh
go test ./...
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags='-s -w' -o bin/filestreambot .
```

CI builds on pushes and pull requests. Pushing a `v*` tag runs checks and creates a draft release with static amd64/arm64 binaries and SHA256 checksums. Publishing the draft and deploying the service are separate steps.

Unit/integration tests cover exact ranges across virtual parts, conditional responses, admission limits, cache concurrency, manifest validation, URL address restrictions, and changing remote validators. Live throughput, real multi-gigabyte uploads, recovery under Telegram throttling, and 2 GB container load testing require deployment measurements.
