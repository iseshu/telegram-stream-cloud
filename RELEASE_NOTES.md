# FileStream 2.1-go

Initial Go implementation of the Telegram-backed streaming origin and cloud leech engine.

## Included

- Persistent, isolated worker sessions with round-robin reads and flood-wait handling.
- Named signed links such as `/watch/FILE_IDHASH/filename.ext` (also `/inline/` and `/dl/`); precise single-range responses and virtual multipart files.
- Bounded header/tail cache and coalesced requests; cancellation and stream admission control.
- URL inspection, parallel range uploads across ready workers, sequential compatibility mode, and single-slice disk fallback.
- Persistent job progress, explicit resume, and channel manifest backups/restoration.
- Owner-only Telegram commands, a responsive player, health/readiness endpoints, Docker deployment, and optional CDN caching policy.

## Installation

Download the binary for your Linux architecture, verify its SHA256 against `SHA256SUMS`, make it executable, and place your private `.env` alongside it. Start with `./filestreambot-linux-amd64 --check` (or the arm64 equivalent), then run without `--check` to serve. Mount persistent storage at `DATA_DIR`; set `BASE_URL` to the HTTPS URL routed to your deployment.

The release archive never includes `.env`, live Telegram sessions, signing keys, or job state. Keep these separately on your host. Read `README.md` for CDN cache-key requirements and resource limits.

## Validation and limits

The automated suite and Go vet pass locally. Live read-only checks authenticated the configured primary bot and seven workers, confirmed channel access, and created persistent MTProto sessions. Tests cover a complete parallel leech with mocked Telegram, exact stitched output, small/large upload chunking, HTTP ranges, cancellation, source changes, and fallback cleanup.

No live media upload or multi-gigabyte load test has been performed. Local race detection is unavailable because this environment has no C compiler; the GitHub workflow runs `go test -race` before creating a draft release. ARM64 is cross-compiled and not executed locally. Docker execution has not been tested in this environment.

This is a streaming origin. A global CDN, HTTPS/domain routing, and hosting must be configured separately. Unknown-length remote sources are rejected; safe resume requires a source validator. The cache prioritizes header/tail blocks and does not parse media indexes. Full browser codec compatibility and instant seeking are not guaranteed.

## JPlayer web player

The watch page uses self-hosted [JPlayer](https://github.com/devgamer383/jplayer) assets pinned to commit `f258e127fc5caf14f2ad7327886412c334223cc5`, embedded in the binary. It includes playback speed, volume, seeking, fullscreen, copy-link, download, and VLC controls. Named `/watch/FILE_IDHASH/filename.ext` links are preserved. Playback uses the browser’s native HTTP range pipeline; the upstream incomplete MKV demuxer is bypassed. Unsupported codecs still require an external player. See `web/vendor/jplayer/LICENSE.md` for its MIT license.
