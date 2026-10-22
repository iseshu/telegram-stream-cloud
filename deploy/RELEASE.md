# Prepare the GitHub release

The workspace contains a release-ready source tree and local binaries under `dist/`. The project is prepared for the `telegram-stream-cloud` GitHub repository.

1. Put the source tree in the intended GitHub repository. The source ZIP excludes `.env`, all runtime data, and compiled binaries.
2. Keep bot tokens and sessions on the deployment host; they are not needed by GitHub Actions.
3. Push the version tag `v2.1-go` after reviewing the source. The release workflow runs vet and the race-enabled tests, builds static Linux amd64/arm64 binaries, writes checksums, and creates a **draft** GitHub release.
4. Review the draft assets and release notes, then publish when ready.
5. Download the correct binary to your host, verify the checksum, supply the private `.env`, run `--check`, and start the service. Use the Dockerfile if your host builds containers from source.

The checked-in workflow needs GitHub Actions enabled with permission to write release assets. Source code and release assets have not been uploaded by preparing this folder.
