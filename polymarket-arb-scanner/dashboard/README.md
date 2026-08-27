# Polymarket Scanner Dashboard

Vite dashboard for local scanner diagnostics and paper-account state.

## Commands

```sh
npm run lint
npm run build

READINESS_ROOT=/absolute/path/to/a/completed/readiness-run
SCANNER_RELEASE_BINARY="$(realpath "$READINESS_ROOT/release/polymarket-arb-scanner")" \
SCANNER_READINESS_MANIFEST="$(realpath "$READINESS_ROOT/readiness-bundle-manifest.json")" \
SCANNER_BUILD_PROVENANCE="$(realpath "$READINESS_ROOT/build-provenance.json")" \
npm run dev -- --host 127.0.0.1
```

The dev server reads diagnostics from `../runtime_diagnostics` by default. Override with `DIAGNOSTICS_DIR` when needed.
Scanner controls and reset actions require a same-origin `Origin` header. Read-only APIs remain available without it.
Readiness polling never starts the scanner. The Start action verifies the complete readiness bundle, canonical copied release path, build provenance, campaign profit-compatibility fingerprint, and release SHA-256 immediately before spawning that exact binary. There is no `cargo run` fallback and changed source, configuration, or artifacts require a new readiness run.

Dashboard resets preserve campaign evidence. A diagnostics reset is refused after any scan/latency/candidate row exists, and a paper reset is refused after a scanner attempt, trade, or broker trade exists. Start a fresh campaign with a new `DIAGNOSTICS_DIR`, `EXTERNAL_PAPER_DATA_DIR`, and `EXTERNAL_PAPER_ACCOUNT`; do not erase a losing prefix.

To display an activation packet, either copy it to the diagnostics directory as `live-activation-packet.json` or set canonical `ACTIVATION_PACKET_PATH=/absolute/path/live-activation-packet.json`. The dashboard reruns the packet verifier when the packet or a referenced artifact changes and never discovers packets from `/tmp`.
