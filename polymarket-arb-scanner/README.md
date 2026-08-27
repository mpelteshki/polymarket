# Polymarket Arbitrage Scanner

Rust scanner for complete-set, negative-risk, bundle, ranked-family, and Combo/RFQ opportunities. It uses Polymarket CLOB depth, fee metadata, freshness checks, execution-size validation, paper execution, and fail-closed live gates.

Profit is never guaranteed. Unit tests prove code behavior; paper evidence estimates execution performance; only settled live fills establish live P&L. The project deliberately keeps those claims separate.

## Current status

| Area | Status | Meaning |
|---|---|---|
| Rust behavior | Verified | Full test suite, formatting, and strict Clippy are CI gates. |
| Paper adapter | Verified operational | Isolated FOK canary and scanner execution-path proof pass without live submission. |
| Paper profitability | Evidence required | Synthetic/canary trades do not count. Real campaign data must pass `paper_profitability_gate.py`. |
| HFT scan path | Verified locally | Readiness run checks WebSocket coverage, REST final guard, quote completeness, and latency. |
| Dashboard | Verified locally | Lint/build/render checks pass; large scan history is streamed incrementally. |
| Live submission | Fail-closed | Requires operator preflight, activation packet, live-ready gate, and guarded launcher. |

See [TRADE_READINESS.md](TRADE_READINESS.md) for every proof artifact and live blocker.

## Requirements

- Rust 1.94.0 via `rust-toolchain.toml`
- Node.js 24 and npm
- `jq`, `curl`, `lsof`, `rg`, `perl`, and `shasum`
- `pm-trader` for paper execution
- `browse` for rendered dashboard verification
- enough temporary disk for two sequential fresh release builds (the build directories are removed after attestation)

## Setup

```sh
cp .env.example .env
cargo test --all-targets

cd dashboard
npm ci
npm run lint
npm run build
```

Keep `LIVE_TRADING_ENABLED=false` while developing or collecting paper evidence. A true ambient live flag now stops ordinary scanner startup unless the guarded launcher supplied its confirmation.

## Read-only scan

```sh
PAPER_TRADING_ENABLED=false \
LIVE_TRADING_ENABLED=false \
cargo run --release -- --once --no-paper
```

Diagnostics default to `runtime_diagnostics/`. High-volume CSVs rotate at the configured size; `trades.csv` is retained because it is profitability evidence.

Public discovery supports Kalshi, Manifold, Seer, and SX Bet by default, with
Limitless, PredictIt, and BetDEX opt-in. Every external token is marked scan-only
and is rejected before notification, paper execution, or live submission.
Limitless stays out of the latency-sensitive default because its useful signal
requires a paired venue. Its opt-in adapter pages the CLOB index, selects only
`isPolyArbitrage=true` binary mirrors, then derives both sides from fresh public
order-book depth. Midpoint prices and unverified title matches never become
executable quotes. See the official
[Limitless market API](https://docs.limitless.exchange/api-reference/markets/browse-active)
and [Polymarket migration mapping](https://docs.limitless.exchange/developers/migrate-from-polymarket).

Evaluate cross-venue mirrors without credentials or submission:

```sh
scripts/cross_venue_shadow.py \
  --shares 100 \
  --gas-transfer-buffer-usd 2 \
  --output paper_campaign/research/cross-venue-shadow.json
```

The report depth-walks both books, recomputes Polymarket's compact `fd` fee
curve, and charges the maximum documented 3% Limitless buy fee at full payout
value. Exact-title discovery is never treated as resolution equivalence. A
reviewed certificate containing both venue slugs and both current rule hashes
can mark rules as checked, but every route remains `shadow_only_no_submit` and
is excluded from the paper profitability gate until an atomic-or-recoverable
cross-chain route exists. Limitless documents CLOB taker fees in its official
[fee schedule](https://docs.limitless.exchange/user-guide/fees).

## Paper campaign

Bootstrap an attested scanner first. The readiness run performs two fresh,
isolated, locked release builds and refuses to continue unless their binaries
are byte-identical. It copies that binary into the readiness bundle:

```sh
BOOTSTRAP_ROOT="$(mktemp -d /tmp/polymarket-readiness-bootstrap-XXXXXX)"
READINESS_ROOT="$BOOTSTRAP_ROOT" \
scripts/trade-readiness.sh --allow-live-blocked

READINESS_MANIFEST="$BOOTSTRAP_ROOT/readiness-bundle-manifest.json"
SCANNER_RELEASE_BINARY="$(jq -r '.build.binary_path' "$READINESS_MANIFEST")"
export READINESS_MANIFEST SCANNER_RELEASE_BINARY
test -x "$SCANNER_RELEASE_BINARY"
```

This first run is expected to report missing profitability evidence. Keep the
source tree, Rust toolchain, and `pm-trader` entry executable unchanged while
collecting the campaign. Use persistent paths so fills survive process
restarts, and run the copied executable rather than rebuilding with
`cargo run`:

```sh
LIVE_TRADING_ENABLED=false \
LIVE_DIAGNOSTICS_ENABLED=false \
PAPER_TRADING_ENABLED=true \
DRY_RUN_PROVIDER=external \
PAPER_REQUIRE_FULL_CLOB_QUOTES=true \
PAPER_MATCH_LIVE_POSITION_SIZE=true \
PAPER_USE_LIMIT_ORDERS=true \
EXTERNAL_PAPER_ORDER_TYPE=fok \
LIVE_ORDER_TYPE=fok \
EXTERNAL_PAPER_DATA_DIR=paper_campaign/account \
EXTERNAL_PAPER_ACCOUNT=arb-campaign \
DIAGNOSTICS_DIR=paper_campaign/diagnostics \
"$SCANNER_RELEASE_BINARY" --duration 691200
```

Keep the requested `PAPER_USE_LIMIT_ORDERS=true` value aligned with the
checked-in/operator profile. Because `LIVE_ORDER_TYPE=fok`, the effective paper
behavior is still market-style FOK; both the requested and effective values are
bound into evidence.

This example runs eight days, allowing a full seven-day evidence span. Paper execution requires complete fresh CLOB quotes, depth-sized fills, supported CLOB fee metadata, and hedged basket parity. Scanner P&L recomputes fees from actual simulated fill prices instead of trusting adapter-reported fees.

Evaluate evidence:

```sh
python3 scripts/paper_profitability_gate.py \
  --trades-csv paper_campaign/diagnostics/trades.csv \
  --attempts-jsonl paper_campaign/diagnostics/paper_execution_attempts.jsonl \
  --output paper_campaign/profitability-report.json
```

Default gate requires:

- at least 100 accepted real scanner paper trades;
- at least 30 distinct events over at least 168 hours;
- evidence no older than 24 hours;
- at least $25 conservative after-cost P&L and 0.25% weighted ROI;
- positive one-sided 95% lower bounds for mean trade P&L and for the mean of per-event mean P&L;
- at least 80% fill success and positive-trade rates;
- an exclusive campaign-account lock plus an exact attempt-ID/status match between terminal trade rows and the v2 `paper_execution_attempts.jsonl` journal, with every started submission reconciled and every accepted fill backed by the submitted broker trade ID and independently recomputed per-leg fees, P&L, and hedge parity;
- a recomputable supported YES-family, NO-family, or binary-bundle payoff certificate and a gas charge no lower than the bound configuration policy; ranked, mint/sell, arbitrary, and test topologies cannot establish profitability;
- no synthetic/canary, partial, parity-failed, non-CLOB, duplicate, malformed, unhedged, or unaccounted execution-error evidence;
- maximum $25 campaign drawdown.

The evaluator snapshots and hashes the exact trades CSV and attempt journal it parses. Thresholds are configurable through the `PAPER_PROFIT_*` variables in `.env.example` for experiments. Lowering them weakens evidence and never satisfies live activation: `verify-readiness-bundle.sh --require-live-ready` independently reruns the gate with fixed conservative activation thresholds.

After the campaign, create the final bundle. It rebuilds twice and requires
every accepted attempt to carry the final copied scanner SHA-256, canonical
`pm-trader` entry-executable SHA-256, one execution-profile hash, and one
profit-compatibility fingerprint. A source, toolchain, adapter, or economic
configuration change invalidates the campaign instead of silently mixing it.
The two isolated release builds use attested stable-rustc path remapping and
must be byte-identical with no temporary build roots embedded. The activation
verifier intentionally binds the binary to this checkout, so the proof covers
fresh build roots on this source tree rather than arbitrary checkout paths:

```sh
FINAL_ROOT="$(mktemp -d /tmp/polymarket-trade-readiness-XXXXXX)"
READINESS_ROOT="$FINAL_ROOT" \
PAPER_PROFITABILITY_TRADES_CSV="$PWD/paper_campaign/diagnostics/trades.csv" \
PAPER_PROFITABILITY_ATTEMPTS_JSONL="$PWD/paper_campaign/diagnostics/paper_execution_attempts.jsonl" \
scripts/trade-readiness.sh --allow-live-blocked

READINESS_MANIFEST="$FINAL_ROOT/readiness-bundle-manifest.json"
SCANNER_RELEASE_BINARY="$(jq -r '.build.binary_path' "$READINESS_MANIFEST")"
export READINESS_MANIFEST SCANNER_RELEASE_BINARY
scripts/verify-readiness-bundle.sh "$READINESS_MANIFEST"
```

## End-to-end readiness

```sh
scripts/trade-readiness.sh --allow-live-blocked
```

To attach a persistent paper campaign:

```sh
PAPER_PROFITABILITY_TRADES_CSV=paper_campaign/diagnostics/trades.csv \
PAPER_PROFITABILITY_ATTEMPTS_JSONL=paper_campaign/diagnostics/paper_execution_attempts.jsonl \
scripts/trade-readiness.sh --allow-live-blocked
```

Expected development result is `live_blocked`: paper operations, HFT, UI, secret scan, no-submit proof, and fail-closed behavior pass while real account/route evidence remains absent.

## Dashboard

```sh
cd dashboard
READINESS_ROOT=/absolute/path/to/a/completed/readiness-run
DIAGNOSTICS_DIR=../runtime_diagnostics \
EXTERNAL_PAPER_DATA_DIR=../paper_campaign/account \
EXTERNAL_PAPER_ACCOUNT=arb-campaign \
SCANNER_RELEASE_BINARY="$(realpath "$READINESS_ROOT/release/polymarket-arb-scanner")" \
SCANNER_READINESS_MANIFEST="$(realpath "$READINESS_ROOT/readiness-bundle-manifest.json")" \
SCANNER_BUILD_PROVENANCE="$(realpath "$READINESS_ROOT/build-provenance.json")" \
npm run dev -- --host 127.0.0.1
```

Open the printed local URL, then use Start explicitly. Readiness GETs never launch a process. The dashboard verifies the complete readiness bundle, matches the effective paper configuration to its campaign profit fingerprint, and hashes the canonical copied release immediately before spawning it; there is no `cargo run` fallback.
Readiness polling is bounded and scan-history aggregation is incremental, including for multi-gigabyte CSV history. Trades and live journals are streamed in full for no-submit proof; unreadable, malformed, truncated, or incomplete evidence blocks that check.
Dashboard mutation APIs require a same-origin browser request; scanner controls and reset actions reject missing, malformed, or cross-origin `Origin` headers.
Dashboard resets cannot erase an active campaign: diagnostics reset is refused after diagnostic rows exist, and paper reset is refused after attempts or trades. Use a new diagnostics directory, paper data directory, and account for a new campaign.

## Live no-submit test

Never start with `cargo run -- --live`. First run real-environment diagnostics without submission:

```sh
OPERATOR_ROOT="$(mktemp -d /tmp/polymarket-live-operator-preflight-XXXXXX)"
LIVE_OPERATOR_PREFLIGHT_ROOT="$OPERATOR_ROOT" \
scripts/live-operator-preflight.sh \
  --readiness-manifest "$READINESS_MANIFEST" \
  --allow-live-blocked

OPERATOR_MANIFEST="$OPERATOR_ROOT/live-operator-preflight-manifest.json"
export OPERATOR_MANIFEST
```

Keep the same private `OPERATOR_ROOT` across repeated no-submit runs while
collecting shadow/replay labels; the named Combo/RFQ activation gate requires
at least 100 recent labeled samples. Regenerate the final operator manifest and
activation packet after the last sample and after any config or credential
identity change.

When every external account, protocol, user-channel, settlement, allowance, closeout, calibration, and profitability gate passes:

```sh
scripts/live-activation-packet.sh \
  --readiness-manifest "$READINESS_MANIFEST" \
  --operator-preflight-manifest "$OPERATOR_MANIFEST" \
  --output-dir /tmp/polymarket-live-activation

LIVE_TRADING_ENABLED=true \
scripts/guarded-live-start.sh \
  --activation-packet /tmp/polymarket-live-activation/live-activation-packet.json \
  --confirm-live --no-paper
```

The guarded launcher never rebuilds with `cargo run`. It executes the exact release binary copied into the readiness bundle after independently checking its build-input provenance and SHA-256. It always passes `--no-paper` to that binary and rejects an extra `--paper`, so ambient paper settings cannot change the verified live process. The binary also recomputes the effective launch-configuration fingerprint; changed settings or credential identities require a new operator preflight and activation packet.

Paper evidence currently exercises the non-atomic `legged_clob_paper` route and
is deliberately recorded as `live_route_compatible=false`. It can establish
conservative paper profitability for a bound economic profile, but it cannot
prove the Combo/RFQ live route profitable. Live activation additionally
requires current authenticated Combo/RFQ finality, settlement, allowance,
closeout, maker, and named replay-calibration evidence. Neither paper nor a
successful activation gate guarantees future profit.

Non-dry-run closeout additionally requires `--live-reconcile-run --confirm-live-closeout` through the same guarded launcher. Closeout can submit irreversible blockchain transactions.

## Quality gates

Local equivalent of CI:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
python3 -m unittest tests/test_paper_profitability_gate.py
for script in scripts/*.sh; do bash -n "$script"; done

cd dashboard
npm audit --audit-level=high
npm run lint
npm run build
```

GitHub Actions also audits Rust dependencies with `cargo audit`.

## Important limits

- Paper fills model execution; they are not exchange fills.
- Build provenance attests the copied scanner bytes and direct `pm-trader` entry executable; transitive Python/venv dependencies and native system libraries remain an explicit trust boundary.
- Confidence statistics assume campaign samples are reasonably representative and independent.
- Fee correctness depends on current authoritative CLOB fee metadata and protocol semantics.
- Multi-leg CLOB execution is non-atomic; partial-fill and orphan exposure must remain fail-closed and visible in evidence.
- Combo/RFQ live promotion requires real authenticated finality, settlement, maker, allowance, and closeout samples.
- Historical diagnostics can show past behavior but cannot establish current market profitability.
- Raw-edge consistency checks validate the scanner's own emitted fields; they are not an independent proof that the detector found every possible opportunity.
