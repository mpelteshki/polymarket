# Trade Readiness

This repo is ready for paper trading, HFT scanning, and dashboard monitoring when the checks below pass.
Live trading remains blocked until every live readiness check is `ready`.

## One-command verifier

```sh
scripts/trade-readiness.sh
```

This runs Rust tests, dashboard lint/build, paper smoke, an isolated paper execution canary, HFT smoke, live no-submit diagnostics, live code-ceiling diagnostics, a live fail-closed startup guard, a temporary dashboard readiness API check, and a rendered dashboard smoke through `browse`.
Exit code is nonzero until live is ready too.
Before those checks it performs two sequential fresh, isolated, locked release
builds with ambient Cargo/Rust/compiler overrides cleared. Stable rustc path
remapping gives each distinct Cargo home and target directory the same virtual
build root; their bytes must be identical, and neither physical temporary root
may remain in either binary. The exact copied release binary is used for scanner canaries,
paper/HFT smoke, and no-submit diagnostics; readiness does not use `cargo run`
for those paths. Verifier commands explicitly blank live signer, CLOB,
Combo/RFQ, relayer, RPC, webhook, and venue-token env values so `.env` cannot
repopulate them; code-ceiling uses fixed redacted dummy credentials only.
It writes `trade_readiness_result.json` inside the printed `run_root`.
It writes paper proof files: `paper-balance.json`, `paper-history.json`, and `paper-execution-canary.json`.
It also writes UI proof files: `ui-dashboard.png`, `ui-mobile-dashboard.png`, `ui-snapshot.json`, `ui-mobile-snapshot.json`, and `ui-after-pause-snapshot.json`.
It writes `live-diagnostics.log`, `live-code-ceiling.log`, and `live-fail-closed.log`; the fail-closed guard must exit nonzero while live gates are blocked, include an expected preflight/block reason, and stay panic-free.
It writes engine-mode proof files: `engine_mode_report.json`, `engine_mode_state.json`, and `engine_mode_journal.jsonl`; live readiness must use a current clear/normal status-page observation when no CLOB engine blocker is active.
It writes `live-unblock-plan.json`; this is an operator-safe packet with grouped live blockers, env names, and required evidence, without credential values.
It writes `no-live-submission-scan.json` plus hit files; this scans the full `run_root` for any live trade rows, Combo/RFQ execution journals, or submit markers.
It writes `artifact-secret-scan.json` and `artifact-secret-hits.txt`; the scan must have zero hits before any readiness mode can pass.
It writes `build-provenance.json`, the copied `release/polymarket-arb-scanner`,
`paper-adapter-provenance.json`, and `no-live-identity-fingerprint.json`.
Build provenance hashes the source/safety inputs, sanitized build environment,
tool versions, exact ordered remap arguments and physical roots, both build
hashes, path-leak scans, and the copied binary. It explicitly records that the
runtime activation verifier embeds this checkout path, so reproducibility is
proved across fresh build roots, not arbitrary checkout locations. Native compiler/system
libraries and transitive `pm-trader` dependencies are explicitly not fully
attested. It writes `readiness-bundle-manifest.json`; this gives a shareable
no-live proof index with SHA-256 hashes, file sizes, pass summary, dashboard
URL, execution binding, and live unblock counts.
It runs `scripts/verify-readiness-bundle.sh` against that manifest and writes `readiness-bundle-verification.txt`; this independently checks file existence, file sizes, SHA-256 hashes, no-live policy, pass summary, sourced protocol drift evidence, and result JSON consistency.

To verify paper/HFT/UI while still reporting live blockers:

```sh
scripts/trade-readiness.sh --allow-live-blocked
```

To verify the real operator live environment without submitting orders:

```sh
scripts/live-operator-preflight.sh \
  --readiness-manifest /tmp/polymarket-trade-readiness-.../readiness-bundle-manifest.json \
  --allow-live-blocked
```

This verifies and binds the named readiness manifest, its copied release binary,
build provenance, and canonical paper adapter. It uses ambient live env values,
first writes redacted `live-env-audit.json`, forces
`LIVE_TRADING_ENABLED=false`, `LIVE_DIAGNOSTICS_ENABLED=true`, and
`PAPER_TRADING_ENABLED=false`, then runs the exact copied binary from the
repository working directory. Pre/post full launch fingerprints must be
byte-identical. It writes `live-operator-preflight-result.json` plus
`live-operator-preflight-manifest.json`, verifies that manifest, and fails if
the scanner/redactor/tee pipeline fails or any live row, Combo/RFQ execution
journal row, submit marker, panic marker, or raw secret value appears. Omit
`--allow-live-blocked` when expecting all live gates to pass.
Set one private `LIVE_OPERATOR_PREFLIGHT_ROOT` and reuse it for repeated
no-submit runs while accumulating `live_route_shadow_journal.jsonl` and
`live_route_replay_journal.jsonl`; a new default temporary root starts a new
calibration history.
It also writes `live-env-template.sh`, a redacted shell skeleton generated from the same env audit records.
The operator preflight verifier also asserts that `live_readiness_report.json.protocol_drift` contains sourced expected/observed protocol checks, that `live-env-audit.json` contains only status fields, not raw values, and that credential exports in `live-env-template.sh` stay blank with `LIVE_TRADING_ENABLED=false`.

To print the same skeleton without running diagnostics:

```sh
scripts/live-env-audit.sh --template
```

Useful artifact checks:

```sh
jq '.overall_state, .checks' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.static' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.paper.execution_canary' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.protocol' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.unblock_plan.operator_sequence' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.no_live_secret_isolation' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.artifact_secret_scan' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.fail_closed_guard' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.no_submission' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.no_submission.global_scan' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.code_ceiling.code_blockers' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.not_ready_checks[] | select(.state != "ready")' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.checks.live.required_envs, .next_live_actions' /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
jq '.pass_summary, .files[] | {label, exists, size_bytes, sha256}' /tmp/polymarket-trade-readiness-*/readiness-bundle-manifest.json
scripts/verify-readiness-bundle.sh /tmp/polymarket-trade-readiness-*/readiness-bundle-manifest.json
jq '.live_ready, .no_live_submission, .artifact_secret_value_scan' /tmp/polymarket-live-operator-preflight-*/live-operator-preflight-result.json
jq '.env_audit.summary, .env_audit.blocking' /tmp/polymarket-live-operator-preflight-*/live-operator-preflight-result.json
jq '.pass_summary, .files[] | {label, exists, size_bytes, sha256}' /tmp/polymarket-live-operator-preflight-*/live-operator-preflight-manifest.json
scripts/verify-live-operator-preflight.sh /tmp/polymarket-live-operator-preflight-*/live-operator-preflight-manifest.json
scripts/live-ready-gate.sh
scripts/live-ready-gate.sh --json --output /tmp/polymarket-live-ready-gate-report.json
scripts/paper-live-parity-audit.sh --result-json /tmp/polymarket-trade-readiness-*/trade_readiness_result.json --activation-packet /tmp/polymarket-live-activation-packet/live-activation-packet.json --output /tmp/polymarket-paper-live-parity-audit.json
scripts/readiness-verifier-selftest.sh --output /tmp/polymarket-readiness-verifier-selftest-report.json
scripts/live-activation-packet.sh --output-dir /tmp/polymarket-live-activation-packet
scripts/verify-live-activation-packet.sh /tmp/polymarket-live-activation-packet/live-activation-packet.json
scripts/guarded-live-start.sh --activation-packet /tmp/polymarket-live-activation-packet/live-activation-packet.json --confirm-live --no-paper
```

`scripts/live-ready-gate.sh` intentionally runs both required-live verifiers and prints a combined redacted failure summary: readiness pass summary, top live blockers, operator no-submit proof status, and live env audit blockers. It does not submit orders. `--json` emits the same gate status as machine-readable JSON for CI or handoff.

`scripts/paper-live-parity-audit.sh` records separate paper-profitability,
HFT-speed, scanner-decision-path, and live-gate evidence. Paper readiness alone
only proves `pm-trader` is operational. Paper profit requires
`paper-profitability-report.json` to pass its sample, event-diversity,
observation-duration, freshness, conservative after-cost return, drawdown,
hedge-parity, confidence, attempt-reconciliation, producer-binary, adapter, and
configuration-binding checks. Synthetic and canary trades never count.
Fastest-path proof needs warmed WebSocket cache/snapshot evidence plus REST
final-guard evidence. The paper route is `legged_clob_paper` and remains
`live_route_compatible=false`; Combo/RFQ live promotion is a separate proof.
No parity flag promises equal fills or future returns.

The readiness canary runs the exact copied release binary with
`--paper-execution-canary`, so the Rust scanner's external paper adapter places
and records a tiny FOK paper fill in an isolated `pm-trader` account.
`scripts/paper-execution-canary.sh` is a broker-only fallback check. Neither
counts as scanner profitability, and neither touches live trading.

`scripts/readiness-verifier-selftest.sh` exercises verifier failure modes without live trading: baseline proof passes, required-live verifiers and the final gate must match the supplied blocked or live-ready inputs, protocol-source and release-build hash/remap/root/flag/path-scan tampering are rejected, env-template credential tampering is rejected, masked runtime panics, launch-config drift, and underlying operator live-state tampering are rejected, injected activation-packet selftests are rejected by default, activation-packet gate/config/protocol/no-submit/live-start tampering is rejected, and guarded live start is tested only with a deliberately tampered packet so it can never reach `exec`.

`scripts/live-activation-packet.sh` writes a redacted JSON/Markdown handoff packet with proof paths, pass summary, final gate status, protocol sources, release-binary provenance, the redacted launch-config fingerprint, env blockers, readiness blockers, and final required commands. It marks `can_enable_live=true` only when the final live-ready gate passes, records whether the selftest report was generated or injected, then writes `live-activation-packet-verification.txt`.

`scripts/verify-live-activation-packet.sh` independently verifies the packet: referenced proof artifacts exist, readiness/operator verifiers pass, selftest is clean and generated by the packet script, packet summaries match referenced manifests, build and launch-config fingerprints match their hashed artifacts, gate fields match the gate report, final required commands point at the guarded live launcher, and embedded no-submit/protocol evidence is clean. Use `--require-live-ready` before enabling live submit; that path requires statistical paper-profitability evidence, warmed WebSocket/HFT evidence, scanner decision-path parity, clean no-submit proofs, and a live-ready real operator preflight.

`scripts/guarded-live-start.sh` is the only documented live launcher. It
verifies the activation packet with `--require-live-ready`, runs
`live-ready-gate.sh --require-live-env-enabled`, requires
`LIVE_TRADING_ENABLED=true`, and requires explicit `--confirm-live`. It
resolves the exact release executable copied into the verified readiness
bundle, rechecks its SHA-256 against build provenance, fixes the repository
working directory and operator-attested diagnostics/adapter environment, and
executes that binary directly with an unconditional `--no-paper`. It rejects
every extra argument except the exact closeout pair. The Rust process
independently reruns the required-live packet verifier, proves its current
executable path/hash, and rejects full launch-config drift before any
live-capable mode can start.

## Paper

First export the copied binary from a bootstrap readiness manifest:

```sh
READINESS_MANIFEST=/tmp/polymarket-trade-readiness-.../readiness-bundle-manifest.json
SCANNER_RELEASE_BINARY="$(jq -r '.build.binary_path' "$READINESS_MANIFEST")"
export READINESS_MANIFEST SCANNER_RELEASE_BINARY
```

```sh
EXTERNAL_PAPER_DATA_DIR=/tmp/polymarket-paper \
EXTERNAL_PAPER_ACCOUNT=smoke-arb \
"$SCANNER_RELEASE_BINARY" --once
```

Pass criteria:

- `pm-trader` initializes or loads the configured account.
- `checks.paper.balance.total_value` and `checks.paper.balance.pnl` parse from `pm-trader balance`.
- `checks.paper.trade_count` parses from `pm-trader history`.
- `checks.paper.execution_canary.ok=true`, `live_trade_attempted=false`, and `trade_count > 0`.
- No live order submission is attempted.
- Paper stats load through the dashboard readiness API.
- `paper-profitability-report.json.verified_profitable=true` before treating Paper as profitable. `paper_ready=true` means account/tool operational, not profit proven.
- `paper-live-parity-audit.json` must reference the same profitability evidence hash.
- The bundle must hash the immutable trades CSV and `paper_execution_attempts.jsonl` snapshots referenced by the profitability report. Attempt-journal records use schema v2. Every started paper submission needs one terminal journal record and an exact attempt-ID/status match in `trades.csv`; accepted fills also require an exclusive account lock, exact submitted broker IDs, supported recomputable payoff topology, a bound gas-policy floor, and independent per-leg accounting recomputation.
- Custom `PAPER_PROFIT_*` values are useful for analysis only. Weaker values never satisfy live activation; `--require-live-ready` reruns the profitability gate from the bundled snapshots with fixed conservative activation thresholds.

Attach persistent campaign evidence to a readiness run:

```sh
PAPER_PROFITABILITY_TRADES_CSV=paper_campaign/diagnostics/trades.csv \
PAPER_PROFITABILITY_ATTEMPTS_JSONL=paper_campaign/diagnostics/paper_execution_attempts.jsonl \
scripts/trade-readiness.sh --allow-live-blocked
```

## HFT Scan

```sh
USE_WEBSOCKET=true \
WS_INITIAL_SNAPSHOT_TIMEOUT_MS=0 \
DIAGNOSTICS_DIR=/tmp/polymarket-hft \
"$SCANNER_RELEASE_BINARY" --once --no-paper
```

Pass criteria:

- `latency_budget.csv` latest row has `status=ok`.
- `blockers` is empty.
- `scan_duration_ms` stays inside the configured submit budget for the target mode.
- `quote_rest_resolution_pct` is high enough for the configured quote coverage.
- `quote_tokens_unique_selected` is nonzero, selected events/markets were scanned, candidate evaluations were written, and `quote_missing_book_tokens=0`.
- `quote_hard_unresolved_tokens` is investigated when status degrades or blockers appear; no-ask tokens can remain visible in diagnostics without blocking HFT readiness.

## UI

```sh
cd dashboard
DIAGNOSTICS_DIR=/tmp/polymarket-hft \
EXTERNAL_PAPER_DATA_DIR=/tmp/polymarket-paper \
EXTERNAL_PAPER_ACCOUNT=smoke-arb \
npm run dev -- --host 127.0.0.1
```

Pass criteria:

- `GET /api/readiness` returns `Paper`, `HFT`, and `UI` as `ready`.
- `Live submit` is `ready` with zero live rows in visible diagnostics.
- `Live code gates` is `ready`; `blocked` means a local implementation gap remains after env-only gates are forced open in no-submit mode, or source still contains severe placeholders such as `todo!()` / `unimplemented!()`.
- `checks.protocol.state` is `ready`, proving local protocol constants and configured endpoints match the expected Polymarket contract/API surface.
- Protocol drift checks are sourced to official docs: `https://docs.polymarket.com/resources/contracts`, `https://docs.polymarket.com/developers/CLOB/introduction`, and `https://docs.polymarket.com/market-makers/combos`.
- `live_readiness_report.json.protocol_drift` records source URLs, expected values, and observed config/SDK values for these checks.
- Dashboard renders `Trade readiness monitor`.
- Dashboard renders `Proof bundle` after a completed verifier run with `readiness-bundle-manifest.json` present.
- Dashboard renders redacted operator env audit blocker names from `live-env-audit.json` when an operator preflight manifest is present.
- Rendered UI smoke clicks `Pause auto refresh` and verifies the switch becomes checked.
- Desktop `1440x900` and mobile `390x844` layouts render required readiness text with no horizontal overflow.

## Live

Run no-submit diagnostics first:

```sh
LIVE_DIAGNOSTICS_ENABLED=true \
LIVE_TRADING_ENABLED=false \
PAPER_TRADING_ENABLED=false \
DIAGNOSTICS_DIR=/tmp/polymarket-live-diag \
"$SCANNER_RELEASE_BINARY" --live-diagnostics --once --no-paper
```

Run the operator preflight before any live submit:

```sh
scripts/live-operator-preflight.sh --readiness-manifest "$READINESS_MANIFEST"
```

Use `--allow-live-blocked` only to collect redacted no-submit evidence while live blockers remain.

Live pass criteria:

- `live_readiness_report.json` has `live_submissions_supported=true`.
- Every check in `live_readiness_report.json` has `state=ready`.
- `combo_rfq_route_promotion_report.json` has `promoted=true`.
- `live-unblock-plan.json` lists no credential values and shows every remaining external gate mapped to an operator step.
- `checks.live.no_live_secret_isolation.ok=true`, proving no-live verifier commands ran with live credential env values stripped or replaced by redacted dummies.
- Fail-closed guard exits nonzero when live route support is explicitly disabled, with `expected_reason_seen=true` and `panic_free=true`.
- `checks.live.no_submission.ok=true` while running blocked/no-submit verification, with zero live rows/journal rows and `submit_log_markers_seen=0`.
- `checks.live.no_submission.global_scan.ok=true`, proving all `run_root` trade logs, Combo/RFQ execution journals, and submit-marker-bearing artifacts are clean.
- `checks.live.code_ceiling.code_blockers` is empty.
- `checks.artifact_secret_scan.ok=true`, proving generated readiness artifacts did not include raw private keys, the operator email, bearer/token-like values, or secret assignments.
- `readiness-bundle-manifest.json` exists and its file entries have `exists=true` plus SHA-256 hashes for the proof artifacts.
- `scripts/verify-readiness-bundle.sh readiness-bundle-manifest.json` exits 0, proving bundle hashes, no-live summary, and sourced protocol drift evidence still match current files.
- `scripts/verify-readiness-bundle.sh --require-live-ready readiness-bundle-manifest.json` exits 0, proving the safe no-submit bundle has activation-ready paper-profitability, HFT, decision-path, code, and no-submit evidence. Its `overall_state` may remain `live_blocked` because real account credentials are intentionally stripped; operator preflight supplies that separate proof.
- `engine_mode_report.json` has `status=clear`, `state.mode=normal`, and no blockers before live submit is allowed.
- `live-operator-preflight-result.json` has `live_ready=true`, `no_live_submission.ok=true`, and `artifact_secret_value_scan.ok=true` when run with real operator env.
- `live-operator-preflight-result.json` has `env_audit.summary.ready=true` and `env_audit.summary.blocking_count=0`; `live-env-audit.json` must not include secret values, addresses, URLs, or token strings.
- `live-env-template.sh` is present in the operator preflight manifest and every export exactly matches the generated blank, `true`, `false`, or `0` skeleton value; shell expansion and arbitrary values are rejected.
- `live-operator-preflight-manifest.json` has file entries with `exists=true` plus SHA-256 hashes for the operator no-submit proof artifacts.
- `scripts/verify-live-operator-preflight.sh live-operator-preflight-manifest.json` exits 0, proving artifact hashes, no-submit policy, sourced protocol drift evidence, env-audit redaction schema, exact safe template values, and secret-value scan still match current files.
- `scripts/readiness-verifier-selftest.sh` exits 0, proving the readiness/operator verifiers reject tampered protocol-source, credential-template, and shell-expansion artifacts while still accepting the untampered no-submit proof.
- `scripts/paper-live-parity-audit.sh --require-paper-profitable --require-fastest-path` exits 0 on the readiness evidence before live submit is enabled. Real operator readiness and activation integrity are enforced by the final live-ready gate.
- `scripts/live-activation-packet.sh` writes `live-activation-packet.json` with `can_enable_live=true` before live submit is enabled.
- `scripts/verify-live-activation-packet.sh live-activation-packet.json` exits 0, proving the activation packet still matches its referenced readiness/operator/gate/selftest artifacts and final guarded live-start command.
- `scripts/verify-live-operator-preflight.sh --require-live-ready live-operator-preflight-manifest.json` exits 0 before live submit is enabled.
- `scripts/live-ready-gate.sh --readiness-manifest readiness-bundle-manifest.json --operator-preflight-manifest live-operator-preflight-manifest.json` exits 0 before live submit is enabled.
- `scripts/live-ready-gate.sh --json --output live-ready-gate-report.json` has `ok=true` before live submit is enabled.
- `scripts/guarded-live-start.sh --activation-packet live-activation-packet.json --confirm-live --no-paper` is used for live start; it must refuse to run while the packet or gate is not live-ready, and the executed binary must remain paper-disabled regardless of ambient settings.
- Authenticated user-channel status is fresh.
- Combo/RFQ endpoints use current docs defaults unless intentionally overridden: `COMBO_RFQ_API_URL=https://combos-rfq-api.polymarket.sh` and `COMBO_RFQ_GATEWAY_WSS_URL=wss://combos-rfq-gateway-quoter.polymarket.sh/ws/rfq`.
- Live account probes pass with `POLYMARKET_PRIVATE_KEY`.
- Wallet identity is configured: `LIVE_SIGNATURE_TYPE=0` for EOA, `1` for
  Proxy, `2` for Safe, or `3` for Poly1271 deposit wallet. Set
  `LIVE_FUNDER_ADDRESS` for non-EOA wallet types.
- Closeout, finality, allowance, and settlement hazard gates pass.
- `live_route_calibration_report.json` has at least 100 recent labeled Combo/RFQ samples, no blockers, a passing risk gate, one-leg-fill rate at most 0.5%, ghost/revert rate at most 0.1%, realized EV, and a current latest label.
- Combo/RFQ closeout planning must show no open Combo exposure, or
  resolved-winning Combo redeem actions must be executable by the configured
  closeout wallet. `LIVE_SIGNATURE_TYPE=0` EOA closeout uses direct Combo Router
  `redeem(bytes31 conditionId, uint256 outcomeIndex, uint256 amount)` execution
  after PositionManager approval and eth-call preflight. `LIVE_SIGNATURE_TYPE=3`
  Deposit Wallet closeout signs a DepositWallet `Batch`, submits it through
  Relayer `/submit`, polls `STATE_CONFIRMED`, then verifies receipt logs,
  finality, PnL, and exposure release. Proxy/safe closeout remains fail-closed
  until a matching wallet-specific closeout executor is added and verified.

Only then enable live submit:

```sh
scripts/live-ready-gate.sh

LIVE_TRADING_ENABLED=true \
LIVE_COMBO_RFQ_ROUTE_ENABLED=true \
COMBO_RFQ_REQUESTER_ENABLED=true \
COMBO_RFQ_ACCEPT_ENABLED=true \
COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED=true \
LIVE_USER_WS_ENABLED=true \
LIVE_CLOSEOUT_ENABLED=true \
LIVE_CLOSEOUT_DRY_RUN=false \
scripts/guarded-live-start.sh --activation-packet /tmp/polymarket-live-activation-packet/live-activation-packet.json --confirm-live --no-paper
```

Startup must fail closed if any live preflight fails.

Non-dry-run closeout uses the same verified launcher plus a separate irreversible-action confirmation:

```sh
LIVE_TRADING_ENABLED=true \
LIVE_CLOSEOUT_ENABLED=true \
LIVE_CLOSEOUT_DRY_RUN=false \
scripts/guarded-live-start.sh \
  --activation-packet /tmp/polymarket-live-activation-packet/live-activation-packet.json \
  --confirm-live --no-paper -- \
  --live-reconcile-run --confirm-live-closeout
```

Running `--live-reconcile-run` without this guarded path remains read-only unless closeout execution is explicitly enabled; write-capable runs fail closed without both confirmations.
