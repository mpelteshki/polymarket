#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage: scripts/live-activation-packet.sh [--readiness-manifest PATH] [--operator-preflight-manifest PATH] [--output-dir DIR] [--selftest-report PATH]

Builds a redacted live activation packet from verified no-submit artifacts.

The packet runs:
  - scripts/verify-readiness-bundle.sh
  - scripts/verify-live-operator-preflight.sh
  - scripts/readiness-verifier-selftest.sh
  - scripts/live-ready-gate.sh --json
  - scripts/verify-live-activation-packet.sh

It never enables live trading and never submits orders. The packet marks
can_enable_live=true only when the final live-ready gate passes.
EOF
}

readiness_manifest=""
operator_manifest=""
output_dir=""
selftest_report_input=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --readiness-manifest)
      readiness_manifest="${2:-}"
      if [[ -z "$readiness_manifest" ]]; then
        echo "--readiness-manifest requires a path" >&2
        exit 2
      fi
      shift 2
      ;;
    --operator-preflight-manifest)
      operator_manifest="${2:-}"
      if [[ -z "$operator_manifest" ]]; then
        echo "--operator-preflight-manifest requires a path" >&2
        exit 2
      fi
      shift 2
      ;;
    --output-dir)
      output_dir="${2:-}"
      if [[ -z "$output_dir" ]]; then
        echo "--output-dir requires a directory" >&2
        exit 2
      fi
      shift 2
      ;;
    --selftest-report)
      selftest_report_input="${2:-}"
      if [[ -z "$selftest_report_input" ]]; then
        echo "--selftest-report requires a path" >&2
        exit 2
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

need awk
need date
need find
need jq
need mktemp
need sort
need stat

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

latest_manifest() {
  local pattern="$1"
  local found
  found="$(
    find -L /tmp -path "$pattern" -type f -print 2>/dev/null \
      | while IFS= read -r path; do
          printf '%s\t%s\n' "$(stat -f '%m' "$path" 2>/dev/null || stat -c '%Y' "$path")" "$path"
        done \
      | sort -nr \
      | awk -F '\t' 'NR == 1 { print $2 }'
  )"
  if [[ -z "$found" ]]; then
    echo "no manifest found for pattern: $pattern" >&2
    exit 2
  fi
  echo "$found"
}

if [[ -z "$readiness_manifest" ]]; then
  readiness_manifest="$(latest_manifest '/tmp/polymarket-trade-readiness-*/readiness-bundle-manifest.json')"
fi
if [[ -z "$operator_manifest" ]]; then
  operator_manifest="$(latest_manifest '/tmp/polymarket-live-operator-preflight-*/live-operator-preflight-manifest.json')"
fi
if [[ -z "$output_dir" ]]; then
  output_dir="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-live-activation-packet-XXXXXX")"
fi

if [[ ! -f "$readiness_manifest" ]]; then
  echo "missing readiness manifest: $readiness_manifest" >&2
  exit 2
fi
if [[ ! -f "$operator_manifest" ]]; then
  echo "missing operator preflight manifest: $operator_manifest" >&2
  exit 2
fi
if [[ -L "$output_dir" ]]; then
  echo "activation packet output directory must not be a symlink: $output_dir" >&2
  exit 2
fi

readiness_manifest="$(cd "$(dirname "$readiness_manifest")" && pwd)/$(basename "$readiness_manifest")"
operator_manifest="$(cd "$(dirname "$operator_manifest")" && pwd)/$(basename "$operator_manifest")"
mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"
chmod 700 "$output_dir"

readiness_verify_log="$output_dir/readiness-bundle-verification.txt"
operator_verify_log="$output_dir/operator-preflight-verification.txt"
selftest_report="$output_dir/readiness-verifier-selftest-report.json"
selftest_source="generated"
selftest_input_path=""
gate_report="$output_dir/live-ready-gate-report.json"
gate_stdout="$output_dir/live-ready-gate.stdout"
gate_stderr="$output_dir/live-ready-gate.stderr"
packet_json="$output_dir/live-activation-packet.json"
packet_md="$output_dir/live-activation-packet.md"
packet_verify_log="$output_dir/live-activation-packet-verification.txt"

"$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_manifest" >"$readiness_verify_log" 2>&1
"$repo_root/scripts/verify-live-operator-preflight.sh" "$operator_manifest" >"$operator_verify_log" 2>&1
if [[ -n "$selftest_report_input" ]]; then
  if [[ ! -f "$selftest_report_input" ]]; then
    echo "missing selftest report: $selftest_report_input" >&2
    exit 2
  fi
  cp "$selftest_report_input" "$selftest_report"
  selftest_source="provided"
  selftest_input_path="$selftest_report_input"
else
  "$repo_root/scripts/readiness-verifier-selftest.sh" \
    --readiness-manifest "$readiness_manifest" \
    --operator-preflight-manifest "$operator_manifest" \
    --output "$selftest_report" >/dev/null
fi

set +e
"$repo_root/scripts/live-ready-gate.sh" \
  --json \
  --readiness-manifest "$readiness_manifest" \
  --operator-preflight-manifest "$operator_manifest" \
  --output "$gate_report" >"$gate_stdout" 2>"$gate_stderr"
gate_rc=$?
set -e

readiness_result="$(jq -r '.result_json // empty' "$readiness_manifest")"
operator_result="$(jq -r '.result_json // empty' "$operator_manifest")"
env_audit_path="$(jq -r '.env_audit.path // empty' "$operator_manifest")"
env_template_path="$(jq -r '.env_audit.template // empty' "$operator_manifest")"
launch_config_fingerprint_path="$(jq -r '.files[]? | select(.label == "launch_config_fingerprint") | .path' "$operator_manifest" | awk 'NR == 1 { print }')"
live_report_path="$(jq -r '.files[]? | select(.label == "live_readiness_report") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
release_binary_path="$(jq -r '.files[]? | select(.label == "release_binary") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
build_provenance_path="$(jq -r '.files[]? | select(.label == "build_provenance") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"

empty_json="$output_dir/empty.json"
printf '{}\n' >"$empty_json"
if [[ -z "$readiness_result" || ! -f "$readiness_result" ]]; then
  readiness_result="$empty_json"
fi
if [[ -z "$operator_result" || ! -f "$operator_result" ]]; then
  operator_result="$empty_json"
fi
if [[ -z "$env_audit_path" || ! -f "$env_audit_path" ]]; then
  env_audit_path="$empty_json"
fi
if [[ -z "$live_report_path" || ! -f "$live_report_path" ]]; then
  live_report_path="$empty_json"
fi
if [[ -z "$launch_config_fingerprint_path" || ! -f "$launch_config_fingerprint_path" ]]; then
  launch_config_fingerprint_path="$empty_json"
fi
if [[ -z "$build_provenance_path" || ! -f "$build_provenance_path" ]]; then
  build_provenance_path="$empty_json"
fi

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg readiness_manifest "$readiness_manifest" \
  --arg operator_preflight_manifest "$operator_manifest" \
  --arg readiness_verify_log "$readiness_verify_log" \
  --arg operator_verify_log "$operator_verify_log" \
  --arg selftest_report "$selftest_report" \
  --arg selftest_source "$selftest_source" \
  --arg selftest_input_path "$selftest_input_path" \
  --arg gate_report "$gate_report" \
  --arg packet_verify_log "$packet_verify_log" \
  --arg env_template "$env_template_path" \
  --arg launch_config_fingerprint "$launch_config_fingerprint_path" \
  --arg release_binary "$release_binary_path" \
  --arg build_provenance "$build_provenance_path" \
  --arg packet_json "$packet_json" \
  --arg packet_md "$packet_md" \
  --arg output_dir "$output_dir" \
  --argjson gate_rc "$gate_rc" \
  --slurpfile readiness_manifest_doc "$readiness_manifest" \
  --slurpfile operator_manifest_doc "$operator_manifest" \
  --slurpfile readiness_result "$readiness_result" \
  --slurpfile operator_result "$operator_result" \
  --slurpfile env_audit "$env_audit_path" \
  --slurpfile launch_config "$launch_config_fingerprint_path" \
  --slurpfile build "$build_provenance_path" \
  --slurpfile live_report "$live_report_path" \
  --slurpfile gate "$gate_report" \
  --slurpfile selftest "$selftest_report" \
  '{
    generated_at: $generated_at,
    status: (if ($gate[0].ok // false) then "live_ready" else "live_blocked" end),
    can_enable_live: ($gate[0].ok // false),
    no_live_trade_attempted: true,
    output_dir: $output_dir,
    selftest: {
      source: $selftest_source,
      input_path: (if $selftest_input_path == "" then null else $selftest_input_path end)
    },
    artifacts: {
      packet_json: $packet_json,
      packet_markdown: $packet_md,
      readiness_manifest: $readiness_manifest,
      operator_preflight_manifest: $operator_preflight_manifest,
      readiness_verification: $readiness_verify_log,
      operator_preflight_verification: $operator_verify_log,
      verifier_selftest_report: $selftest_report,
      live_ready_gate_report: $gate_report,
      activation_packet_verification: $packet_verify_log,
      live_env_template: $env_template,
      launch_config_fingerprint: $launch_config_fingerprint,
      release_binary: $release_binary,
      build_provenance: $build_provenance
    },
    launch_config: ($launch_config[0] // {}),
    build: ($build[0] // {}),
    paper_execution_binding: ($readiness_manifest_doc[0].paper_execution_binding // {}),
    gate: {
      rc: $gate_rc,
      ok: ($gate[0].ok // false),
      readiness_state: ($gate[0].readiness.overall_state // "unknown"),
      readiness_blockers: ($gate[0].readiness.live_blocker_count // null),
      operator_live_ready: ($gate[0].operator_preflight.live_ready // false),
      operator_env_ready: ($gate[0].operator_preflight.env_summary.ready // false),
      operator_env_blockers: ($gate[0].operator_preflight.env_summary.blocking_count // null),
      live_trading_enabled: ($gate[0].final_live_env.live_trading_enabled // "false")
    },
    pass_summary: {
      readiness: ($readiness_manifest_doc[0].pass_summary // {}),
      operator_preflight: ($operator_manifest_doc[0].pass_summary // {}),
      selftest_ok: ($selftest[0].ok // false)
    },
    protocol_drift: ($live_report[0].protocol_drift // {}),
    no_submit: {
      readiness_global_scan: ($readiness_result[0].checks.live.no_submission.global_scan // {}),
      operator_no_live_submission: ($operator_result[0].no_live_submission // {}),
      artifact_secret_scan: {
        readiness: ($readiness_result[0].checks.artifact_secret_scan // {}),
        operator: ($operator_result[0].artifact_secret_value_scan // {})
      }
    },
    blockers: {
      env_summary: ($env_audit[0].summary // {}),
      env_blockers: ($env_audit[0].blocking // []),
      top_readiness_blockers: [
        ($readiness_result[0].checks.live.not_ready_checks // [])[:12][]?
        | {
            key: (.key // "check"),
            state: (.state // "unknown"),
            detail: ((.detail // "") | tostring | if length > 320 then .[0:320] + "..." else . end)
          }
      ],
      required_envs: ($readiness_result[0].checks.live.required_envs // [])
    },
    operator_sequence: ($readiness_result[0].checks.live.unblock_plan.operator_sequence // []),
    final_required_commands: {
      verifier_selftest: ("scripts/readiness-verifier-selftest.sh --readiness-manifest " + $readiness_manifest + " --operator-preflight-manifest " + $operator_preflight_manifest),
      readiness_live_ready: ("scripts/verify-readiness-bundle.sh --require-live-ready " + $readiness_manifest),
      operator_live_ready: ("scripts/verify-live-operator-preflight.sh --require-live-ready " + $operator_preflight_manifest),
      live_ready_gate: ("scripts/live-ready-gate.sh --require-live-env-enabled --readiness-manifest " + $readiness_manifest + " --operator-preflight-manifest " + $operator_preflight_manifest),
      live_start: ("LIVE_TRADING_ENABLED=true scripts/guarded-live-start.sh --activation-packet " + $packet_json + " --confirm-live --no-paper")
    }
  }' >"$packet_json"

jq -r '
  def yn: if . then "true" else "false" end;
  [
    "# Polymarket Live Activation Packet",
    "",
    "- generated_at: " + .generated_at,
    "- status: " + .status,
    "- can_enable_live: " + (.can_enable_live | yn),
    "- no_live_trade_attempted: " + (.no_live_trade_attempted | yn),
    "",
    "## Proof Artifacts",
    "",
    "- readiness_manifest: `" + .artifacts.readiness_manifest + "`",
    "- operator_preflight_manifest: `" + .artifacts.operator_preflight_manifest + "`",
    "- verifier_selftest_report: `" + .artifacts.verifier_selftest_report + "`",
    "- live_ready_gate_report: `" + .artifacts.live_ready_gate_report + "`",
    "- activation_packet_verification: `" + .artifacts.activation_packet_verification + "`",
    "- live_env_template: `" + (.artifacts.live_env_template // "") + "`",
    "",
    "## Current Pass Summary",
    "",
    "- paper_ready: " + ((.pass_summary.readiness.paper_ready // false) | yn),
    "- paper_execution_canary_ok: " + ((.pass_summary.readiness.paper_execution_canary_ok // false) | yn),
    "- paper_scanner_trade_proof_ok: " + ((.pass_summary.readiness.paper_scanner_trade_proof_ok // false) | yn),
    "- paper_live_decision_path_parity_ok: " + ((.pass_summary.readiness.paper_live_decision_path_parity_ok // false) | yn),
    "- hft_ready: " + ((.pass_summary.readiness.hft_ready // false) | yn),
    "- ui_ready: " + ((.pass_summary.readiness.ui_ready // false) | yn),
    "- no_live_submission_ok: " + ((.pass_summary.readiness.global_no_live_scan_ok // false) | yn),
    "- live_code_blocker_count: " + ((.pass_summary.readiness.live_code_blocker_count // null) | tostring),
    "- operator_no_submit_ok: " + ((.pass_summary.operator_preflight.no_live_submission_ok // false) | yn),
    "- selftest_ok: " + (.pass_summary.selftest_ok | yn),
    "",
    "## Live Gate",
    "",
    "- gate_rc: " + (.gate.rc | tostring),
    "- gate_ok: " + (.gate.ok | yn),
    "- readiness_state: " + (.gate.readiness_state | tostring),
    "- readiness_blockers: " + (.gate.readiness_blockers | tostring),
    "- operator_env_blockers: " + (.gate.operator_env_blockers | tostring),
    "- LIVE_TRADING_ENABLED: " + (.gate.live_trading_enabled | tostring),
    "",
    "## Protocol Drift",
    "",
    "- status: " + (.protocol_drift.status // "unknown"),
    "- sources: " + ((.protocol_drift.source_urls // []) | join(", ")),
    "",
    "## Top Env Blockers",
    "",
    ((.blockers.env_blockers // [])[:12] | map("- `" + .group + ":" + .name + "` issue=" + (.issue // "blocked") + " expected=" + .expected) | join("\n")),
    "",
    "## Top Readiness Blockers",
    "",
    ((.blockers.top_readiness_blockers // [])[:8] | map("- `" + .key + "` " + .state + ": " + .detail) | join("\n")),
    "",
    "## Required Final Commands",
    "",
    "```sh",
    .final_required_commands.verifier_selftest,
    .final_required_commands.readiness_live_ready,
    .final_required_commands.operator_live_ready,
    .final_required_commands.live_ready_gate,
    .final_required_commands.live_start,
    "```"
  ] | .[]
' "$packet_json" >"$packet_md"

if [[ "$selftest_source" == "provided" ]]; then
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$packet_json" >"$packet_verify_log" 2>&1
else
  "$repo_root/scripts/verify-live-activation-packet.sh" "$packet_json" >"$packet_verify_log" 2>&1
fi

printf 'live_activation_packet_ok=1 can_enable_live=%s status=%s output_dir=%s packet_json=%s packet_md=%s\n' \
  "$(jq -r '.can_enable_live' "$packet_json")" \
  "$(jq -r '.status' "$packet_json")" \
  "$output_dir" \
  "$packet_json" \
  "$packet_md"
