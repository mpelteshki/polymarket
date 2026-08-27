#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/live-ready-gate.sh [--readiness-manifest PATH] [--operator-preflight-manifest PATH] [--require-live-env-enabled] [--json] [--output PATH]

Final no-submit gate before enabling live trading.

It requires:
  - readiness bundle verifier passes with --require-live-ready
  - operator preflight verifier passes with --require-live-ready
  - optional LIVE_TRADING_ENABLED=true when --require-live-env-enabled is set

Use --json for machine-readable gate status. Use --output PATH to also write
that JSON report.

If paths are omitted, latest /tmp/polymarket-trade-readiness-* and
/tmp/polymarket-live-operator-preflight-* manifests are used.
EOF
}

readiness_manifest=""
operator_manifest=""
require_live_env_enabled=0
output_mode="text"
report_output=""

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
    --require-live-env-enabled)
      require_live_env_enabled=1
      shift
      ;;
    --json)
      output_mode="json"
      shift
      ;;
    --output)
      report_output="${2:-}"
      if [[ -z "$report_output" ]]; then
        echo "--output requires a path" >&2
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

need find
need jq
need mktemp
need awk
need sed
need stat
need sort

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readiness_verify_log="$(mktemp "${TMPDIR:-/tmp}/polymarket-readiness-verify.XXXXXX")"
operator_verify_log="$(mktemp "${TMPDIR:-/tmp}/polymarket-operator-verify.XXXXXX")"
gate_report_json="$(mktemp "${TMPDIR:-/tmp}/polymarket-live-ready-gate.XXXXXX")"
readiness_result_fallback="$(mktemp "${TMPDIR:-/tmp}/polymarket-readiness-result.XXXXXX")"
env_audit_fallback="$(mktemp "${TMPDIR:-/tmp}/polymarket-env-audit.XXXXXX")"
cleanup() {
  rm -f "$readiness_verify_log" "$operator_verify_log" "$gate_report_json" "$readiness_result_fallback" "$env_audit_fallback"
}
trap cleanup EXIT
printf '{}\n' >"$readiness_result_fallback"
printf '{"summary":{"blocking_count":0,"ready":false},"blocking":[]}\n' >"$env_audit_fallback"

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

set +e
"$repo_root/scripts/verify-readiness-bundle.sh" --require-live-ready "$readiness_manifest" >"$readiness_verify_log" 2>&1
readiness_rc=$?
"$repo_root/scripts/verify-live-operator-preflight.sh" --require-live-ready "$operator_manifest" >"$operator_verify_log" 2>&1
operator_rc=$?
set -e

live_env_ok=1
if [[ "$require_live_env_enabled" -eq 1 && "${LIVE_TRADING_ENABLED:-false}" != "true" ]]; then
  live_env_ok=0
fi

readiness_state="$(jq -r '.overall_state // "unknown"' "$readiness_manifest")"
operator_live_ready="$(jq -r '.live_ready // false' "$operator_manifest")"
readiness_run="$(jq -r '.run_root // empty' "$readiness_manifest")"
operator_run="$(jq -r '.run_root // empty' "$operator_manifest")"
live_env="${LIVE_TRADING_ENABLED:-false}"
readiness_result="$(jq -r '.result_json // empty' "$readiness_manifest")"
if [[ -z "$readiness_result" || ! -f "$readiness_result" ]]; then
  readiness_result="$readiness_result_fallback"
fi
env_audit_path="$(jq -r '.env_audit.path // empty' "$operator_manifest")"
if [[ -z "$env_audit_path" || ! -f "$env_audit_path" ]]; then
  env_audit_path="$env_audit_fallback"
fi
gate_ok=0
if [[ "$readiness_rc" -eq 0 && "$operator_rc" -eq 0 && "$live_env_ok" -eq 1 ]]; then
  gate_ok=1
fi

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg readiness_manifest "$readiness_manifest" \
  --arg operator_preflight_manifest "$operator_manifest" \
  --arg readiness_run "$readiness_run" \
  --arg operator_run "$operator_run" \
  --arg live_env "$live_env" \
  --argjson gate_ok "$([[ "$gate_ok" -eq 1 ]] && echo true || echo false)" \
  --argjson require_live_env_enabled "$([[ "$require_live_env_enabled" -eq 1 ]] && echo true || echo false)" \
  --argjson live_env_ok "$([[ "$live_env_ok" -eq 1 ]] && echo true || echo false)" \
  --argjson readiness_rc "$readiness_rc" \
  --argjson operator_rc "$operator_rc" \
  --slurpfile readiness_manifest_doc "$readiness_manifest" \
  --slurpfile operator_manifest_doc "$operator_manifest" \
  --slurpfile readiness_result "$readiness_result" \
  --slurpfile env_audit "$env_audit_path" \
  --rawfile readiness_verifier "$readiness_verify_log" \
  --rawfile operator_verifier "$operator_verify_log" \
  '{
    generated_at: $generated_at,
    ok: $gate_ok,
    inputs: {
      readiness_manifest: $readiness_manifest,
      operator_preflight_manifest: $operator_preflight_manifest,
      require_live_env_enabled: $require_live_env_enabled,
      live_env: $live_env
    },
    readiness: {
      rc: $readiness_rc,
      run_root: $readiness_run,
      overall_state: ($readiness_manifest_doc[0].overall_state // "unknown"),
      pass_summary: ($readiness_manifest_doc[0].pass_summary // {}),
      live_unblock: ($readiness_manifest_doc[0].live_unblock // {}),
      live_blocker_count: (($readiness_result[0].checks.live.not_ready_checks // []) | length),
      top_live_blockers: [
        ($readiness_result[0].checks.live.not_ready_checks // [])[:12][]?
        | {
            key: (.key // "check"),
            state: (.state // "unknown"),
            detail: ((.detail // "") | tostring | if length > 260 then .[0:260] + "..." else . end)
          }
      ],
      verifier_output: ($readiness_verifier | split("\n") | map(select(length > 0)))
    },
    operator_preflight: {
      rc: $operator_rc,
      run_root: $operator_run,
      live_ready: ($operator_manifest_doc[0].live_ready // false),
      pass_summary: ($operator_manifest_doc[0].pass_summary // {}),
      env_audit: ($operator_manifest_doc[0].env_audit // {}),
      env_summary: ($env_audit[0].summary // {}),
      env_blockers: ($env_audit[0].blocking // []),
      no_submit_policy: ($operator_manifest_doc[0].no_submit_policy // {}),
      verifier_output: ($operator_verifier | split("\n") | map(select(length > 0)))
    },
    final_live_env: {
      require_live_env_enabled: $require_live_env_enabled,
      ok: $live_env_ok,
      live_trading_enabled: $live_env
    }
  }' >"$gate_report_json"

if [[ -n "$report_output" ]]; then
  cat "$gate_report_json" >"$report_output"
fi

if [[ "$readiness_rc" -ne 0 || "$operator_rc" -ne 0 || "$live_env_ok" -ne 1 ]]; then
  if [[ "$output_mode" == "json" ]]; then
    cat "$gate_report_json"
    exit 1
  fi
  echo "live_ready_gate_failed=1"
  echo "readiness_manifest=$readiness_manifest"
  echo "operator_preflight_manifest=$operator_manifest"
  echo "readiness_state=$readiness_state readiness_rc=$readiness_rc"
  jq -r '
    "readiness_pass_summary=" +
    ([
      "paper=" + ((.pass_summary.paper_ready // false) | tostring),
      "hft=" + ((.pass_summary.hft_ready // false) | tostring),
      "ui=" + ((.pass_summary.ui_ready // false) | tostring),
      "no_live=" + ((.pass_summary.live_no_submission_ok // false) | tostring),
      "global_no_live=" + ((.pass_summary.global_no_live_scan_ok // false) | tostring),
      "code_blockers=" + ((.pass_summary.live_code_blocker_count // "null") | tostring),
      "source_static=" + ((.pass_summary.source_static_blocker_count // "null") | tostring),
      "fail_closed=" + ((.pass_summary.fail_closed_ok // false) | tostring),
      "secret_scan=" + ((.pass_summary.artifact_secret_scan_ok // false) | tostring)
    ] | join(" "))
  ' "$readiness_manifest"
  if [[ "$readiness_result" != "$readiness_result_fallback" ]]; then
    jq -r '
      def short_detail:
        tostring
        | if length > 260 then .[0:260] + "..." else . end;
      "readiness_live_blockers=" + ((.checks.live.not_ready_checks // []) | length | tostring),
      ((.checks.live.not_ready_checks // [])[:8][]? | "  " + (.key // "check") + ": " + (.state // "unknown") + " " + ((.detail // "") | short_detail))
    ' "$readiness_result"
  fi
  if [[ "$readiness_rc" -ne 0 ]]; then
    sed 's/^/readiness_verifier: /' "$readiness_verify_log"
  fi

  echo "operator_live_ready=$operator_live_ready operator_rc=$operator_rc"
  jq -r '
    "operator_pass_summary=" +
    ([
      "cargo=" + ((.pass_summary.cargo_status_ok // false) | tostring),
      "live_ready=" + ((.pass_summary.live_ready // false) | tostring),
      "env_ready=" + ((.pass_summary.env_audit_ready // false) | tostring),
      "env_blocking=" + ((.pass_summary.env_audit_blocking_count // "null") | tostring),
      "no_live=" + ((.pass_summary.no_live_submission_ok // false) | tostring),
      "secret_scan=" + ((.pass_summary.artifact_secret_value_scan_ok // false) | tostring),
      "trade_hits=" + ((.pass_summary.live_trade_row_hits // "null") | tostring),
      "journal_hits=" + ((.pass_summary.combo_execution_journal_hits // "null") | tostring),
      "marker_hits=" + ((.pass_summary.submit_marker_hits // "null") | tostring)
    ] | join(" "))
  ' "$operator_manifest"
  if [[ "$env_audit_path" != "$env_audit_fallback" ]]; then
    jq -r '
      "operator_env_blockers=" + ((.summary.blocking_count // 0) | tostring),
      (.blocking[:12][]? | "  " + .group + ":" + .name + " issue=" + (.issue // "blocked") + " expected=" + .expected)
    ' "$env_audit_path"
  fi
  if [[ "$operator_rc" -ne 0 ]]; then
    sed 's/^/operator_verifier: /' "$operator_verify_log"
  fi
  if [[ "$live_env_ok" -ne 1 ]]; then
    echo "live_env: LIVE_TRADING_ENABLED is not true"
  fi
  echo "readiness_run=$readiness_run"
  echo "operator_run=$operator_run"
  exit 1
fi

if [[ "$output_mode" == "json" ]]; then
  cat "$gate_report_json"
  exit 0
fi

printf 'live_ready_gate_ok=1 readiness_state=%s operator_live_ready=%s live_env=%s readiness_run=%s operator_run=%s\n' \
  "$readiness_state" "$operator_live_ready" "$live_env" "$readiness_run" "$operator_run"
