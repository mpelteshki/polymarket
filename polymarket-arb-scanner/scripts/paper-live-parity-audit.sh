#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/paper-live-parity-audit.sh [--result-json PATH] [--profitability-report PATH] [--latency-csv PATH] [--scan-summary PATH] [--activation-packet PATH|--no-activation-packet] [--output PATH] [--require-paper-profitable] [--require-live-identical] [--require-fastest-path]

Writes machine-readable proof for:
  - paper operational readiness
  - paper profitability evidence
  - paper/live behavior parity
  - WebSocket/HFT speed evidence

Normal mode exits 0 and records blockers. Require flags make missing proof fail.
EOF
}

result_json=""
profitability_report=""
latency_csv=""
scan_summary=""
activation_packet=""
output=""
no_activation_packet=0
require_paper_profitable=0
require_live_identical=0
require_fastest_path=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --result-json)
      result_json="${2:-}"
      shift 2
      ;;
    --profitability-report)
      profitability_report="${2:-}"
      shift 2
      ;;
    --latency-csv)
      latency_csv="${2:-}"
      shift 2
      ;;
    --scan-summary)
      scan_summary="${2:-}"
      shift 2
      ;;
    --activation-packet)
      activation_packet="${2:-}"
      shift 2
      ;;
    --no-activation-packet)
      no_activation_packet=1
      activation_packet=""
      shift
      ;;
    --output)
      output="${2:-}"
      shift 2
      ;;
    --require-paper-profitable)
      require_paper_profitable=1
      shift
      ;;
    --require-live-identical)
      require_live_identical=1
      shift
      ;;
    --require-fastest-path)
      require_fastest_path=1
      shift
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
need python3
need sort
need stat

latest_file() {
  local pattern="$1"
  find -L /tmp -path "$pattern" -type f -print 2>/dev/null \
    | while IFS= read -r path; do
        printf '%s\t%s\n' "$(stat -f '%m' "$path" 2>/dev/null || stat -c '%Y' "$path")" "$path"
      done \
    | sort -nr \
    | awk -F '\t' 'NR == 1 { print $2 }'
}

if [[ -z "$result_json" ]]; then
  result_json="$(latest_file '/tmp/polymarket-trade-readiness-*/trade_readiness_result.json')"
fi
if [[ -z "$activation_packet" && "$no_activation_packet" -ne 1 ]]; then
  activation_packet="$(latest_file '/tmp/polymarket-live-activation-packet-*/live-activation-packet.json')"
fi
if [[ -z "$output" ]]; then
  output="${TMPDIR:-/tmp}/polymarket-paper-live-parity-audit-$(date +%s).json"
fi

if [[ -z "$result_json" || ! -f "$result_json" ]]; then
  echo "missing result json: ${result_json:-<empty>}" >&2
  exit 2
fi

if [[ -z "$latency_csv" ]]; then
  latency_csv="$(jq -r '.evidence.hft_dir // empty' "$result_json")/latency_budget.csv"
fi
if [[ -z "$scan_summary" ]]; then
  scan_summary="$(jq -r '.evidence.hft_dir // empty' "$result_json")/scan_summary.csv"
fi
if [[ -z "$profitability_report" ]]; then
  profitability_report="$(dirname "$result_json")/paper-profitability-report.json"
fi
if [[ ! -f "$profitability_report" ]]; then
  echo "missing paper profitability report: $profitability_report" >&2
  exit 2
fi

if [[ -z "$activation_packet" || ! -f "$activation_packet" ]]; then
  activation_packet=""
fi

csv_last_value() {
  local file="$1"
  local key="$2"
  if [[ ! -f "$file" ]]; then
    return 0
  fi
  python3 - "$file" "$key" <<'PY'
import csv
import sys

path, key = sys.argv[1], sys.argv[2]
value = ""
with open(path, newline="") as handle:
    for row in csv.DictReader(handle):
        if any((cell or "").strip() for cell in row.values()):
            value = row.get(key, "") or ""
print(value)
PY
}

json_bool() {
  if [[ "$1" -eq 1 ]]; then
    echo true
  else
    echo false
  fi
}

num_gt() {
  local value="${1:-}"
  local threshold="${2:-0}"
  awk -v v="$value" -v t="$threshold" 'BEGIN { print ((v + 0) > (t + 0)) ? 1 : 0 }'
}

paper_ready="$(jq -r '.checks.paper.ready // false' "$result_json")"
paper_trade_count="$(jq -r '.checks.paper.trade_count // 0' "$result_json")"
paper_pnl="$(jq -r '.checks.paper.balance.pnl // 0' "$result_json")"
paper_total_value="$(jq -r '.checks.paper.balance.total_value // 0' "$result_json")"
scanner_trade_proof_ok="$(jq -r '.checks.paper.scanner_trade_proof.ok // false' "$result_json")"
scanner_trade_proof_synthetic="$(
  jq -r '.checks.paper.scanner_trade_proof | if has("synthetic") then .synthetic else true end' "$result_json"
)"
scanner_trade_proof_profit_counted="$(
  jq -r '.checks.paper.scanner_trade_proof | if has("counts_for_profitability") then .counts_for_profitability else true end' "$result_json"
)"
scanner_trade_proof_live_attempted="$(
  jq -r '.checks.paper.scanner_trade_proof | if has("live_trade_attempted") then .live_trade_attempted else true end' "$result_json"
)"
scanner_trade_proof_paper_ok_rows="$(jq -r '.checks.paper.scanner_trade_proof.paper_ok_rows // 0' "$result_json")"
scanner_trade_proof_trades_csv="$(jq -r '.checks.paper.scanner_trade_proof.trades_csv // empty' "$result_json")"
scanner_trade_proof_plan_hash="$(jq -r '.checks.paper.scanner_trade_proof.synthetic_plan_hash // empty' "$result_json")"
scanner_trade_proof_plan_hash_algorithm="$(jq -r '.checks.paper.scanner_trade_proof.synthetic_plan_hash_algorithm // empty' "$result_json")"
scanner_trade_proof_decision_parity_ok="$(jq -r '.checks.paper.scanner_trade_proof.decision_path_parity.ok // false' "$result_json")"
scanner_trade_proof_paper_decision_hash="$(jq -r '.checks.paper.scanner_trade_proof.decision_path_parity.paper_decision_hash // empty' "$result_json")"
scanner_trade_proof_live_decision_hash="$(jq -r '.checks.paper.scanner_trade_proof.decision_path_parity.live_decision_hash // empty' "$result_json")"
scanner_trade_proof_decision_hash_algorithm="$(jq -r '.checks.paper.scanner_trade_proof.decision_path_parity.hash_algorithm // empty' "$result_json")"
paper_operational=0
[[ "$paper_ready" == "true" ]] && paper_operational=1
paper_profitable=0
profitability_verified="$(jq -r '.verified_profitable // false' "$profitability_report")"
[[ "$profitability_verified" == "true" ]] && paper_profitable=1
scanner_paper_path=0
if [[ "$scanner_trade_proof_ok" == "true" \
  && "$scanner_trade_proof_synthetic" == "true" \
  && "$scanner_trade_proof_profit_counted" == "false" \
  && "$scanner_trade_proof_live_attempted" == "false" \
  && "$scanner_trade_proof_paper_ok_rows" -gt 0 ]]; then
  scanner_paper_path=1
fi
scanner_decision_path=0
if [[ "$scanner_paper_path" -eq 1 \
  && "$scanner_trade_proof_decision_parity_ok" == "true" \
  && "$scanner_trade_proof_decision_hash_algorithm" == "fnv1a64" \
  && -n "$scanner_trade_proof_paper_decision_hash" \
  && "$scanner_trade_proof_paper_decision_hash" == "$scanner_trade_proof_live_decision_hash" ]]; then
  scanner_decision_path=1
fi

hft_latency_ms="$(jq -r '.checks.hft.latency_ms // null' "$result_json")"
quote_rest_requested="$(csv_last_value "$latency_csv" quote_rest_requested)"
quote_rest_resolved="$(csv_last_value "$latency_csv" quote_rest_resolved)"
quote_cache_hits="$(csv_last_value "$latency_csv" quote_cache_hits)"
ws_snapshot_wait_ms="$(csv_last_value "$latency_csv" ws_snapshot_wait_ms)"
ws_snapshot_ready_tokens="$(csv_last_value "$latency_csv" ws_snapshot_ready_tokens)"
ws_snapshot_total_tokens="$(csv_last_value "$latency_csv" ws_snapshot_total_tokens)"
ws_snapshot_min_ready_tokens="$(csv_last_value "$latency_csv" ws_snapshot_min_ready_tokens)"
ws_snapshot_satisfied="$(csv_last_value "$latency_csv" ws_snapshot_satisfied)"
latency_history_json="$(
  PARITY_WS_RECENT_WINDOW_ROWS="${PARITY_WS_RECENT_WINDOW_ROWS:-50}" python3 - "$latency_csv" <<'PY'
import csv
import json
import os
import sys
from collections import deque

path = sys.argv[1]
try:
    window_limit = max(1, int(os.environ.get("PARITY_WS_RECENT_WINDOW_ROWS", "50")))
except ValueError:
    window_limit = 50

def number(row, key):
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0

def integer(row, key):
    return int(number(row, key))

row_count = 0
recent = deque(maxlen=window_limit)
try:
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            if not any((cell or "").strip() for cell in row.values()):
                continue
            row_count += 1
            recent.append({
                "scan_id": integer(row, "scan_id"),
                "status": row.get("status") or "",
                "scan_duration_ms": number(row, "scan_duration_ms"),
                "quote_cache_hits": integer(row, "quote_cache_hits"),
                "quote_rest_requested": integer(row, "quote_rest_requested"),
                "quote_rest_resolved": integer(row, "quote_rest_resolved"),
                "ws_snapshot_wait_ms": number(row, "ws_snapshot_wait_ms"),
                "ws_snapshot_ready_tokens": integer(row, "ws_snapshot_ready_tokens"),
                "ws_snapshot_total_tokens": integer(row, "ws_snapshot_total_tokens"),
                "ws_snapshot_min_ready_tokens": integer(row, "ws_snapshot_min_ready_tokens"),
                "ws_snapshot_satisfied": (row.get("ws_snapshot_satisfied") or "").lower() == "true",
            })
except FileNotFoundError:
    pass

recent_ws = [
    row for row in recent
    if row["ws_snapshot_satisfied"]
    and row["ws_snapshot_total_tokens"] > 0
    and row["ws_snapshot_ready_tokens"] >= row["ws_snapshot_min_ready_tokens"]
]

print(json.dumps({
    "rows": row_count,
    "recent_window_limit": window_limit,
    "recent_window_rows": len(recent),
    "recent_ws_snapshot_satisfied_rows": len(recent_ws),
    "recent_ws_snapshot_satisfied": len(recent_ws) > 0,
    "latest": recent[-1] if recent else None,
    "latest_satisfied_ws_snapshot": recent_ws[-1] if recent_ws else None,
}))
PY
)"
recent_ws_snapshot_satisfied="$(
  jq -r 'if .recent_ws_snapshot_satisfied then 1 else 0 end' <<<"$latency_history_json"
)"
latency_history_rows="$(
  jq -r '.rows // 0' <<<"$latency_history_json"
)"
opportunities_found="$(csv_last_value "$scan_summary" opportunities_found)"
cumulative_pnl_usd="$(csv_last_value "$scan_summary" cumulative_pnl_usd)"
raw_yes_candidates="$(csv_last_value "$scan_summary" raw_yes_candidates)"
raw_no_candidates="$(csv_last_value "$scan_summary" raw_no_candidates)"
raw_bundle_candidates="$(csv_last_value "$scan_summary" raw_bundle_candidates)"
raw_ranked_candidates="$(csv_last_value "$scan_summary" raw_ranked_candidates)"
best_raw_edge_type="$(csv_last_value "$scan_summary" best_raw_edge_type)"
best_raw_edge_event_id="$(csv_last_value "$scan_summary" best_raw_edge_event_id)"
best_raw_edge_event_title="$(csv_last_value "$scan_summary" best_raw_edge_event_title)"
best_raw_edge_net_profit="$(csv_last_value "$scan_summary" best_raw_edge_net_profit)"
best_raw_edge_roi_pct="$(csv_last_value "$scan_summary" best_raw_edge_roi_pct)"
best_raw_edge_cost="$(csv_last_value "$scan_summary" best_raw_edge_cost)"
best_raw_edge_revenue="$(csv_last_value "$scan_summary" best_raw_edge_revenue)"
raw_candidate_total="$(
  awk \
    -v yes="${raw_yes_candidates:-0}" \
    -v no="${raw_no_candidates:-0}" \
    -v bundle="${raw_bundle_candidates:-0}" \
    -v ranked="${raw_ranked_candidates:-0}" \
    'BEGIN { print (yes + 0) + (no + 0) + (bundle + 0) + (ranked + 0) }'
)"
raw_edge_history_json="$(
  python3 - "$scan_summary" <<'PY'
import csv
import json
import sys

path = sys.argv[1]

def number(row, key):
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0

def integer(row, key):
    return int(number(row, key))

scan_rows = 0
positive_rows = 0
max_edge = None
missed_count = 0
first_missed = None
try:
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            if not any((cell or "").strip() for cell in row.values()):
                continue
            raw_total = sum(
                integer(row, key)
                for key in (
                    "raw_yes_candidates",
                    "raw_no_candidates",
                    "raw_bundle_candidates",
                    "raw_ranked_candidates",
                )
            )
            edge = {
                "scan_id": integer(row, "scan_id"),
                "type": row.get("best_raw_edge_type") or None,
                "event_id": row.get("best_raw_edge_event_id") or None,
                "event_title": row.get("best_raw_edge_event_title") or None,
                "cost": number(row, "best_raw_edge_cost"),
                "revenue": number(row, "best_raw_edge_revenue"),
                "net_profit": number(row, "best_raw_edge_net_profit"),
                "roi_pct": number(row, "best_raw_edge_roi_pct"),
                "raw_candidate_total": raw_total,
                "opportunities_found": integer(row, "opportunities_found"),
            }
            scan_rows += 1
            if max_edge is None or edge["net_profit"] > max_edge["net_profit"]:
                max_edge = edge
            if edge["net_profit"] > 0:
                positive_rows += 1
            if (
                edge["net_profit"] > 0
                and edge["raw_candidate_total"] == 0
                and edge["opportunities_found"] == 0
            ):
                missed_count += 1
                if first_missed is None:
                    first_missed = edge
except FileNotFoundError:
    pass

print(json.dumps({
    "scan_rows": scan_rows,
    "max_best_raw_edge": max_edge,
    "positive_best_raw_edge_rows": positive_rows,
    "missed_positive_raw_edge_rows": missed_count,
    "first_missed_positive_raw_edge": first_missed,
    "no_missed_positive_raw_edge": missed_count == 0,
    "scope": "scanner_reported_best_edge_internal_consistency",
    "proves_detector_completeness": False,
}))
PY
)"
missed_positive_edge_guard="$(jq -r 'if .no_missed_positive_raw_edge then 1 else 0 end' <<<"$raw_edge_history_json")"

fastest_path=0
if [[ "$(num_gt "$latency_history_rows" 0)" -eq 1 \
  && "$(num_gt "$quote_cache_hits" 0)" -eq 1 \
  && "$recent_ws_snapshot_satisfied" -eq 1 ]]; then
  fastest_path=1
fi

final_rest_guard=0
if [[ "$(num_gt "$quote_rest_requested" 0)" -eq 1 && "$(num_gt "$quote_rest_resolved" 0)" -eq 1 ]]; then
  final_rest_guard=1
fi

can_enable_live=false
live_gate_ok=false
live_status="unknown"
live_readiness_blockers="$(jq -r '.checks.live.not_ready_checks | length // 0' "$result_json")"
live_no_submit_ok="$(jq -r '.checks.live.no_submission.ok // false' "$result_json")"
live_fail_closed_ok="$(jq -r '.checks.live.fail_closed_guard.ok // false' "$result_json")"
live_no_submit_guard=0
if [[ "$live_no_submit_ok" == "true" && "$live_fail_closed_ok" == "true" ]]; then
  live_no_submit_guard=1
fi
if [[ -n "$activation_packet" ]]; then
  can_enable_live="$(jq -r '.can_enable_live // false' "$activation_packet")"
  live_gate_ok="$(jq -r '.gate.ok // false' "$activation_packet")"
  live_status="$(jq -r '.status // "unknown"' "$activation_packet")"
  live_readiness_blockers="$(jq -r '.gate.readiness_blockers // 0' "$activation_packet")"
fi

live_identical=0
if [[ "$can_enable_live" == "true" && "$live_gate_ok" == "true" && "$paper_profitable" -eq 1 && "$fastest_path" -eq 1 && "$scanner_decision_path" -eq 1 ]]; then
  live_identical=1
fi

ok=0
if [[ "$paper_operational" -eq 1 && "$paper_profitable" -eq 1 && "$live_identical" -eq 1 && "$fastest_path" -eq 1 ]]; then
  ok=1
fi

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg result_json "$result_json" \
  --arg profitability_report "$profitability_report" \
  --arg activation_packet "$activation_packet" \
  --arg latency_csv "$latency_csv" \
  --arg scan_summary "$scan_summary" \
  --arg paper_trade_count "$paper_trade_count" \
  --arg paper_pnl "$paper_pnl" \
  --arg paper_total_value "$paper_total_value" \
  --arg scanner_trade_proof_paper_ok_rows "$scanner_trade_proof_paper_ok_rows" \
  --arg scanner_trade_proof_trades_csv "$scanner_trade_proof_trades_csv" \
  --arg scanner_trade_proof_plan_hash "$scanner_trade_proof_plan_hash" \
  --arg scanner_trade_proof_plan_hash_algorithm "$scanner_trade_proof_plan_hash_algorithm" \
  --arg scanner_trade_proof_paper_decision_hash "$scanner_trade_proof_paper_decision_hash" \
  --arg scanner_trade_proof_live_decision_hash "$scanner_trade_proof_live_decision_hash" \
  --arg scanner_trade_proof_decision_hash_algorithm "$scanner_trade_proof_decision_hash_algorithm" \
  --arg hft_latency_ms "$hft_latency_ms" \
  --arg quote_rest_requested "${quote_rest_requested:-0}" \
  --arg quote_rest_resolved "${quote_rest_resolved:-0}" \
  --arg quote_cache_hits "${quote_cache_hits:-0}" \
  --arg ws_snapshot_wait_ms "${ws_snapshot_wait_ms:-0}" \
  --arg ws_snapshot_ready_tokens "${ws_snapshot_ready_tokens:-0}" \
  --arg ws_snapshot_total_tokens "${ws_snapshot_total_tokens:-0}" \
  --arg ws_snapshot_min_ready_tokens "${ws_snapshot_min_ready_tokens:-0}" \
  --arg ws_snapshot_satisfied "${ws_snapshot_satisfied:-false}" \
  --argjson latency_history "$latency_history_json" \
  --arg opportunities_found "${opportunities_found:-0}" \
  --arg cumulative_pnl_usd "${cumulative_pnl_usd:-0}" \
  --arg raw_candidate_total "${raw_candidate_total:-0}" \
  --arg best_raw_edge_type "$best_raw_edge_type" \
  --arg best_raw_edge_event_id "$best_raw_edge_event_id" \
  --arg best_raw_edge_event_title "$best_raw_edge_event_title" \
  --arg best_raw_edge_net_profit "${best_raw_edge_net_profit:-0}" \
  --arg best_raw_edge_roi_pct "${best_raw_edge_roi_pct:-0}" \
  --arg best_raw_edge_cost "${best_raw_edge_cost:-0}" \
  --arg best_raw_edge_revenue "${best_raw_edge_revenue:-0}" \
  --arg live_status "$live_status" \
  --arg live_readiness_blockers "$live_readiness_blockers" \
  --argjson raw_edge_history "$raw_edge_history_json" \
  --slurpfile profitability "$profitability_report" \
  --argjson ok "$(json_bool "$ok")" \
  --argjson paper_operational "$(json_bool "$paper_operational")" \
  --argjson paper_profitable "$(json_bool "$paper_profitable")" \
  --argjson scanner_paper_path "$(json_bool "$scanner_paper_path")" \
  --argjson scanner_decision_path "$(json_bool "$scanner_decision_path")" \
  --argjson missed_positive_edge_guard "$(json_bool "$missed_positive_edge_guard")" \
  --argjson fastest_path "$(json_bool "$fastest_path")" \
  --argjson final_rest_guard "$(json_bool "$final_rest_guard")" \
  --argjson live_no_submit_guard "$(json_bool "$live_no_submit_guard")" \
  --argjson live_identical "$(json_bool "$live_identical")" \
  --argjson can_enable_live "$can_enable_live" \
  --argjson live_gate_ok "$live_gate_ok" \
  '
  def n($x): ($x | tonumber? // 0);
  def blocker($key; $detail): {key: $key, detail: $detail};
  {
    generated_at: $generated_at,
    ok: $ok,
    limitations: [
      "raw-edge consistency uses scanner-emitted fields and does not independently prove detector completeness",
      "paper/live decision-path parity does not prove equal fills or future profitability"
    ],
    verdict: {
      paper_operational: $paper_operational,
      scanner_paper_execution_path_proven: $scanner_paper_path,
      scanner_live_decision_path_parity_proven: $scanner_decision_path,
      scanner_no_missed_positive_raw_edge: $missed_positive_edge_guard,
      paper_profitable_proven: $paper_profitable,
      hft_fastest_path_proven: $fastest_path,
      final_rest_guard_seen: $final_rest_guard,
      live_no_submit_guard_proven: $live_no_submit_guard,
      live_can_enable: $can_enable_live,
      live_gate_ok: $live_gate_ok,
      paper_live_identical: $live_identical
    },
    paper: {
      ready: $paper_operational,
      trade_count: n($paper_trade_count),
      pnl: n($paper_pnl),
      total_value: n($paper_total_value),
      profitability_evidence: ($profitability[0] // {}),
      scanner_trade_proof: {
        ok: $scanner_paper_path,
        synthetic: true,
        counts_for_profitability: false,
        synthetic_plan_hash: (if $scanner_trade_proof_plan_hash == "" then null else $scanner_trade_proof_plan_hash end),
        synthetic_plan_hash_algorithm: (if $scanner_trade_proof_plan_hash_algorithm == "" then null else $scanner_trade_proof_plan_hash_algorithm end),
        decision_path_parity: {
          ok: $scanner_decision_path,
          paper_decision_hash: (if $scanner_trade_proof_paper_decision_hash == "" then null else $scanner_trade_proof_paper_decision_hash end),
          live_decision_hash: (if $scanner_trade_proof_live_decision_hash == "" then null else $scanner_trade_proof_live_decision_hash end),
          hash_algorithm: (if $scanner_trade_proof_decision_hash_algorithm == "" then null else $scanner_trade_proof_decision_hash_algorithm end),
          live_submit_attempted: false,
          note: "scanner opportunity legs match before paper/live execution adapter boundary"
        },
        paper_ok_rows: n($scanner_trade_proof_paper_ok_rows),
        trades_csv: (if $scanner_trade_proof_trades_csv == "" then null else $scanner_trade_proof_trades_csv end)
      },
      profitable_proven: $paper_profitable,
      note: (
        if $paper_profitable then
          "real scanner paper fills passed minimum sample, diversity, duration, freshness, after-cost return, drawdown, and confidence gates"
        else
          "paper operational only; profitability evidence gate is blocked"
        end
      )
    },
    speed: {
      hft_latency_ms: n($hft_latency_ms),
      websocket_cache_hits: n($quote_cache_hits),
      ws_snapshot_wait_ms: n($ws_snapshot_wait_ms),
      ws_snapshot_ready_tokens: n($ws_snapshot_ready_tokens),
      ws_snapshot_total_tokens: n($ws_snapshot_total_tokens),
      ws_snapshot_min_ready_tokens: n($ws_snapshot_min_ready_tokens),
      ws_snapshot_satisfied: ($ws_snapshot_satisfied == "true"),
      latency_history: $latency_history,
      quote_rest_requested: n($quote_rest_requested),
      quote_rest_resolved: n($quote_rest_resolved),
      final_rest_guard_seen: $final_rest_guard,
      fastest_path_proven: $fastest_path,
      note: (
        if $fastest_path then
          "Recent WebSocket snapshot evidence and latest cache hits present; REST final guard still used before action"
        else
          "HFT scan ran, but recent WebSocket snapshot evidence plus latest cache hits are missing"
        end
      )
    },
    live: {
      status: $live_status,
      can_enable_live: $can_enable_live,
      gate_ok: $live_gate_ok,
      readiness_blockers: n($live_readiness_blockers),
      no_submit_guard_proven: $live_no_submit_guard,
      behavior_same_as_paper: $live_identical,
      note: (
        if $live_identical then
          "live gate allowed and paper profit/speed evidence exists"
        else
          "live cannot be assumed same as paper while gate or paper-profit/speed proof is missing"
        end
      )
    },
    scanner: {
      opportunities_found_latest_scan: n($opportunities_found),
      raw_candidate_total_latest_scan: n($raw_candidate_total),
      cumulative_pnl_usd_latest_scan: n($cumulative_pnl_usd),
      best_raw_edge: {
        type: (if $best_raw_edge_type == "" then null else $best_raw_edge_type end),
        event_id: (if $best_raw_edge_event_id == "" then null else $best_raw_edge_event_id end),
        event_title: (if $best_raw_edge_event_title == "" then null else $best_raw_edge_event_title end),
        cost: n($best_raw_edge_cost),
        revenue: n($best_raw_edge_revenue),
        net_profit: n($best_raw_edge_net_profit),
        roi_pct: n($best_raw_edge_roi_pct)
      },
      raw_edge_history: $raw_edge_history,
      no_missed_positive_raw_edge: $missed_positive_edge_guard,
      internal_edge_accounting_consistent: $missed_positive_edge_guard,
      detector_completeness_proven: false
    },
    blockers: [
      (if $paper_operational then empty else blocker("paper_operational"; "paper account/tool not ready") end),
      (if $paper_profitable then empty else blocker("paper_profitable"; "paper profitability evidence report is blocked") end),
      (if $scanner_decision_path then empty else blocker("scanner_live_decision_path_parity"; "paper/live scanner decision hashes do not match") end),
      (if $missed_positive_edge_guard then empty else blocker("scanner_missed_positive_raw_edge"; "best raw edge was positive but scanner emitted no raw candidate") end),
      (if $fastest_path then empty else blocker("hft_fastest_path"; "no warmed WebSocket cache/snapshot evidence in recent HFT proof") end),
      (if $can_enable_live then empty else blocker("live_gate"; "activation packet cannot enable live") end),
      (if $live_gate_ok then empty else blocker("live_gate_ok"; "live-ready gate is blocked") end),
      (if $live_identical then empty else blocker("paper_live_identical"; "paper/live identical behavior not proven") end)
    ],
    evidence: {
      result_json: $result_json,
      profitability_report: $profitability_report,
      activation_packet: (if $activation_packet == "" then null else $activation_packet end),
      latency_csv: $latency_csv,
      scan_summary: $scan_summary
    }
  }
  ' >"$output"

failed=0
if [[ "$require_paper_profitable" -eq 1 && "$paper_profitable" -ne 1 ]]; then
  echo "paper profitability proof missing" >&2
  failed=1
fi
if [[ "$require_live_identical" -eq 1 && "$live_identical" -ne 1 ]]; then
  echo "paper/live identical behavior proof missing" >&2
  failed=1
fi
if [[ "$require_fastest_path" -eq 1 && "$fastest_path" -ne 1 ]]; then
  echo "fastest WebSocket/HFT path proof missing" >&2
  failed=1
fi

printf 'paper_live_parity_audit_ok=%s paper_profitable=%s fastest_path=%s live_identical=%s output=%s\n' \
  "$(json_bool "$ok")" \
  "$(json_bool "$paper_profitable")" \
  "$(json_bool "$fastest_path")" \
  "$(json_bool "$live_identical")" \
  "$output"

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
