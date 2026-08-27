#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/paper-execution-canary.sh [--data-dir PATH] [--account NAME] [--amount USD] [--balance USD] [--output PATH]

Creates an isolated pm-trader paper account, places one tiny FOK paper buy on
an active market, then verifies history/balance. This proves paper execution
plumbing without touching live trading or scanner account state.
EOF
}

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

need date
need jq
need pm-trader

run_id="$(date +%s)"
data_dir="${TMPDIR:-/tmp}/polymarket-paper-execution-canary-${run_id}"
account="paper-canary-${run_id}"
amount_usd="1.00"
balance_usd="100.00"
output="${TMPDIR:-/tmp}/polymarket-paper-execution-canary-${run_id}.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir)
      data_dir="${2:-}"
      shift 2
      ;;
    --account)
      account="${2:-}"
      shift 2
      ;;
    --amount)
      amount_usd="${2:-}"
      shift 2
      ;;
    --balance)
      balance_usd="${2:-}"
      shift 2
      ;;
    --output)
      output="${2:-}"
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

mkdir -p "$(dirname "$output")" "$data_dir"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/paper-canary-work.XXXXXX")"
markets_json="$work_dir/markets.json"
init_json="$work_dir/init.json"
buy_json="$work_dir/buy.json"
history_json="$work_dir/history.json"
balance_json="$work_dir/balance.json"
attempts_jsonl="$work_dir/attempts.jsonl"
: >"$attempts_jsonl"

finish_failure() {
  local reason="$1"
  local exit_code="${2:-1}"
  jq -n \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg reason "$reason" \
    --arg data_dir "$data_dir" \
    --arg account "$account" \
    --arg amount_usd "$amount_usd" \
    --slurpfile attempts "$attempts_jsonl" \
    '{
      ok: false,
      generated_at: $generated_at,
      reason: $reason,
      data_dir: $data_dir,
      account: $account,
      amount_usd: ($amount_usd | tonumber? // null),
      live_trade_attempted: false,
      attempts: $attempts
    }' >"$output"
  exit "$exit_code"
}

pm-trader --data-dir "$data_dir" --account "$account" init --balance "$balance_usd" >"$init_json" \
  || finish_failure "pm_trader_init_failed"

pm-trader markets list --limit "${PAPER_CANARY_MARKET_LIMIT:-80}" --sort liquidity >"$markets_json" \
  || finish_failure "pm_trader_market_list_failed"

candidate_count="$(jq '(.data // []) | length' "$markets_json")"
if [[ "$candidate_count" -eq 0 ]]; then
  finish_failure "no_market_candidates"
fi

selected_slug=""
selected_outcome=""
selected_question=""
selected_attempt=0

while IFS=$'\t' read -r slug outcome question; do
  if [[ -z "$slug" || -z "$outcome" ]]; then
    continue
  fi
  selected_attempt=$((selected_attempt + 1))
  set +e
  pm-trader --data-dir "$data_dir" --account "$account" buy "$slug" "$outcome" "$amount_usd" --type fok >"$buy_json" 2>"$work_dir/buy.err"
  buy_rc=$?
  set -e
  if [[ "$buy_rc" -ne 0 ]]; then
    jq -n \
      --arg slug "$slug" \
      --arg outcome "$outcome" \
      --arg question "$question" \
      --arg rc "$buy_rc" \
      --rawfile stderr "$work_dir/buy.err" \
      '{slug: $slug, outcome: $outcome, question: $question, rc: ($rc | tonumber), stderr: $stderr}' >>"$attempts_jsonl"
    continue
  fi
  if jq -e '.ok == true and (.data.trade.id // null) != null and ((.data.trade.shares // 0) > 0)' "$buy_json" >/dev/null; then
    selected_slug="$slug"
    selected_outcome="$outcome"
    selected_question="$question"
    break
  fi
  jq -n \
    --arg slug "$slug" \
    --arg outcome "$outcome" \
    --arg question "$question" \
    --slurpfile response "$buy_json" \
    '{slug: $slug, outcome: $outcome, question: $question, rc: 0, response: $response[0]}' >>"$attempts_jsonl"
done < <(
  jq -r '
    (.data // [])
    | map(select((.active // false) == true and (.closed // false) == false and ((.outcomes // []) | length) > 0))
    | map(select(((.outcome_prices[0] // 0) | tonumber) > 0.001 and ((.outcome_prices[0] // 0) | tonumber) < 0.999))
    | .[]
    | [.slug, .outcomes[0], .question]
    | @tsv
  ' "$markets_json"
)

if [[ -z "$selected_slug" ]]; then
  finish_failure "no_candidate_filled"
fi

pm-trader --data-dir "$data_dir" --account "$account" history --limit 10 >"$history_json" \
  || finish_failure "pm_trader_history_failed"
pm-trader --data-dir "$data_dir" --account "$account" balance >"$balance_json" \
  || finish_failure "pm_trader_balance_failed"

trade_count="$(jq '(.data // []) | length' "$history_json")"
if [[ "$trade_count" -lt 1 ]]; then
  finish_failure "history_missing_canary_trade"
fi

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg data_dir "$data_dir" \
  --arg account "$account" \
  --arg slug "$selected_slug" \
  --arg outcome "$selected_outcome" \
  --arg question "$selected_question" \
  --arg amount_usd "$amount_usd" \
  --arg selected_attempt "$selected_attempt" \
  --slurpfile init "$init_json" \
  --slurpfile buy "$buy_json" \
  --slurpfile history "$history_json" \
  --slurpfile balance "$balance_json" \
  --slurpfile attempts "$attempts_jsonl" \
  '{
    ok: true,
    generated_at: $generated_at,
    data_dir: $data_dir,
    account: $account,
    market: {
      slug: $slug,
      outcome: $outcome,
      question: $question
    },
    amount_usd: ($amount_usd | tonumber? // null),
    selected_attempt: ($selected_attempt | tonumber? // null),
    live_trade_attempted: false,
    trade_id: ($buy[0].data.trade.id // null),
    shares: ($buy[0].data.trade.shares // null),
    avg_price: ($buy[0].data.trade.avg_price // null),
    order_type: ($buy[0].data.trade.order_type // null),
    trade_count: (($history[0].data // []) | length),
    init: $init[0],
    buy: $buy[0],
    history: $history[0],
    balance: $balance[0],
    failed_attempts: $attempts
  }' >"$output"
