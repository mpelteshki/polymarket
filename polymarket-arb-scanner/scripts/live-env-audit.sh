#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/live-env-audit.sh [--json|--template]

Prints redacted live environment audit JSON.

No secret values, addresses, URLs, or token strings are emitted. Output records
presence, shape checks, expected state, and blocking counts only.

Use --template to print a shell env skeleton from the same requirement records.
EOF
}

output_mode="json"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      output_mode="json"
      shift
      ;;
    --template)
      output_mode="template"
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

need date
need jq

tmp="$(mktemp "${TMPDIR:-/tmp}/polymarket-live-env-audit.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
: >"$tmp"

json_bool() {
  if [[ "$1" -eq 1 ]]; then
    echo true
  else
    echo false
  fi
}

is_address() {
  [[ "$1" =~ ^0x[0-9A-Fa-f]{40}$ ]]
}

is_url() {
  [[ "$1" =~ ^https?://[^[:space:]]+$ ]]
}

is_true_value() {
  case "$1" in
    true|TRUE|1|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

is_false_value() {
  case "$1" in
    false|FALSE|0|no|NO|off|OFF) return 0 ;;
    *) return 1 ;;
  esac
}

emit_requirement() {
  local name="$1"
  local group="$2"
  local credential="$3"
  local required="$4"
  local blocking="$5"
  local present="$6"
  local ok="$7"
  local expected="$8"
  local issue="$9"
  jq -n \
    --arg name "$name" \
    --arg group "$group" \
    --arg expected "$expected" \
    --arg issue "$issue" \
    --argjson credential "$credential" \
    --argjson required "$required" \
    --argjson blocking "$blocking" \
    --argjson present "$present" \
    --argjson ok "$ok" \
    '{
      name: $name,
      group: $group,
      credential: $credential,
      required: $required,
      blocking: $blocking,
      present: $present,
      ok: $ok,
      expected: $expected,
      issue: (if $issue == "" then null else $issue end)
    }' >>"$tmp"
}

audit_nonempty() {
  local name="$1" group="$2" credential="$3"
  local value="${!name:-}"
  local present=0 ok=0 issue="missing"
  if [[ -n "$value" ]]; then
    present=1
    ok=1
    issue=""
  fi
  emit_requirement "$name" "$group" "$credential" true true \
    "$(json_bool "$present")" "$(json_bool "$ok")" "nonempty" "$issue"
}

audit_nonempty_optional() {
  local name="$1" group="$2" credential="$3"
  local value="${!name:-}"
  local present=0 ok=1 issue=""
  if [[ -n "$value" ]]; then
    present=1
  fi
  emit_requirement "$name" "$group" "$credential" false false \
    "$(json_bool "$present")" "$(json_bool "$ok")" "nonempty_if_used" "$issue"
}

audit_nonempty_primary_or_alias() {
  local name="$1" alias="$2" group="$3" credential="$4"
  local value="${!name:-}" alias_value="${!alias:-}"
  local present=0 ok=0 issue="missing"
  if [[ -n "$value" || -n "$alias_value" ]]; then
    present=1
    ok=1
    issue=""
  fi
  emit_requirement "$name" "$group" "$credential" true true \
    "$(json_bool "$present")" "$(json_bool "$ok")" "${name}_or_${alias}_nonempty" "$issue"
}

audit_bool_true() {
  local name="$1" group="$2"
  local value="${!name:-}"
  local present=0 ok=0 issue="missing"
  if [[ -n "$value" ]]; then
    present=1
    if is_true_value "$value"; then
      ok=1
      issue=""
    else
      issue="not_true"
    fi
  fi
  emit_requirement "$name" "$group" false true true \
    "$(json_bool "$present")" "$(json_bool "$ok")" "true" "$issue"
}

audit_bool_false() {
  local name="$1" group="$2"
  local value="${!name:-}"
  local present=0 ok=0 issue="missing"
  if [[ -n "$value" ]]; then
    present=1
    if is_false_value "$value"; then
      ok=1
      issue=""
    else
      issue="not_false"
    fi
  fi
  emit_requirement "$name" "$group" false true true \
    "$(json_bool "$present")" "$(json_bool "$ok")" "false" "$issue"
}

audit_address() {
  local name="$1" group="$2" required="$3" blocking="$4"
  local value="${!name:-}"
  local present=0 ok=0 issue=""
  if [[ -n "$value" ]]; then
    present=1
    if is_address "$value"; then
      ok=1
    else
      issue="invalid_address"
    fi
  elif [[ "$required" == "true" ]]; then
    issue="missing"
  else
    ok=1
  fi
  emit_requirement "$name" "$group" false "$required" "$blocking" \
    "$(json_bool "$present")" "$(json_bool "$ok")" "0x40_hex_address" "$issue"
}

audit_url() {
  local name="$1" group="$2" credential="$3" required="$4" blocking="$5"
  local value="${!name:-}"
  local present=0 ok=0 issue=""
  if [[ -n "$value" ]]; then
    present=1
    if is_url "$value"; then
      ok=1
    else
      issue="invalid_url"
    fi
  elif [[ "$required" == "true" ]]; then
    issue="missing"
  else
    ok=1
  fi
  emit_requirement "$name" "$group" "$credential" "$required" "$blocking" \
    "$(json_bool "$present")" "$(json_bool "$ok")" "http_or_https_url" "$issue"
}

audit_preflight_live_flag() {
  local value="${LIVE_TRADING_ENABLED:-}"
  local present=0 ok=1 issue=""
  if [[ -n "$value" ]]; then
    present=1
    if is_true_value "$value"; then
      ok=0
      issue="ambient_true_but_preflight_forces_false"
    fi
  fi
  emit_requirement "LIVE_TRADING_ENABLED" "final_flip" false false false \
    "$(json_bool "$present")" "$(json_bool "$ok")" "not_true_during_no_submit_preflight" "$issue"
}

signature_type="${LIVE_SIGNATURE_TYPE:-}"
signature_ok=0
signature_issue="missing"
if [[ "$signature_type" =~ ^[0-3]$ ]]; then
  signature_ok=1
  signature_issue=""
elif [[ -n "$signature_type" ]]; then
  signature_issue="expected_0_1_2_3"
fi
emit_requirement "LIVE_SIGNATURE_TYPE" "identity_and_clob_auth" false true true \
  "$(json_bool "$([[ -n "$signature_type" ]] && echo 1 || echo 0)")" \
  "$(json_bool "$signature_ok")" "0|1|2|3" "$signature_issue"

audit_nonempty "POLYMARKET_PRIVATE_KEY" "identity_and_clob_auth" true
audit_nonempty "POLYMARKET_API_KEY" "identity_and_clob_auth" true
audit_nonempty "POLYMARKET_API_SECRET" "identity_and_clob_auth" true
audit_nonempty "POLYMARKET_API_PASSPHRASE" "identity_and_clob_auth" true
audit_nonempty "CLOB_API_KEY" "identity_and_clob_auth" true
audit_nonempty "CLOB_SECRET" "identity_and_clob_auth" true
audit_nonempty_primary_or_alias \
  "CLOB_PASS_PHRASE" "CLOB_PASSPHRASE" "identity_and_clob_auth" true
audit_address "LIVE_SIGNER_ADDRESS" "identity_and_clob_auth" false false
audit_bool_true "LIVE_USER_WS_ENABLED" "identity_and_clob_auth"

if [[ "$signature_type" =~ ^[1-3]$ ]]; then
  audit_address "LIVE_FUNDER_ADDRESS" "identity_and_clob_auth" true true
else
  audit_address "LIVE_FUNDER_ADDRESS" "identity_and_clob_auth" false false
fi

audit_bool_true "LIVE_COMBO_RFQ_ROUTE_ENABLED" "combo_rfq_protocol"
audit_bool_true "COMBO_RFQ_REQUESTER_ENABLED" "combo_rfq_protocol"
audit_bool_true "COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED" "combo_rfq_protocol"
audit_bool_true "COMBO_RFQ_ACCEPT_ENABLED" "combo_rfq_protocol"
audit_bool_true "COMBO_RFQ_STREAM_ENABLED" "combo_rfq_protocol"
audit_nonempty "COMBO_RFQ_BEARER_TOKEN" "combo_rfq_protocol" true
audit_nonempty "COMBO_RFQ_STREAM_BEARER_TOKEN" "combo_rfq_protocol" true
audit_nonempty "COMBO_RFQ_PARTICIPANT_ID" "combo_rfq_protocol" false

audit_url "POLYGON_RPC_URL" "settlement_and_closeout" true true true
audit_address "COMBO_RFQ_EXCHANGE_V3_ADDRESS" "funding_and_approvals" true true
audit_bool_true "ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED" "settlement_and_closeout"
audit_bool_true "SETTLEMENT_MONITOR_ENABLED" "settlement_and_closeout"
audit_bool_true "LIVE_CLOSEOUT_ENABLED" "settlement_and_closeout"
audit_bool_false "LIVE_CLOSEOUT_DRY_RUN" "settlement_and_closeout"

if [[ "$signature_type" == "3" ]]; then
  audit_url "RELAYER_API_URL" "settlement_and_closeout" false true true
  audit_nonempty "RELAYER_API_KEY" "settlement_and_closeout" true
  audit_address "RELAYER_API_KEY_ADDRESS" "settlement_and_closeout" true true
else
  audit_url "RELAYER_API_URL" "settlement_and_closeout" false false false
  audit_nonempty_optional "RELAYER_API_KEY" "settlement_and_closeout" true
  audit_address "RELAYER_API_KEY_ADDRESS" "settlement_and_closeout" false false
fi

audit_preflight_live_flag

if [[ "$output_mode" == "template" ]]; then
  jq -s -r '
    def value:
      if .name == "LIVE_TRADING_ENABLED" then "false"
      elif .expected == "true" then "true"
      elif .expected == "false" then "false"
      elif .expected == "0|1|2|3" then "0"
      else ""
      end;
    def note:
      "# expected=" + .expected
      + (if .credential then " credential=true" else "" end)
      + (if .required then " required=true" else " required=false" end);
    [
      "# Polymarket live env skeleton",
      "# Generated by scripts/live-env-audit.sh --template",
      "# Fill values in shell or secret manager; do not commit secrets.",
      "# Keep LIVE_TRADING_ENABLED=false for operator preflight.",
      ""
    ],
    (
      group_by(.group)[]
      | ["# " + .[0].group]
      + (map([note, "export " + .name + "=\"" + value + "\""])
      | add)
      + [""]
    )
    | .[]
  ' "$tmp"
else
  jq -s \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{
      generated_at: $generated_at,
      purpose: "operator-safe live env audit; values intentionally omitted",
      mode: "no_submit_preflight",
      records: .,
      summary: {
        total_count: length,
        required_count: ([.[] | select(.required == true)] | length),
        credential_count: ([.[] | select(.credential == true)] | length),
        present_required_count: ([.[] | select(.required == true and .present == true)] | length),
        missing_required_count: ([.[] | select(.required == true and .present != true)] | length),
        invalid_required_count: ([.[] | select(.required == true and .present == true and .ok != true)] | length),
        blocking_count: ([.[] | select(.blocking == true and .ok != true)] | length),
        warning_count: ([.[] | select(.blocking != true and .ok != true)] | length),
        ready: (([.[] | select(.blocking == true and .ok != true)] | length) == 0)
      },
      missing_required: [.[] | select(.required == true and .present != true) | .name],
      blocking: [.[] | select(.blocking == true and .ok != true) | {name, group, issue, expected}]
    }' "$tmp"
fi
