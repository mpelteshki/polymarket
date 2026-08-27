#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage: scripts/live-operator-preflight.sh [--readiness-manifest PATH] [--allow-live-blocked]

Runs operator live preflight with real ambient live env, but forces no-submit:
  LIVE_TRADING_ENABLED=false
  LIVE_DIAGNOSTICS_ENABLED=true
  PAPER_TRADING_ENABLED=false

It never creates an account and never enables live order submit. It writes a
redacted proof bundle under /tmp/polymarket-live-operator-preflight-* unless
LIVE_OPERATOR_PREFLIGHT_ROOT is set.
EOF
}

allow_live_blocked=0
readiness_manifest=""
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
    --allow-live-blocked)
      allow_live_blocked=1
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
need cmp
need find
need jq
need mktemp
need perl
need rg
need shasum
need sort
need stat
need wc

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${LIVE_OPERATOR_PREFLIGHT_ROOT:-}" ]]; then
  run_root="$LIVE_OPERATOR_PREFLIGHT_ROOT"
  if [[ "$run_root" != /* ]]; then
    run_root="$PWD/$run_root"
  fi
  if [[ -L "$run_root" ]]; then
    echo "operator preflight root must not be a symlink: $run_root" >&2
    exit 2
  fi
  mkdir -p "$run_root"
  run_root="$(cd "$run_root" && pwd)"
else
  run_root="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-live-operator-preflight-XXXXXX")"
fi
live_dir="$run_root/live"
log_path="$run_root/live-operator-preflight.log"
result_json="$run_root/live-operator-preflight-result.json"
manifest_json="$run_root/live-operator-preflight-manifest.json"
manifest_files_json="$run_root/live-operator-preflight-files.json"
manifest_verification_txt="$run_root/live-operator-preflight-verification.txt"
env_audit_json="$run_root/live-env-audit.json"
env_template_sh="$run_root/live-env-template.sh"
launch_config_fingerprint_json="$run_root/launch-config-fingerprint.json"
launch_config_fingerprint_post_json="$run_root/launch-config-fingerprint-post.json"
launch_config_fingerprint_log="$run_root/launch-config-fingerprint.log"
secret_hits_jsonl="$run_root/artifact-secret-value-hits.jsonl"
submit_marker_hits="$run_root/no-live-submit-marker-hits.txt"
live_trade_hits="$run_root/no-live-trade-row-hits.txt"
combo_journal_hits="$run_root/no-live-combo-journal-hits.txt"
standard_journal_hits="$run_root/no-live-standard-journal-hits.txt"
runtime_panic_hits="$run_root/runtime-panic-hits.txt"
if [[ -L "$live_dir" ]]; then
  echo "operator preflight root must not be a symlink: $run_root" >&2
  exit 2
fi
mkdir -p "$live_dir"
chmod 700 "$run_root" "$live_dir"

latest_readiness_manifest() {
  find -L /tmp -path '/tmp/polymarket-trade-readiness-*/readiness-bundle-manifest.json' -type f -print 2>/dev/null \
    | while IFS= read -r path; do
        printf '%s\t%s\n' "$(stat -f '%m' "$path" 2>/dev/null || stat -c '%Y' "$path")" "$path"
      done \
    | sort -nr \
    | awk -F '\t' 'NR == 1 { print $2 }'
}

if [[ -z "$readiness_manifest" ]]; then
  readiness_manifest="$(latest_readiness_manifest)"
fi
if [[ -z "$readiness_manifest" || ! -f "$readiness_manifest" ]]; then
  echo "operator preflight requires a readiness bundle manifest" >&2
  exit 2
fi
readiness_manifest="$(cd "$(dirname "$readiness_manifest")" && pwd)/$(basename "$readiness_manifest")"
"$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_manifest" >/dev/null
release_binary="$(jq -r '.files[]? | select(.label == "release_binary") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
build_provenance="$(jq -r '.files[]? | select(.label == "build_provenance") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
if [[ -z "$release_binary" || ! -x "$release_binary" || -z "$build_provenance" || ! -f "$build_provenance" ]]; then
  echo "readiness manifest lacks executable release/build provenance" >&2
  exit 2
fi
release_binary_sha="$(shasum -a 256 "$release_binary" | awk '{print $1}')"
if [[ "$release_binary_sha" != "$(jq -r '.binary.sha256 // empty' "$build_provenance")" ]]; then
  echo "readiness release binary does not match build provenance" >&2
  exit 2
fi
paper_adapter_path="$(jq -r '.paper_execution_binding.paper_adapter.canonical_path // empty' "$readiness_manifest")"
if [[ -z "$paper_adapter_path" || ! -x "$paper_adapter_path" ]]; then
  echo "readiness manifest lacks canonical paper adapter provenance" >&2
  exit 2
fi

secret_env_names=(
  POLYMARKET_PRIVATE_KEY
  POLYMARKET_API_KEY
  POLYMARKET_API_SECRET
  POLYMARKET_API_PASSPHRASE
  CLOB_API_KEY
  CLOB_SECRET
  CLOB_PASS_PHRASE
  CLOB_PASSPHRASE
  COMBO_RFQ_BEARER_TOKEN
  COMBO_RFQ_STREAM_BEARER_TOKEN
  RELAYER_API_KEY
  POLYGON_RPC_URL
  WEBHOOK_URL
  BETDEX_AUTH_TOKEN
)

secret_env_list="$(printf '%s\n' "${secret_env_names[@]}")"
present_secret_envs=()
for secret_env_name in "${secret_env_names[@]}"; do
  if [[ -n "${!secret_env_name:-}" ]]; then
    present_secret_envs+=("$secret_env_name")
  fi
done
if [[ "${#present_secret_envs[@]}" -gt 0 ]]; then
  present_secret_env_list="$(printf '%s\n' "${present_secret_envs[@]}")"
else
  present_secret_env_list=""
fi

redact_stream() {
  perl -pe '
    s/ip=[^ ]+/ip=<redacted>/g;
    s/\b(?:\d{1,3}\.){3}\d{1,3}\b/<ipv4-redacted>/g;
    s/0x[[:xdigit:]]{64}/<hex64-redacted>/g;
    s/([A-Za-z0-9_\-]*secret[A-Za-z0-9_\-]*[=:])[^\s",}]+/${1}<redacted>/ig;
    s/([A-Za-z0-9_\-]*token[A-Za-z0-9_\-]*[=:])[^\s",}]+/${1}<redacted>/ig;
    s/([A-Za-z0-9_\-]*key[A-Za-z0-9_\-]*[=:])[^\s",}]+/${1}<redacted>/ig;
  '
}

section() {
  printf '\n== %s ==\n' "$1"
}

write_manifest_file_entry() {
  local label="$1"
  local path="$2"
  if [[ -e "$path" ]]; then
    local sha size
    sha="$(shasum -a 256 "$path" | awk '{print $1}')"
    size="$(wc -c <"$path" | tr -d '[:space:]')"
    jq -n \
      --arg label "$label" \
      --arg path "$path" \
      --arg sha "$sha" \
      --arg size "$size" \
      '{label: $label, path: $path, exists: true, size_bytes: ($size | tonumber), sha256: $sha}'
  else
    jq -n \
      --arg label "$label" \
      --arg path "$path" \
      '{label: $label, path: $path, exists: false, size_bytes: 0, sha256: null}'
  fi
}

section "redacted live env audit"
"$repo_root/scripts/live-env-audit.sh" >"$env_audit_json"
"$repo_root/scripts/live-env-audit.sh" --template >"$env_template_sh"
jq -r '"env_audit_ready=\(.summary.ready) blocking=\(.summary.blocking_count) missing_required=\(.summary.missing_required_count)"' "$env_audit_json"
env_audit_ready="$(jq -r '.summary.ready // false' "$env_audit_json")"
env_audit_blocking_count="$(jq -r '.summary.blocking_count // 1' "$env_audit_json")"

section "effective launch config fingerprint"
(
  cd "$repo_root"
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=true \
  PAPER_TRADING_ENABLED=false \
  DIAGNOSTICS_DIR="$live_dir" \
  EXTERNAL_PAPER_COMMAND="$paper_adapter_path" \
  "$release_binary" \
    --launch-config-fingerprint-output "$launch_config_fingerprint_json"
) >"$launch_config_fingerprint_log" 2>&1
if jq -e '
  .paper_live_profile_config.schema_version == 1 and
  .paper_live_profile_config.execution_route == "legged_clob_paper" and
  .paper_live_profile_config.order_mode == "market_style" and
  .paper_live_profile_config.effective_order_type == "fok" and
  .paper_live_profile_config.live_order_type == "fok" and
  .paper_live_profile_config.effective_paper_use_limit_orders == false and
  .paper_live_profile_config.full_clob_required == true and
  .paper_live_profile_config.match_live_position_size == true and
  (.paper_live_profile_config.effective_position_size_usd | type == "number" and . > 0) and
  .paper_live_profile_config.effective_position_size_usd == .paper_live_profile_config.live_position_size_usd and
  (.paper_live_profile_config.paper_max_share_mismatch_pct | type == "number" and . >= 0 and . <= 0.5) and
  (.paper_live_profile_config.min_net_profit_usd | type == "number" and . > 0) and
  (.paper_live_profile_config.min_roi_pct | type == "number" and . > 0) and
  (.paper_live_profile_config.max_signal_age_secs | type == "number" and . > 0) and
  (.paper_live_profile_config.gas_fallback_usd | type == "number" and . >= 0) and
  (.paper_live_profile_config.live_signature_type |
    type == "number" and . == floor and . >= 0 and . <= 3) and
  (.paper_live_profile_config.order_size_step_shares | type == "number" and . > 0) and
  .paper_live_profile_config.validate_opportunities_at_target_size == true and
  .paper_live_profile_config.execute_only_full_clob_prices == true and
  (.paper_live_profile_config.live_slippage_bps | type == "number") and
  (.paper_live_profile_config.live_edge_haircut_usd | type == "number" and . >= 0) and
  (.paper_live_profile_config.live_edge_haircut_bps | type == "number") and
  (.paper_live_profile_config.live_min_leg_size_usd | type == "number" and . > 0) and
  (.paper_live_profile_config.live_max_refresh_to_submit_ms | type == "number" and . > 0) and
  .paper_live_profile_config.clob_api_url == "https://clob.polymarket.com" and
  .paper_live_profile_config.gamma_api_url == "https://gamma-api.polymarket.com" and
  (.paper_live_profile_config.external_paper_command == "pm-trader" or
    (.paper_live_profile_config.external_paper_command | endswith("/pm-trader")))
' "$launch_config_fingerprint_json" >/dev/null; then
  paper_live_profile_config_safe=true
else
  paper_live_profile_config_safe=false
fi

section "operator live no-submit diagnostics"
set +e
(
  cd "$repo_root"
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=true \
  PAPER_TRADING_ENABLED=false \
  DIAGNOSTICS_DIR="$live_dir" \
  EXTERNAL_PAPER_COMMAND="$paper_adapter_path" \
  "$release_binary" \
    --live-diagnostics --once --no-paper \
    --expected-launch-config-fingerprint "$launch_config_fingerprint_json"
) 2>&1 | redact_stream | tee "$log_path"
pipeline_status=("${PIPESTATUS[@]}")
scanner_status="${pipeline_status[0]:-1}"
redactor_status="${pipeline_status[1]:-1}"
tee_status="${pipeline_status[2]:-1}"
cargo_status="$scanner_status"
set -e

section "post-diagnostics launch config fingerprint"
(
  cd "$repo_root"
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=true \
  PAPER_TRADING_ENABLED=false \
  DIAGNOSTICS_DIR="$live_dir" \
  EXTERNAL_PAPER_COMMAND="$paper_adapter_path" \
  "$release_binary" \
    --launch-config-fingerprint-output "$launch_config_fingerprint_post_json"
) >>"$launch_config_fingerprint_log" 2>&1
if ! cmp -s "$launch_config_fingerprint_json" "$launch_config_fingerprint_post_json"; then
  echo "operator launch configuration changed during diagnostics" >&2
  exit 1
fi
: >"$runtime_panic_hits"
rg -n -H -i 'panicked at|CryptoProvider' \
  "$launch_config_fingerprint_log" "$log_path" >"$runtime_panic_hits" || true
runtime_panic_hit_count="$(awk 'END { print NR + 0 }' "$runtime_panic_hits")"

section "no-live artifact scan"
: >"$live_trade_hits"
while IFS= read -r -d '' trade_file; do
  awk -F, -v file="$trade_file" '
    NR == 1 {
      mode = 0
      status = 0
      for (i = 1; i <= NF; i++) {
        if ($i == "mode") mode = i
        if ($i == "status") status = i
      }
      next
    }
    mode > 0 && $0 !~ /^[[:space:]]*$/ &&
      (tolower($mode) == "live" || tolower($mode) == "live_combo_rfq") &&
      (status == 0 || tolower($status) !~ /^blocked/) {
      print file ":" NR
    }
  ' "$trade_file" >>"$live_trade_hits"
done < <(find "$run_root" -type f -name 'trades.csv' -print0)

: >"$combo_journal_hits"
while IFS= read -r -d '' journal_file; do
  awk -v file="$journal_file" '
    $0 !~ /^[[:space:]]*$/ {
      line = tolower($0)
      if (line ~ /"status"[[:space:]]*:[[:space:]]*"blocked/) next
      print file ":" FNR
    }
  ' "$journal_file" >>"$combo_journal_hits"
done < <(find "$run_root" -type f -name 'combo_rfq_execution_journal.jsonl' -print0)

: >"$standard_journal_hits"
while IFS= read -r -d '' journal_file; do
  awk -v file="$journal_file" '
    $0 !~ /^[[:space:]]*$/ {
      line = tolower($0)
      if (line ~ /"status"[[:space:]]*:[[:space:]]*"blocked/) next
      print file ":" FNR
    }
  ' "$journal_file" >>"$standard_journal_hits"
done < <(find "$run_root" -type f -name 'live_execution_journal.jsonl' -print0)

: >"$submit_marker_hits"
find "$run_root" -type f \
  \( -name '*.log' -o -name '*.txt' -o -name '*.json' -o -name '*.jsonl' \) \
  ! -name "$(basename "$submit_marker_hits")" \
  ! -name "$(basename "$standard_journal_hits")" \
  ! -name "$(basename "$secret_hits_jsonl")" \
  -print0 \
  | xargs -0 rg -H -n -I -i -e 'sdk_post_orders|post_orders|submit_orders|submitted live|live order submitted|placing live order|CLOB order submit|order submission succeeded|accepted_pending_finality' >"$submit_marker_hits" || true

: >"$secret_hits_jsonl"
while IFS= read -r -d '' artifact_file; do
  for secret_env_name in "${secret_env_names[@]}"; do
    secret_value="${!secret_env_name:-}"
    if [[ "${#secret_value}" -ge 8 ]] && rg -q -F -- "$secret_value" "$artifact_file"; then
      jq -n --arg env "$secret_env_name" --arg path "$artifact_file" \
        '{env: $env, path: $path}' >>"$secret_hits_jsonl"
    fi
  done
done < <(find "$run_root" -type f \
  \( -name '*.log' -o -name '*.txt' -o -name '*.json' -o -name '*.jsonl' \) \
  ! -name "$(basename "$secret_hits_jsonl")" \
  -print0)

live_trade_hit_count="$(awk 'END { print NR + 0 }' "$live_trade_hits")"
combo_journal_hit_count="$(awk 'END { print NR + 0 }' "$combo_journal_hits")"
standard_journal_hit_count="$(awk 'END { print NR + 0 }' "$standard_journal_hits")"
submit_marker_hit_count="$(awk 'END { print NR + 0 }' "$submit_marker_hits")"
secret_hit_count="$(awk 'END { print NR + 0 }' "$secret_hits_jsonl")"

live_report="$live_dir/live_readiness_report.json"
combo_report="$live_dir/combo_rfq_route_promotion_report.json"
calibration_report="$live_dir/live_route_calibration_report.json"
live_report_slurp="$live_report"
combo_report_slurp="$combo_report"
if [[ -f "$live_report" ]]; then
  live_supported="$(jq '.live_submissions_supported // false' "$live_report")"
  all_live_checks_ready="$(jq '([.checks[]? | select(.state != "ready")] | length) == 0' "$live_report")"
else
  live_report_slurp="$run_root/missing-live-readiness-report.json"
  jq -n '{live_submissions_supported: false, checks: []}' >"$live_report_slurp"
  live_supported=false
  all_live_checks_ready=false
fi
if [[ -f "$combo_report" ]]; then
  combo_promoted="$(jq '.promoted // false' "$combo_report")"
else
  combo_report_slurp="$run_root/missing-combo-rfq-route-promotion-report.json"
  jq -n '{promoted: false, blockers: ["combo_rfq_route_promotion_report_missing"]}' >"$combo_report_slurp"
  combo_promoted=false
fi
if [[ "$live_supported" == "true" \
  && "$all_live_checks_ready" == "true" \
  && "$combo_promoted" == "true" \
  && "$env_audit_ready" == "true" \
  && "$env_audit_blocking_count" -eq 0 \
  && "$paper_live_profile_config_safe" == "true" \
  && "$scanner_status" -eq 0 \
  && "$redactor_status" -eq 0 \
  && "$tee_status" -eq 0 \
  && "$runtime_panic_hit_count" -eq 0 ]]; then
  live_ready=true
else
  live_ready=false
fi

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg run_root "$run_root" \
  --arg repo_root "$repo_root" \
  --arg live_dir "$live_dir" \
  --arg log_path "$log_path" \
  --arg result_json "$result_json" \
  --arg manifest_json "$manifest_json" \
  --arg readiness_manifest "$readiness_manifest" \
  --arg release_binary "$release_binary" \
  --arg build_provenance "$build_provenance" \
  --arg release_binary_sha "$release_binary_sha" \
  --arg paper_adapter_path "$paper_adapter_path" \
  --arg env_audit_json "$env_audit_json" \
  --arg env_template_sh "$env_template_sh" \
  --arg live_report "$live_report" \
  --arg combo_report "$combo_report" \
  --arg calibration_report "$calibration_report" \
  --arg live_trade_hits "$live_trade_hits" \
  --arg combo_journal_hits "$combo_journal_hits" \
  --arg standard_journal_hits "$standard_journal_hits" \
  --arg submit_marker_hits "$submit_marker_hits" \
  --arg secret_hits_jsonl "$secret_hits_jsonl" \
  --arg secret_env_list "$secret_env_list" \
  --arg present_secret_env_list "$present_secret_env_list" \
  --arg cargo_status "$cargo_status" \
  --arg scanner_status "$scanner_status" \
  --arg redactor_status "$redactor_status" \
  --arg tee_status "$tee_status" \
  --arg live_trade_hit_count "$live_trade_hit_count" \
  --arg combo_journal_hit_count "$combo_journal_hit_count" \
  --arg standard_journal_hit_count "$standard_journal_hit_count" \
  --arg submit_marker_hit_count "$submit_marker_hit_count" \
  --arg secret_hit_count "$secret_hit_count" \
  --arg runtime_panic_hits "$runtime_panic_hits" \
  --arg runtime_panic_hit_count "$runtime_panic_hit_count" \
  --argjson live_supported "$live_supported" \
  --argjson all_live_checks_ready "$all_live_checks_ready" \
  --argjson combo_promoted "$combo_promoted" \
  --argjson paper_live_profile_config_safe "$paper_live_profile_config_safe" \
  --argjson live_ready "$live_ready" \
  --slurpfile env_audit "$env_audit_json" \
  --slurpfile launch_config_fingerprint "$launch_config_fingerprint_json" \
  --slurpfile live "$live_report_slurp" \
  --slurpfile combo "$combo_report_slurp" \
  '{
    generated_at: $generated_at,
    run_root: $run_root,
    live_dir: $live_dir,
    log_path: $log_path,
    result_json: $result_json,
    manifest_json: $manifest_json,
    readiness_manifest: $readiness_manifest,
    release_build: {
      binary_path: $release_binary,
      provenance_path: $build_provenance,
      binary_sha256: $release_binary_sha
    },
    effective_launch_env: {
      working_directory: $repo_root,
      diagnostics_dir: $live_dir,
      live_diagnostics_enabled: true,
      paper_trading_enabled: false,
      external_paper_command: $paper_adapter_path
    },
    env_audit_json: $env_audit_json,
    env_template_sh: $env_template_sh,
    no_submit_policy: {
      live_trading_enabled_forced: false,
      account_created: false
    },
    env_audit: ($env_audit[0] // null),
    launch_config_fingerprint: ($launch_config_fingerprint[0] // null),
    ambient_secret_env_names: ($secret_env_list | split("\n") | map(select(length > 0))),
    ambient_secret_envs_present: ($present_secret_env_list | split("\n") | map(select(length > 0))),
    cargo_status: ($cargo_status | tonumber? // null),
    process_status: {
      scanner: ($scanner_status | tonumber? // null),
      redactor: ($redactor_status | tonumber? // null),
      tee: ($tee_status | tonumber? // null)
    },
    runtime_panic_scan: {
      ok: (($runtime_panic_hit_count | tonumber? // 1) == 0),
      hit_count: ($runtime_panic_hit_count | tonumber? // null),
      hits_path: $runtime_panic_hits
    },
    live_ready: $live_ready,
    live_submissions_supported: $live_supported,
    all_live_checks_ready: $all_live_checks_ready,
    combo_promoted: $combo_promoted,
    paper_live_profile_config_safe: $paper_live_profile_config_safe,
    live_not_ready_checks: (
      if ($live | length) > 0 then
        [$live[0].checks[]? | select(.state != "ready") | {key, state, detail}]
      else
        []
      end
    ),
    combo_blockers: (
      if ($combo | length) > 0 then
        ($combo[0].blockers // [])
      else
        []
      end
    ),
    no_live_submission: {
      ok: (($live_trade_hit_count | tonumber? // 0) == 0 and ($combo_journal_hit_count | tonumber? // 0) == 0 and ($standard_journal_hit_count | tonumber? // 0) == 0 and ($submit_marker_hit_count | tonumber? // 0) == 0),
      live_trade_row_hits: ($live_trade_hit_count | tonumber? // 0),
      combo_execution_journal_hits: ($combo_journal_hit_count | tonumber? // 0),
      standard_execution_journal_hits: ($standard_journal_hit_count | tonumber? // 0),
      submit_marker_hits: ($submit_marker_hit_count | tonumber? // 0),
      hit_files: {
        live_trade_rows: $live_trade_hits,
        combo_execution_journals: $combo_journal_hits,
        standard_execution_journals: $standard_journal_hits,
        submit_markers: $submit_marker_hits
      }
    },
    artifact_secret_value_scan: {
      ok: (($secret_hit_count | tonumber? // 0) == 0),
      hit_count: ($secret_hit_count | tonumber? // 0),
      hits_jsonl: $secret_hits_jsonl
    },
    reports: {
      live_readiness_report: $live_report,
      combo_rfq_route_promotion_report: $combo_report,
      live_route_calibration_report: $calibration_report
    }
  }' >"$result_json"

section "operator preflight manifest"
: >"$manifest_files_json"
write_manifest_file_entry "operator_preflight_result" "$result_json" >>"$manifest_files_json"
write_manifest_file_entry "operator_preflight_log" "$log_path" >>"$manifest_files_json"
write_manifest_file_entry "live_env_audit" "$env_audit_json" >>"$manifest_files_json"
write_manifest_file_entry "live_env_template" "$env_template_sh" >>"$manifest_files_json"
write_manifest_file_entry "launch_config_fingerprint" "$launch_config_fingerprint_json" >>"$manifest_files_json"
write_manifest_file_entry "launch_config_fingerprint_post" "$launch_config_fingerprint_post_json" >>"$manifest_files_json"
write_manifest_file_entry "launch_config_fingerprint_log" "$launch_config_fingerprint_log" >>"$manifest_files_json"
write_manifest_file_entry "readiness_manifest" "$readiness_manifest" >>"$manifest_files_json"
write_manifest_file_entry "release_binary" "$release_binary" >>"$manifest_files_json"
write_manifest_file_entry "build_provenance" "$build_provenance" >>"$manifest_files_json"
write_manifest_file_entry "live_readiness_report" "$live_report" >>"$manifest_files_json"
write_manifest_file_entry "combo_rfq_route_promotion_report" "$combo_report" >>"$manifest_files_json"
write_manifest_file_entry "engine_mode_report" "$live_dir/engine_mode_report.json" >>"$manifest_files_json"
write_manifest_file_entry "engine_mode_state" "$live_dir/engine_mode_state.json" >>"$manifest_files_json"
write_manifest_file_entry "engine_mode_journal" "$live_dir/engine_mode_journal.jsonl" >>"$manifest_files_json"
write_manifest_file_entry "diagnostics_daemon_report" "$live_dir/diagnostics_daemon_report.json" >>"$manifest_files_json"
write_manifest_file_entry "settlement_hazard_report" "$live_dir/settlement_hazard_report.json" >>"$manifest_files_json"
write_manifest_file_entry "combo_rfq_finality_report" "$live_dir/combo_rfq_finality_report.json" >>"$manifest_files_json"
write_manifest_file_entry "live_route_calibration_report" "$live_dir/live_route_calibration_report.json" >>"$manifest_files_json"
write_manifest_file_entry "no_live_trade_row_hits" "$live_trade_hits" >>"$manifest_files_json"
write_manifest_file_entry "no_live_combo_journal_hits" "$combo_journal_hits" >>"$manifest_files_json"
write_manifest_file_entry "no_live_standard_journal_hits" "$standard_journal_hits" >>"$manifest_files_json"
write_manifest_file_entry "no_live_submit_marker_hits" "$submit_marker_hits" >>"$manifest_files_json"
write_manifest_file_entry "artifact_secret_value_hits" "$secret_hits_jsonl" >>"$manifest_files_json"
write_manifest_file_entry "runtime_panic_hits" "$runtime_panic_hits" >>"$manifest_files_json"
jq -s '.' "$manifest_files_json" >"$manifest_files_json.tmp"
mv "$manifest_files_json.tmp" "$manifest_files_json"
jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg run_root "$run_root" \
  --arg result_json "$result_json" \
  --arg manifest_json "$manifest_json" \
  --arg readiness_manifest "$readiness_manifest" \
  --arg release_binary "$release_binary" \
  --arg build_provenance "$build_provenance" \
  --arg release_binary_sha "$release_binary_sha" \
  --arg launch_config_fingerprint_json "$launch_config_fingerprint_json" \
  --slurpfile result "$result_json" \
  --slurpfile files "$manifest_files_json" \
  '{
    generated_at: $generated_at,
    run_root: $run_root,
    result_json: $result_json,
    manifest_json: $manifest_json,
    readiness_manifest: $readiness_manifest,
    release_build: {
      binary_path: $release_binary,
      provenance_path: $build_provenance,
      binary_sha256: $release_binary_sha
    },
    live_ready: ($result[0].live_ready // false),
    no_submit_policy: ($result[0].no_submit_policy // {}),
    env_audit: {
      path: ($result[0].env_audit_json // null),
      template: ($result[0].env_template_sh // null),
      ready: ($result[0].env_audit.summary.ready // false),
      blocking_count: ($result[0].env_audit.summary.blocking_count // null),
      missing_required_count: ($result[0].env_audit.summary.missing_required_count // null),
      warning_count: ($result[0].env_audit.summary.warning_count // null)
    },
    launch_config_fingerprint: {
      path: $launch_config_fingerprint_json,
      schema_version: ($result[0].launch_config_fingerprint.schema_version // null),
      algorithm: ($result[0].launch_config_fingerprint.algorithm // null),
      combined_fingerprint: ($result[0].launch_config_fingerprint.combined_fingerprint // null)
    },
    pass_summary: {
      cargo_status_ok: (($result[0].cargo_status // 1) == 0),
      process_pipeline_ok: (
        ($result[0].process_status.scanner // 1) == 0 and
        ($result[0].process_status.redactor // 1) == 0 and
        ($result[0].process_status.tee // 1) == 0
      ),
      runtime_panic_free: ($result[0].runtime_panic_scan.ok // false),
      live_ready: ($result[0].live_ready // false),
      env_audit_ready: ($result[0].env_audit.summary.ready // false),
      env_audit_blocking_count: ($result[0].env_audit.summary.blocking_count // null),
      paper_live_profile_config_safe: ($result[0].paper_live_profile_config_safe // false),
      no_live_submission_ok: ($result[0].no_live_submission.ok // false),
      artifact_secret_value_scan_ok: ($result[0].artifact_secret_value_scan.ok // false),
      live_trade_row_hits: ($result[0].no_live_submission.live_trade_row_hits // null),
      combo_execution_journal_hits: ($result[0].no_live_submission.combo_execution_journal_hits // null),
      standard_execution_journal_hits: ($result[0].no_live_submission.standard_execution_journal_hits // null),
      submit_marker_hits: ($result[0].no_live_submission.submit_marker_hits // null),
      artifact_secret_value_hits: ($result[0].artifact_secret_value_scan.hit_count // null)
    },
    reports: ($result[0].reports // {}),
    files: $files[0]
  }' >"$manifest_json"

while IFS= read -r -d '' proof_file; do
  for secret_env_name in "${secret_env_names[@]}"; do
    secret_value="${!secret_env_name:-}"
    if [[ "${#secret_value}" -ge 8 ]] && rg -q -F -- "$secret_value" "$proof_file"; then
      jq -n --arg env "$secret_env_name" --arg path "$proof_file" \
        '{env: $env, path: $path}' >>"$secret_hits_jsonl"
    fi
  done
done < <(printf '%s\0' "$result_json" "$manifest_json" "$manifest_files_json")
secret_hit_count="$(awk 'END { print NR + 0 }' "$secret_hits_jsonl")"

section "summary"
"$repo_root/scripts/verify-live-operator-preflight.sh" "$manifest_json" | tee "$manifest_verification_txt"
jq -r '
  "cargo_status=\(.cargo_status)",
  "runtime_panic_free=\(.runtime_panic_scan.ok) hits=\(.runtime_panic_scan.hit_count)",
  "live_ready=\(.live_ready)",
  "live_submissions_supported=\(.live_submissions_supported)",
  "all_live_checks_ready=\(.all_live_checks_ready)",
  "combo_promoted=\(.combo_promoted)",
  "env_audit_ready=\(.env_audit.summary.ready) blocking=\(.env_audit.summary.blocking_count) missing_required=\(.env_audit.summary.missing_required_count)",
  "no_live_submission=\(.no_live_submission.ok) trade_hits=\(.no_live_submission.live_trade_row_hits) journal_hits=\(.no_live_submission.combo_execution_journal_hits) marker_hits=\(.no_live_submission.submit_marker_hits)",
  "artifact_secret_value_scan=\(.artifact_secret_value_scan.ok) hits=\(.artifact_secret_value_scan.hit_count)",
  "run_root=\(.run_root)",
  "result_json=\(.result_json)",
  "manifest_json=\(.manifest_json)"
' "$result_json"
echo "manifest_verification=$manifest_verification_txt"

if [[ "$cargo_status" -ne 0 ]]; then
  echo "operator live preflight command failed; inspect $log_path" >&2
  exit 1
fi
if [[ "$runtime_panic_hit_count" -ne 0 ]]; then
  echo "operator live preflight runtime panic detected; inspect $runtime_panic_hits" >&2
  exit 1
fi
if [[ "$live_trade_hit_count" -ne 0 || "$combo_journal_hit_count" -ne 0 || "$submit_marker_hit_count" -ne 0 ]]; then
  echo "no-live submission proof failed; inspect hit files under $run_root" >&2
  exit 1
fi
if [[ "$secret_hit_count" -ne 0 ]]; then
  echo "artifact secret value scan failed; inspect $secret_hits_jsonl" >&2
  exit 1
fi
if [[ "$live_ready" != "true" && "$allow_live_blocked" -ne 1 ]]; then
  echo "live operator preflight blocked; rerun with --allow-live-blocked to collect no-submit proof only" >&2
  exit 1
fi

if [[ "$live_ready" == "true" ]]; then
  echo "operator live preflight ready; no live submissions attempted"
else
  echo "operator live preflight blocked; no live submissions attempted"
fi
