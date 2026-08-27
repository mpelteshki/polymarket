#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/verify-live-operator-preflight.sh [--require-live-ready] <live-operator-preflight-manifest.json>

Verifies operator preflight proof integrity:
  - every manifest file exists
  - file sizes match
  - SHA-256 hashes match
  - no-submit policy stayed false
  - live protocol drift report carries sourced expected/observed checks
  - live env audit exposes only redacted status fields
  - live env template keeps credential slots blank
  - no live trade rows, Combo/RFQ execution rows, or submit markers exist
  - raw secret env value scan stayed clean

Use --require-live-ready only when real operator live gates are expected to pass.
EOF
}

require_live_ready=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --require-live-ready)
      require_live_ready=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

manifest="${1:-}"
if [[ -z "$manifest" ]]; then
  usage >&2
  exit 2
fi
if [[ ! -f "$manifest" ]]; then
  echo "missing manifest: $manifest" >&2
  exit 2
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

need awk
need cmp
need jq
need mktemp
need sed
need shasum
need sort
need wc

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
failures=0
work_root="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-operator-verify.XXXXXX")"
trap 'rm -rf "$work_root"' EXIT
fail() {
  echo "operator preflight verification failed: $*" >&2
  failures=$((failures + 1))
}

manifest_abs="$(cd "$(dirname "$manifest")" && pwd)/$(basename "$manifest")"
result_json="$(jq -r '.result_json // empty' "$manifest_abs")"
env_audit_path="$(jq -r '.env_audit.path // empty' "$manifest_abs")"
env_template_path="$(jq -r '.env_audit.template // empty' "$manifest_abs")"
launch_config_fingerprint_path="$(jq -r '.files[]? | select(.label == "launch_config_fingerprint") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
launch_config_fingerprint_post_path="$(jq -r '.files[]? | select(.label == "launch_config_fingerprint_post") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
readiness_manifest="$(jq -r '.files[]? | select(.label == "readiness_manifest") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
release_binary="$(jq -r '.files[]? | select(.label == "release_binary") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
build_provenance="$(jq -r '.files[]? | select(.label == "build_provenance") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
live_readiness_report="$(jq -r '.files[]? | select(.label == "live_readiness_report") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
combo_route_promotion_report="$(jq -r '.files[]? | select(.label == "combo_rfq_route_promotion_report") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
live_route_calibration_report="$(jq -r '.files[]? | select(.label == "live_route_calibration_report") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
file_count="$(jq '.files | length' "$manifest_abs")"

if [[ "$file_count" -le 0 ]]; then
  fail "manifest has no file entries"
fi

if [[ -z "$launch_config_fingerprint_path" || ! -f "$launch_config_fingerprint_path" ]]; then
  fail "launch config fingerprint missing: ${launch_config_fingerprint_path:-<empty>}"
else
  jq -e '
    (keys_unsorted | sort) == [
      "algorithm",
      "combined_fingerprint",
      "config_field_count",
      "config_fingerprint",
      "direct_live_identities",
      "direct_live_identity_fingerprint",
      "paper_live_profile_config",
      "profit_compatibility_fingerprint",
      "schema_version"
    ] and
    .schema_version == 1 and
    .algorithm == "keccak256-domain-separated-v1" and
    (.config_field_count | type == "number" and . > 100) and
    (.config_fingerprint | type == "string" and test("^0x[0-9a-f]{64}$")) and
    (.direct_live_identity_fingerprint | type == "string" and test("^0x[0-9a-f]{64}$")) and
    (.combined_fingerprint | type == "string" and test("^0x[0-9a-f]{64}$")) and
    (.profit_compatibility_fingerprint | type == "string" and test("^0x[0-9a-f]{64}$")) and
    (.paper_live_profile_config | type == "object") and
    .paper_live_profile_config.schema_version == 1 and
    .paper_live_profile_config.execution_route == "legged_clob_paper" and
    (.paper_live_profile_config.order_mode | type == "string") and
    (.paper_live_profile_config.effective_order_type | type == "string") and
    (.paper_live_profile_config.live_order_type | type == "string") and
    (.paper_live_profile_config.paper_use_limit_orders_requested | type == "boolean") and
    (.paper_live_profile_config.effective_paper_use_limit_orders | type == "boolean") and
    (.paper_live_profile_config.full_clob_required | type == "boolean") and
    (.paper_live_profile_config.match_live_position_size | type == "boolean") and
    (.paper_live_profile_config.effective_position_size_usd | type == "number") and
    (.paper_live_profile_config.live_position_size_usd | type == "number") and
    (.paper_live_profile_config.paper_max_share_mismatch_pct | type == "number") and
    (.paper_live_profile_config.min_net_profit_usd | type == "number") and
    (.paper_live_profile_config.min_roi_pct | type == "number") and
    (.paper_live_profile_config.max_signal_age_secs | type == "number") and
    (.paper_live_profile_config.gas_fallback_usd | type == "number") and
    (.paper_live_profile_config.assume_gasless_for_proxy_signature_types | type == "boolean") and
    (.paper_live_profile_config.live_signature_type |
      type == "number" and . == floor and . >= 0 and . <= 3) and
    (.paper_live_profile_config.order_size_step_shares | type == "number") and
    (.paper_live_profile_config.validate_opportunities_at_target_size | type == "boolean") and
    (.paper_live_profile_config.execute_only_full_clob_prices | type == "boolean") and
    (.paper_live_profile_config.live_slippage_bps | type == "number") and
    (.paper_live_profile_config.live_edge_haircut_usd | type == "number") and
    (.paper_live_profile_config.live_edge_haircut_bps | type == "number") and
    (.paper_live_profile_config.live_min_leg_size_usd | type == "number") and
    (.paper_live_profile_config.live_max_refresh_to_submit_ms | type == "number") and
    (.paper_live_profile_config.clob_api_url | type == "string") and
    (.paper_live_profile_config.gamma_api_url | type == "string") and
    (.paper_live_profile_config.external_paper_command | type == "string") and
    (.paper_live_profile_config | keys_unsorted | sort) == [
      "assume_gasless_for_proxy_signature_types",
      "clob_api_url",
      "effective_order_type",
      "effective_paper_use_limit_orders",
      "effective_position_size_usd",
      "execute_only_full_clob_prices",
      "execution_route",
      "external_paper_command",
      "full_clob_required",
      "gamma_api_url",
      "gas_fallback_usd",
      "live_edge_haircut_bps",
      "live_edge_haircut_usd",
      "live_max_refresh_to_submit_ms",
      "live_min_leg_size_usd",
      "live_order_type",
      "live_position_size_usd",
      "live_signature_type",
      "live_slippage_bps",
      "match_live_position_size",
      "max_signal_age_secs",
      "min_net_profit_usd",
      "min_roi_pct",
      "order_mode",
      "order_size_step_shares",
      "paper_max_share_mismatch_pct",
      "paper_use_limit_orders_requested",
      "schema_version",
      "validate_opportunities_at_target_size"
    ] and
    (.direct_live_identities | map(.name)) == [
      "BETDEX_AUTH_TOKEN",
      "CLOB_API_KEY",
      "CLOB_PASSPHRASE",
      "CLOB_PASS_PHRASE",
      "CLOB_SECRET",
      "COMBO_RFQ_BEARER_TOKEN",
      "COMBO_RFQ_PARTICIPANT_ID",
      "COMBO_RFQ_STREAM_BEARER_TOKEN",
      "LIVE_FUNDER_ADDRESS",
      "LIVE_SIGNER_ADDRESS",
      "POLYGON_RPC_URL",
      "POLYMARKET_API_KEY",
      "POLYMARKET_API_PASSPHRASE",
      "POLYMARKET_API_SECRET",
      "POLYMARKET_PRIVATE_KEY",
      "RELAYER_API_KEY",
      "RELAYER_API_KEY_ADDRESS",
      "WEBHOOK_URL"
    ] and
    all(.direct_live_identities[]; (
      (keys_unsorted | sort) == ["name", "present"] and
      (.present | type == "boolean")
    ))
  ' "$launch_config_fingerprint_path" >/dev/null \
    || fail "launch config fingerprint schema is not clean"
fi

if [[ -z "$launch_config_fingerprint_post_path" || ! -f "$launch_config_fingerprint_post_path" \
  || ! -f "$launch_config_fingerprint_path" ]] \
  || ! cmp -s "$launch_config_fingerprint_path" "$launch_config_fingerprint_post_path"; then
  fail "pre/post diagnostics launch config fingerprints do not match"
fi

if [[ -z "$readiness_manifest" || ! -f "$readiness_manifest" ]]; then
  fail "referenced readiness manifest is missing"
elif ! "$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_manifest" >/dev/null 2>&1; then
  fail "referenced readiness bundle does not verify"
fi
paper_adapter_path="$(jq -r '.paper_execution_binding.paper_adapter.canonical_path // empty' "$readiness_manifest" 2>/dev/null || true)"
if [[ -z "$release_binary" || ! -x "$release_binary" || -z "$build_provenance" || ! -f "$build_provenance" ]]; then
  fail "operator preflight release binary/build provenance is missing"
else
  release_binary_sha="$(shasum -a 256 "$release_binary" | awk '{print $1}')"
  jq -e \
    --arg readiness "$readiness_manifest" \
    --arg binary "$release_binary" \
    --arg provenance "$build_provenance" \
    --arg sha "$release_binary_sha" \
    --slurpfile build "$build_provenance" \
    '
      .readiness_manifest == $readiness and
      .release_build == {
        binary_path: $binary,
        provenance_path: $provenance,
        binary_sha256: $sha
      } and
      $build[0].binary.path == $binary and
      $build[0].binary.sha256 == $sha
    ' "$manifest_abs" >/dev/null || fail "operator release build binding is not clean"
fi

while IFS=$'\t' read -r label path expected_exists expected_size expected_sha; do
  if [[ "$expected_exists" != "true" ]]; then
    fail "$label marked exists=$expected_exists"
    continue
  fi
  if [[ ! -f "$path" ]]; then
    fail "$label missing at $path"
    continue
  fi
  actual_size="$(wc -c <"$path" | tr -d '[:space:]')"
  actual_sha="$(shasum -a 256 "$path" | awk '{print $1}')"
  if [[ "$actual_size" != "$expected_size" ]]; then
    fail "$label size mismatch expected=$expected_size actual=$actual_size path=$path"
  fi
  if [[ -z "$expected_sha" || "$expected_sha" == "null" ]]; then
    fail "$label missing manifest sha path=$path"
  elif [[ "$actual_sha" != "$expected_sha" ]]; then
    fail "$label sha mismatch expected=$expected_sha actual=$actual_sha path=$path"
  fi
done < <(jq -r '.files[] | [.label, .path, (.exists | tostring), (.size_bytes | tostring), (.sha256 // "")] | @tsv' "$manifest_abs")

jq -e '
  .no_submit_policy.live_trading_enabled_forced == false and
  .no_submit_policy.account_created == false and
  .pass_summary.cargo_status_ok == true and
  .pass_summary.process_pipeline_ok == true and
  .pass_summary.runtime_panic_free == true and
  .pass_summary.no_live_submission_ok == true and
  .pass_summary.artifact_secret_value_scan_ok == true and
  .pass_summary.live_trade_row_hits == 0 and
  .pass_summary.combo_execution_journal_hits == 0 and
  .pass_summary.standard_execution_journal_hits == 0 and
  .pass_summary.submit_marker_hits == 0 and
  .pass_summary.artifact_secret_value_hits == 0
' "$manifest_abs" >/dev/null || fail "manifest no-submit summary is not clean"

if [[ -z "$env_audit_path" || ! -f "$env_audit_path" ]]; then
  fail "env audit missing: ${env_audit_path:-<empty>}"
else
  jq -e '
    (.purpose | type == "string" and test("values intentionally omitted")) and
    (.mode == "no_submit_preflight") and
    (.records | type == "array" and length == 29) and
    ([.records[].name] | sort) == ([
      "CLOB_API_KEY", "CLOB_PASS_PHRASE", "CLOB_SECRET",
      "COMBO_RFQ_ACCEPT_ENABLED", "COMBO_RFQ_BEARER_TOKEN", "COMBO_RFQ_EXCHANGE_V3_ADDRESS",
      "COMBO_RFQ_PARTICIPANT_ID", "COMBO_RFQ_REQUESTER_ENABLED",
      "COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED", "COMBO_RFQ_STREAM_BEARER_TOKEN",
      "COMBO_RFQ_STREAM_ENABLED", "LIVE_CLOSEOUT_DRY_RUN", "LIVE_CLOSEOUT_ENABLED",
      "LIVE_COMBO_RFQ_ROUTE_ENABLED", "LIVE_FUNDER_ADDRESS", "LIVE_SIGNATURE_TYPE",
      "LIVE_SIGNER_ADDRESS", "LIVE_TRADING_ENABLED", "LIVE_USER_WS_ENABLED",
      "ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED", "POLYGON_RPC_URL", "POLYMARKET_API_KEY",
      "POLYMARKET_API_PASSPHRASE", "POLYMARKET_API_SECRET", "POLYMARKET_PRIVATE_KEY",
      "RELAYER_API_KEY", "RELAYER_API_KEY_ADDRESS", "RELAYER_API_URL", "SETTLEMENT_MONITOR_ENABLED"
    ] | sort) and
    ([.records[].name] | length) == ([.records[].name] | unique | length) and
    all(.records[]; (
      (keys_unsorted | sort) == ["blocking","credential","expected","group","issue","name","ok","present","required"] and
      (.name | type == "string") and
      (.group | type == "string") and
      (.credential | type == "boolean") and
      (.required | type == "boolean") and
      (.blocking | type == "boolean") and
      (.present | type == "boolean") and
      (.ok | type == "boolean") and
      (.expected | type == "string") and
      ((.issue == null) or (.issue | type == "string"))
    )) and
    .summary == {
      total_count: (.records | length),
      required_count: ([.records[] | select(.required == true)] | length),
      credential_count: ([.records[] | select(.credential == true)] | length),
      present_required_count: ([.records[] | select(.required == true and .present == true)] | length),
      missing_required_count: ([.records[] | select(.required == true and .present != true)] | length),
      invalid_required_count: ([.records[] | select(.required == true and .present == true and .ok != true)] | length),
      blocking_count: ([.records[] | select(.blocking == true and .ok != true)] | length),
      warning_count: ([.records[] | select(.blocking != true and .ok != true)] | length),
      ready: (([.records[] | select(.blocking == true and .ok != true)] | length) == 0)
    } and
    .missing_required == [.records[] | select(.required == true and .present != true) | .name] and
    .blocking == [.records[] | select(.blocking == true and .ok != true) | {name, group, issue, expected}] and
    (.blocking | type == "array") and
    all(.blocking[]; (
      ((keys_unsorted - ["name","group","issue","expected"]) | length) == 0 and
      (.name | type == "string") and
      (.group | type == "string") and
      (.expected | type == "string") and
      ((.issue == null) or (.issue | type == "string"))
    )) and
    (.missing_required | type == "array") and
    all(.missing_required[]; type == "string")
  ' "$env_audit_path" >/dev/null || fail "env audit redaction schema is not clean"
fi

if [[ -z "$live_readiness_report" || ! -f "$live_readiness_report" ]]; then
  fail "live_readiness_report missing: ${live_readiness_report:-<empty>}"
else
  jq -e '
    .protocol_drift.status == "ready" and
    (.protocol_drift.source_urls | type == "array") and
    (.protocol_drift.source_urls | index("https://docs.polymarket.com/resources/contracts") != null) and
    (.protocol_drift.source_urls | index("https://docs.polymarket.com/developers/CLOB/introduction") != null) and
    (.protocol_drift.source_urls | index("https://docs.polymarket.com/market-makers/combos") != null) and
    ([
      .protocol_drift.checks[]?
      | select(
          .key == "combo_rfq_api_url" and
          .state == "ready" and
          .expected == "https://combos-rfq-api.polymarket.sh" and
          .observed == "https://combos-rfq-api.polymarket.sh" and
          .source_url == "https://docs.polymarket.com/market-makers/combos"
        )
    ] | length == 1) and
    ([
      .protocol_drift.checks[]?
      | select(
          .key == "combo_rfq_gateway_wss_url" and
          .state == "ready" and
          ((.expected // "") | split(",") | index("wss://combos-rfq-gateway-quoter.polymarket.sh/ws/rfq") != null) and
          .observed == "wss://combos-rfq-gateway-quoter.polymarket.sh/ws/rfq" and
          .source_url == "https://docs.polymarket.com/market-makers/combos"
        )
    ] | length == 1)
  ' "$live_readiness_report" >/dev/null || fail "live_readiness_report protocol drift evidence is not clean"
fi

if [[ -z "$env_template_path" || ! -f "$env_template_path" ]]; then
  fail "env template missing: ${env_template_path:-<empty>}"
elif [[ -n "$env_audit_path" && -f "$env_audit_path" ]]; then
  if ! awk '
    /^$/ || /^#[[:space:]]/ || /^export [A-Z][A-Z0-9_]*="(true|false|0)?"$/ { next }
    { exit 1 }
  ' "$env_template_path"; then
    fail "env template contains non-template shell syntax"
  fi
  expected_template_names="$work_root/expected-template-names.txt"
  actual_template_names="$work_root/actual-template-names.txt"
  jq -r '.records[].name' "$env_audit_path" | sort >"$expected_template_names"
  sed -n 's/^export \([A-Z][A-Z0-9_]*\)=.*/\1/p' "$env_template_path" | sort >"$actual_template_names"
  cmp -s "$expected_template_names" "$actual_template_names" \
    || fail "env template export names/count do not exactly match audit records"
  while IFS=$'\t' read -r record_name credential expected; do
    expected_value=""
    if [[ "$record_name" == "LIVE_TRADING_ENABLED" ]]; then
      expected_value="false"
    elif [[ "$credential" == "true" ]]; then
      expected_value=""
    elif [[ "$expected" == "true" ]]; then
      expected_value="true"
    elif [[ "$expected" == "false" ]]; then
      expected_value="false"
    elif [[ "$expected" == "0|1|2|3" ]]; then
      expected_value="0"
    fi
    expected_line="export ${record_name}=\"${expected_value}\""
    if [[ "$(awk -v expected="$expected_line" '$0 == expected { count++ } END { print count + 0 }' "$env_template_path")" -ne 1 ]]; then
      fail "env template export is not the exact generated value: $record_name"
    fi
  done < <(jq -r '.records[] | [.name, (.credential | tostring), .expected] | @tsv' "$env_audit_path")
fi

if [[ "$require_live_ready" -eq 1 ]]; then
  jq -e '
    .live_ready == true and
    .pass_summary.live_ready == true and
    .env_audit.ready == true and
    .pass_summary.env_audit_ready == true and
    .pass_summary.env_audit_blocking_count == 0
  ' "$manifest_abs" >/dev/null \
    || fail "manifest live_ready is not true"
fi

if [[ -z "$result_json" || ! -f "$result_json" ]]; then
  fail "result_json missing: ${result_json:-<empty>}"
else
  jq -e \
    --arg env_audit_path "$env_audit_path" \
    --arg env_template_path "$env_template_path" \
    --arg live_readiness_report "$live_readiness_report" \
    --arg combo_route_promotion_report "$combo_route_promotion_report" \
    --arg live_route_calibration_report "$live_route_calibration_report" \
    --arg launch_config_fingerprint_path "$launch_config_fingerprint_path" \
    --arg repo_root "$repo_root" \
    --arg paper_adapter_path "$paper_adapter_path" \
    --slurpfile launch_config_fingerprint "$launch_config_fingerprint_path" \
    '
    .no_submit_policy.live_trading_enabled_forced == false and
    .no_submit_policy.account_created == false and
    .env_audit_json == $env_audit_path and
    .env_template_sh == $env_template_path and
    .reports.live_readiness_report == $live_readiness_report and
    .reports.combo_rfq_route_promotion_report == $combo_route_promotion_report and
    .reports.live_route_calibration_report == $live_route_calibration_report and
    .launch_config_fingerprint == $launch_config_fingerprint[0] and
    .effective_launch_env == {
      working_directory: $repo_root,
      diagnostics_dir: .live_dir,
      live_diagnostics_enabled: true,
      paper_trading_enabled: false,
      external_paper_command: $paper_adapter_path
    } and
    .cargo_status == 0 and
    .process_status == {scanner: 0, redactor: 0, tee: 0} and
    .runtime_panic_scan.ok == true and
    .runtime_panic_scan.hit_count == 0 and
    .no_live_submission.ok == true and
    .no_live_submission.live_trade_row_hits == 0 and
    .no_live_submission.combo_execution_journal_hits == 0 and
    .no_live_submission.standard_execution_journal_hits == 0 and
    .no_live_submission.submit_marker_hits == 0 and
    .artifact_secret_value_scan.ok == true and
    .artifact_secret_value_scan.hit_count == 0 and
    (.paper_live_profile_config_safe | type == "boolean")
  ' "$result_json" >/dev/null || fail "result_json no-submit checks are not clean"
  if [[ "$require_live_ready" -eq 1 ]]; then
    jq -e '
      .live_ready == true and
      .live_submissions_supported == true and
      .all_live_checks_ready == true and
      .combo_promoted == true and
      .paper_live_profile_config_safe == true and
      (.live_not_ready_checks | type == "array" and length == 0) and
      (.combo_blockers | type == "array" and length == 0) and
      .env_audit.summary.ready == true and
      .env_audit.summary.blocking_count == 0
    ' "$result_json" >/dev/null || fail "result_json underlying live readiness is not clean"
  fi
fi

if [[ -n "$launch_config_fingerprint_path" && -f "$launch_config_fingerprint_path" ]]; then
  jq -e \
    --arg path "$launch_config_fingerprint_path" \
    --slurpfile launch_config_fingerprint "$launch_config_fingerprint_path" \
    '
    .launch_config_fingerprint.path == $path and
    .launch_config_fingerprint.schema_version == $launch_config_fingerprint[0].schema_version and
    .launch_config_fingerprint.algorithm == $launch_config_fingerprint[0].algorithm and
    .launch_config_fingerprint.combined_fingerprint == $launch_config_fingerprint[0].combined_fingerprint
    and .pass_summary.paper_live_profile_config_safe == (
      $launch_config_fingerprint[0].paper_live_profile_config.schema_version == 1 and
      $launch_config_fingerprint[0].paper_live_profile_config.execution_route == "legged_clob_paper" and
      $launch_config_fingerprint[0].paper_live_profile_config.order_mode == "market_style" and
      $launch_config_fingerprint[0].paper_live_profile_config.effective_order_type == "fok" and
      $launch_config_fingerprint[0].paper_live_profile_config.live_order_type == "fok" and
      $launch_config_fingerprint[0].paper_live_profile_config.effective_paper_use_limit_orders == false and
      $launch_config_fingerprint[0].paper_live_profile_config.full_clob_required == true and
      $launch_config_fingerprint[0].paper_live_profile_config.match_live_position_size == true and
      ($launch_config_fingerprint[0].paper_live_profile_config.effective_position_size_usd > 0) and
      $launch_config_fingerprint[0].paper_live_profile_config.effective_position_size_usd == $launch_config_fingerprint[0].paper_live_profile_config.live_position_size_usd and
      ($launch_config_fingerprint[0].paper_live_profile_config.paper_max_share_mismatch_pct >= 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.paper_max_share_mismatch_pct <= 0.5) and
      ($launch_config_fingerprint[0].paper_live_profile_config.min_net_profit_usd > 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.min_roi_pct > 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.max_signal_age_secs > 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.gas_fallback_usd >= 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.live_signature_type >= 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.live_signature_type <= 3) and
      ($launch_config_fingerprint[0].paper_live_profile_config.live_signature_type ==
        ($launch_config_fingerprint[0].paper_live_profile_config.live_signature_type | floor)) and
      ($launch_config_fingerprint[0].paper_live_profile_config.order_size_step_shares > 0) and
      $launch_config_fingerprint[0].paper_live_profile_config.validate_opportunities_at_target_size == true and
      $launch_config_fingerprint[0].paper_live_profile_config.execute_only_full_clob_prices == true and
      ($launch_config_fingerprint[0].paper_live_profile_config.live_edge_haircut_usd >= 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.live_min_leg_size_usd > 0) and
      ($launch_config_fingerprint[0].paper_live_profile_config.live_max_refresh_to_submit_ms > 0) and
      $launch_config_fingerprint[0].paper_live_profile_config.clob_api_url == "https://clob.polymarket.com" and
      $launch_config_fingerprint[0].paper_live_profile_config.gamma_api_url == "https://gamma-api.polymarket.com" and
      ($launch_config_fingerprint[0].paper_live_profile_config.external_paper_command == "pm-trader" or
        ($launch_config_fingerprint[0].paper_live_profile_config.external_paper_command | endswith("/pm-trader")))
    )
    ' "$manifest_abs" >/dev/null || fail "manifest launch config fingerprint does not match artifact"
fi

if [[ -n "$live_route_calibration_report" && -f "$live_route_calibration_report" \
  && -n "$combo_route_promotion_report" && -f "$combo_route_promotion_report" ]]; then
  jq -e \
    --slurpfile calibration "$live_route_calibration_report" \
    '
      ($calibration[0].min_required_samples | type == "number") and
      ($calibration[0].labeled_replay_samples | type == "number") and
      ($calibration[0].realized_ev_samples | type == "number") and
      ($calibration[0].risk_gate_pass | type == "boolean") and
      ($calibration[0].routes | type == "array") and
      ([.checks[] | select(.key == "combo_rfq_replay_calibration")] | length) == 1 and
      ([.checks[] | select(.key == "combo_rfq_replay_calibration")][0] as $check |
        [$calibration[0].routes[] | select(.route == "combo_rfq_candidate")] as $routes |
        if ($routes | length) == 1 and $routes[0].risk_gate_pass == true then
          $check.state == "ready" and
          $check.detail == ("labeled_samples=" + ($routes[0].labeled_samples | tostring))
        else
          $check.state != "ready"
        end
      )
    ' "$combo_route_promotion_report" >/dev/null \
      || fail "named Combo/RFQ replay calibration check does not match calibration report"
fi

if [[ "$require_live_ready" -eq 1 ]]; then
  if [[ -n "$live_readiness_report" && -f "$live_readiness_report" ]]; then
    jq -e '
      .live_submissions_supported == true and
      (.checks | type == "array" and length > 0) and
      all(.checks[]; .state == "ready")
    ' "$live_readiness_report" >/dev/null || fail "live readiness report has blocked checks"
  fi
  if [[ -z "$combo_route_promotion_report" || ! -f "$combo_route_promotion_report" ]]; then
    fail "combo route promotion report missing: ${combo_route_promotion_report:-<empty>}"
  else
    jq -e '
      .promoted == true and
      (.blockers | type == "array" and length == 0) and
      ([.checks[] | select(.key == "combo_rfq_replay_calibration")] | length) == 1 and
      ([.checks[] | select(.key == "combo_rfq_replay_calibration")][0].state == "ready") and
      ([.checks[] | select(.key == "combo_rfq_replay_calibration")][0].detail | test("^labeled_samples=[0-9]+$"))
    ' "$combo_route_promotion_report" >/dev/null || fail "combo route promotion is not ready"
  fi
  if [[ -z "$live_route_calibration_report" || ! -f "$live_route_calibration_report" ]]; then
    fail "live route calibration report missing: ${live_route_calibration_report:-<empty>}"
  else
    jq -e '
      .min_required_samples >= 100 and
      .labeled_replay_samples >= 100 and
      .realized_ev_samples >= 100 and
      .risk_gate_pass == true and
      (.blockers | type == "array" and length == 0) and
      ([.routes[] | select(.route == "combo_rfq_candidate")] | length) == 1 and
      ([.routes[] | select(.route == "combo_rfq_candidate")][0] as $combo |
        $combo.min_required_samples >= 100 and
        $combo.labeled_samples >= $combo.min_required_samples and
        $combo.realized_ev_samples >= $combo.min_required_samples and
        $combo.risk_gate_pass == true and
        ($combo.blockers | type == "array" and length == 0) and
        ($combo.p_one_leg_fill_observed | type == "number" and . <= 0.005) and
        ($combo.p_ghost_revert_observed | type == "number" and . <= 0.001) and
        ($combo.avg_realized_ev_usd | type == "number") and
        ($combo.latest_label_at | type == "string" and length > 0)
      )
    ' "$live_route_calibration_report" >/dev/null \
      || fail "named Combo/RFQ replay calibration is not activation-ready"
  fi
fi

if [[ "$failures" -ne 0 ]]; then
  exit 1
fi

live_ready="$(jq -r '.live_ready // false' "$manifest_abs")"
printf 'operator_preflight_ok=1 manifest=%s live_ready=%s files=%s\n' \
  "$manifest_abs" "$live_ready" "$file_count"
