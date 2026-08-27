#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/verify-readiness-bundle.sh [--require-live-ready] <readiness-bundle-manifest.json>

Verifies readiness bundle integrity:
  - every manifest file exists
  - file sizes match
  - SHA-256 hashes match
  - live protocol drift report carries sourced expected/observed checks
  - no-live policy stayed false
  - paper/HFT/UI/no-live/fail-closed/secret-scan pass summary stayed clean
  - paper/live parity audit matches manifest summary
  - result_json matches manifest summary

Use --require-live-ready before building an activation packet. It requires paper
profitability, HFT speed, scanner decision-path parity, and no-submit evidence.
Real account/env readiness is verified separately by operator preflight.
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
need cargo
need cc
need cmp
need find
need head
need jq
need mktemp
need perl
need python3
need realpath
need rustc
need sed
need shasum
need sort
need wc

failures=0
work_root="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-readiness-bundle-verify.XXXXXX")"
cleanup() {
  rm -rf "$work_root"
}
trap cleanup EXIT

fail() {
  echo "bundle verification failed: $*" >&2
  failures=$((failures + 1))
}

manifest_abs="$(cd "$(dirname "$manifest")" && pwd)/$(basename "$manifest")"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_json="$(jq -r '.result_json // empty' "$manifest_abs")"
live_readiness_report="$(jq -r '.files[]? | select(.label == "live_readiness_report") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
paper_live_parity_audit="$(jq -r '.files[]? | select(.label == "paper_live_parity_audit") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
paper_profitability_report="$(jq -r '.files[]? | select(.label == "paper_profitability_report") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
release_binary="$(jq -r '.files[]? | select(.label == "release_binary") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
build_provenance="$(jq -r '.files[]? | select(.label == "build_provenance") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
no_live_identity_fingerprint="$(jq -r '.files[]? | select(.label == "no_live_identity_fingerprint") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
paper_profitability_source_trades="$(jq -r '.files[]? | select(.label == "paper_profitability_source_trades") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
paper_profitability_source_attempts="$(jq -r '.files[]? | select(.label == "paper_profitability_source_attempts") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
paper_adapter_provenance="$(jq -r '.files[]? | select(.label == "paper_adapter_provenance") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
paper_adapter_test_log="$(jq -r '.files[]? | select(.label == "paper_adapter_test_log") | .path' "$manifest_abs" | awk 'NR == 1 { print }')"
overall_state="$(jq -r '.overall_state // "unknown"' "$manifest_abs")"
file_count="$(jq '.files | length' "$manifest_abs")"
binary_sha=""
campaign_profit_compatibility_fingerprint=""

if [[ "$file_count" -le 0 ]]; then
  fail "manifest has no file entries"
fi

for required_label in \
  no_live_identity_fingerprint \
  paper_profitability_report \
  paper_profitability_source_trades \
  paper_profitability_source_attempts \
  paper_adapter_provenance \
  paper_adapter_test_log; do
  label_count="$(jq --arg label "$required_label" '[.files[]? | select(.label == $label)] | length' "$manifest_abs")"
  if [[ "$label_count" -ne 1 ]]; then
    fail "$required_label must have exactly one manifest entry; found=$label_count"
  fi
done

if [[ -z "$paper_adapter_test_log" || ! -f "$paper_adapter_test_log" ]]; then
  fail "paper adapter unit-proof log is missing"
else
  paper_adapter_running_count="$(rg -c '^running 1 test$' "$paper_adapter_test_log" || true)"
  if [[ "${paper_adapter_running_count:-0}" -ne 1 ]] \
    || ! rg -q '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out;' "$paper_adapter_test_log"; then
    fail "paper adapter unit proof did not execute exactly one passing test"
  fi
fi

if [[ -z "$no_live_identity_fingerprint" || ! -f "$no_live_identity_fingerprint" ]]; then
  fail "dotenv-resistant no-live identity proof is missing"
else
  jq -e '
    (.profit_compatibility_fingerprint | type == "string" and test("^0x[0-9a-f]{64}$")) and
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
    all(.direct_live_identities[];
      (keys_unsorted | sort) == ["name", "present"] and
      .present == false)
  ' "$no_live_identity_fingerprint" >/dev/null \
    || fail "dotenv-resistant no-live identity proof is not clean"

  campaign_profit_compatibility_fingerprint="$(jq -r '.profit_compatibility_fingerprint // empty' "$no_live_identity_fingerprint")"
  jq -e \
    --arg campaign_fingerprint "$campaign_profit_compatibility_fingerprint" \
    '
      .paper_execution_binding as $binding |
      $binding.campaign_profit_compatibility_fingerprint == $campaign_fingerprint and
      ($binding.profit_compatibility_fingerprint_values | type == "array") and
      ($binding.profit_compatibility_fingerprint_values | length) <= 1 and
      all($binding.profit_compatibility_fingerprint_values[];
        type == "string" and
        test("^0x[0-9a-f]{64}$") and
        . == $campaign_fingerprint)
    ' "$manifest_abs" >/dev/null \
    || fail "manifest campaign/evidence profit-compatibility fingerprints are not clean"
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

if [[ -z "$build_provenance" || ! -f "$build_provenance" ]]; then
  fail "build provenance missing: ${build_provenance:-<empty>}"
elif [[ -z "$release_binary" || ! -f "$release_binary" ]]; then
  fail "release binary missing: ${release_binary:-<empty>}"
else
  toolchain_env=(env -u RUSTUP_TOOLCHAIN -u RUSTC -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER -u RUSTFLAGS -u CARGO_BUILD_RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS -u CARGO_INCREMENTAL)
  current_rustc_verbose_version="$("${toolchain_env[@]}" rustc -vV)"
  current_host_target="$(sed -n 's/^host: //p' <<<"$current_rustc_verbose_version")"
  current_cargo_version="$("${toolchain_env[@]}" cargo --version)"
  current_cargo_verbose_version="$("${toolchain_env[@]}" cargo -vV)"
  current_rustc_version="$("${toolchain_env[@]}" rustc --version)"
  current_cc_path="$(command -v cc || true)"
  current_cc_version="$(cc --version 2>/dev/null | head -n 1 || true)"
  jq -e \
    --arg source_root "$repo_root" \
    --arg binary "$release_binary" \
    --arg host_target "$current_host_target" \
    --arg cargo_version "$current_cargo_version" \
    --arg cargo_verbose_version "$current_cargo_verbose_version" \
    --arg rustc_version "$current_rustc_version" \
    --arg rustc_verbose_version "$current_rustc_verbose_version" \
    --arg cc_path "$current_cc_path" \
    --arg cc_version "$current_cc_version" \
    '
    .schema_version == 2 and
    .source_root == $source_root and
    .build_command == ["cargo","build","--locked","--release","--bin","polymarket-arb-scanner","--target",$host_target] and
    .cargo_version == $cargo_version and
    .cargo_verbose_version == $cargo_verbose_version and
    .rustc_version == $rustc_version and
    .rustc_verbose_version == $rustc_verbose_version and
    .host_target == $host_target and
    .build_environment.isolated_target_dir == true and
    .build_environment.isolated_cargo_home == true and
    .build_environment.cargo_incremental == "0" and
    .build_environment.source_date_epoch == "0" and
    (.build_environment.deterministic_path_remapping as $remap |
      $remap.transport == "CARGO_ENCODED_RUSTFLAGS" and
      $remap.compiler_option == "--remap-path-prefix" and
      $remap.scope == "rustc_default_all" and
      $remap.explicit_scope_flag == false and
      $remap.physical_build_roots_recorded == true and
      $remap.source_root_portable == false and
      $remap.embedded_source_root_runtime_dependency == true and
      $remap.normalized_mappings == [
        {physical_role:"isolated_build_root",virtual:"/polymarket-build"},
        {physical_role:"source_root",virtual:"/polymarket-source"}
      ] and
      ($remap.builds | length) == 2 and
      $remap.builds[0].ordinal == 1 and
      $remap.builds[1].ordinal == 2 and
      $remap.builds[0].physical_build_root != $remap.builds[1].physical_build_root and
      all($remap.builds[];
        (.physical_build_root | type == "string" and startswith("/")) and
        .physical_build_root != $source_root and
        .cargo_home == (.physical_build_root + "/cargo-home") and
        .target_dir == (.physical_build_root + "/target") and
        .encoded_rustflags_argv == [
          ("--remap-path-prefix=" + .physical_build_root + "=/polymarket-build"),
          ("--remap-path-prefix=" + $source_root + "=/polymarket-source")
        ] and
        .ephemeral_path_scan == {
          clean:true,
          scanned_prefixes:[.physical_build_root,.cargo_home,.target_dir]
        }
      )
    ) and
    (["CARGO_HOME","CARGO_BUILD_RUSTFLAGS","CARGO_BUILD_RUSTC","CARGO_BUILD_RUSTC_WRAPPER","CARGO_BUILD_TARGET",
      "CARGO_ENCODED_RUSTFLAGS","CARGO_INCREMENTAL","CARGO_PROFILE_RELEASE_CODEGEN_UNITS","CARGO_PROFILE_RELEASE_DEBUG",
      "CARGO_PROFILE_RELEASE_INCREMENTAL","CARGO_PROFILE_RELEASE_LTO","CARGO_PROFILE_RELEASE_OPT_LEVEL",
      "CARGO_PROFILE_RELEASE_PANIC","CARGO_PROFILE_RELEASE_STRIP","CARGO_TARGET_DIR","RUSTC",
      "RUSTC_WRAPPER","RUSTC_WORKSPACE_WRAPPER","RUSTFLAGS","RUSTUP_TOOLCHAIN","SOURCE_DATE_EPOCH",
      "CC","CFLAGS","CXX","CXXFLAGS","AR","MACOSX_DEPLOYMENT_TARGET","SDKROOT"]
      - .build_environment.cleared_ambient_names | length) == 0 and
    ([.build_environment.cleared_ambient_names[]] | length) == ([.build_environment.cleared_ambient_names[]] | unique | length) and
    .build_environment.secret_env_names_cleared == [
      "POLYMARKET_PRIVATE_KEY","POLYMARKET_API_KEY","POLYMARKET_API_SECRET","POLYMARKET_API_PASSPHRASE",
      "CLOB_API_KEY","CLOB_SECRET","CLOB_PASS_PHRASE","CLOB_PASSPHRASE","LIVE_SIGNER_ADDRESS",
      "COMBO_RFQ_BEARER_TOKEN","COMBO_RFQ_PARTICIPANT_ID","COMBO_RFQ_STREAM_BEARER_TOKEN",
      "RELAYER_API_KEY","RELAYER_API_KEY_ADDRESS","LIVE_FUNDER_ADDRESS","POLYGON_RPC_URL","WEBHOOK_URL","BETDEX_AUTH_TOKEN"
    ] and
    (.build_environment.ambient_overrides_detected_and_cleared | type == "array") and
    all(.build_environment.ambient_overrides_detected_and_cleared[]; type == "string") and
    (.build_environment as $build_environment |
      all($build_environment.ambient_overrides_detected_and_cleared[];
        . as $name | ($build_environment.cleared_ambient_names | index($name)) != null)) and
    .native_toolchain == {cc_path:$cc_path,cc_version:$cc_version,fully_attested:false} and
    .reproducibility_check == {
      fresh_isolated_builds:2,
      byte_identical:true,
      second_binary_sha256:.binary.sha256
    } and
    .inputs_unchanged_during_build == true and
    .binary.path == $binary and
    (.binary.size_bytes | type == "number" and . > 0) and
    (.binary.sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.inputs | type == "array" and length > 0) and
    ([.inputs[].path] | length) == ([.inputs[].path] | unique | length) and
    all(.inputs[]; (
      (.path | type == "string") and
      ((.path | startswith("/")) | not) and
      (.path | split("/") | index("..") == null) and
      (.size_bytes | type == "number" and . >= 0) and
      (.sha256 | type == "string" and test("^[0-9a-f]{64}$"))
    ))
    ' "$build_provenance" >/dev/null || fail "build provenance schema is not clean"

  if LC_ALL=C rg -aFq -- "$repo_root" "$release_binary"; then
    :
  else
    source_scan_status=$?
    if [[ "$source_scan_status" -eq 1 ]]; then
      fail "release binary no longer carries the declared runtime source-root verifier binding"
    else
      fail "could not scan release binary for its runtime source-root verifier binding"
    fi
  fi
  while IFS= read -r ephemeral_path; do
    if LC_ALL=C rg -aFq -- "$ephemeral_path" "$release_binary"; then
      fail "release binary leaked attested ephemeral build path: $ephemeral_path"
    else
      path_scan_status=$?
      if [[ "$path_scan_status" -ne 1 ]]; then
        fail "could not scan release binary for attested ephemeral path: $ephemeral_path"
      fi
    fi
  done < <(jq -r '.build_environment.deterministic_path_remapping.builds[].ephemeral_path_scan.scanned_prefixes[]' "$build_provenance" | LC_ALL=C sort -u)

  expected_inputs="$work_root/expected-build-inputs.txt"
  provenance_inputs="$work_root/provenance-build-inputs.txt"
  (
    cd "$repo_root"
    {
      printf '%s\n' Cargo.toml Cargo.lock rust-toolchain.toml .env.example
      find . -maxdepth 1 -type f -name 'build.rs' -print
      if [[ -d .cargo ]]; then find .cargo -type f -print; fi
      find src -type f -name '*.rs' -print
      find scripts -maxdepth 1 -type f \( -name '*.sh' -o -name '*.py' \) -print
      find tests -type f \( -name '*.rs' -o -name '*.py' \) -print
      find dashboard \
        \( -path 'dashboard/node_modules' -o -path 'dashboard/dist' \) -prune \
        -o -type f -print
    } | LC_ALL=C sort -u
  ) >"$expected_inputs"
  jq -r '.inputs[].path' "$build_provenance" | LC_ALL=C sort -u >"$provenance_inputs"
  cmp -s "$expected_inputs" "$provenance_inputs" \
    || fail "build provenance input set does not match current build/safety inputs"

  while IFS=$'\t' read -r relative_path expected_size expected_sha; do
    input_path="$repo_root/$relative_path"
    if [[ ! -f "$input_path" ]]; then
      fail "build provenance input missing: $relative_path"
      continue
    fi
    actual_size="$(wc -c <"$input_path" | tr -d '[:space:]')"
    actual_sha="$(shasum -a 256 "$input_path" | awk '{print $1}')"
    [[ "$actual_size" == "$expected_size" ]] \
      || fail "build provenance input size mismatch: $relative_path"
    [[ "$actual_sha" == "$expected_sha" ]] \
      || fail "build provenance input hash mismatch: $relative_path"
  done < <(jq -r '.inputs[] | [.path, (.size_bytes|tostring), .sha256] | @tsv' "$build_provenance")

  binary_size="$(wc -c <"$release_binary" | tr -d '[:space:]')"
  binary_sha="$(shasum -a 256 "$release_binary" | awk '{print $1}')"
  jq -e \
    --arg binary "$release_binary" \
    --arg provenance "$build_provenance" \
    --argjson size "$binary_size" \
    --arg sha "$binary_sha" \
    --slurpfile build "$build_provenance" \
    '
    .build.binary_path == $binary and
    .build.provenance_path == $provenance and
    .build.binary_sha256 == $sha and
    .build.inputs_unchanged_during_build == true and
    $build[0].binary.path == $binary and
    $build[0].binary.size_bytes == $size and
    $build[0].binary.sha256 == $sha
    ' "$manifest_abs" >/dev/null || fail "manifest build provenance does not match binary"
  [[ -x "$release_binary" ]] || fail "release binary is not executable: $release_binary"
fi

jq -e '
  .no_live_policy.live_trade_attempted == false and
  .no_live_policy.account_created == false and
  .no_live_policy.credential_values_recorded == false and
    .pass_summary.paper_ready == true and
    .pass_summary.paper_execution_canary_ok == true and
    .pass_summary.paper_adapter_unit_proof_ok == true and
    .pass_summary.paper_scanner_trade_proof_ok == true and
    .pass_summary.paper_live_decision_path_parity_ok == true and
    .pass_summary.hft_ready == true and
    .pass_summary.ui_ready == true and
    (.pass_summary.paper_profitable_proven | type == "boolean") and
    (.pass_summary.paper_profitability_sample_count | type == "number") and
    (.pass_summary.hft_fastest_path_proven | type == "boolean") and
    (.pass_summary.paper_live_identical | type == "boolean") and
    .pass_summary.live_no_submission_ok == true and
  .pass_summary.global_no_live_scan_ok == true and
  .pass_summary.live_code_blocker_count == 0 and
  .pass_summary.source_static_blocker_count == 0 and
  .pass_summary.fail_closed_ok == true and
  .pass_summary.runtime_panic_free == true and
  .pass_summary.artifact_secret_scan_ok == true
' "$manifest_abs" >/dev/null || fail "manifest pass summary or no-live policy is not clean"

if [[ -z "$result_json" || ! -f "$result_json" ]]; then
  fail "result_json missing: ${result_json:-<empty>}"
else
  result_state="$(jq -r '.overall_state // "unknown"' "$result_json")"
  if [[ "$result_state" != "$overall_state" ]]; then
    fail "overall_state mismatch manifest=$overall_state result=$result_state"
  fi
  jq -e '
    .checks.paper.ready == true and
    .checks.paper.adapter_unit_proof.ok == true and
    (.checks.paper.adapter_unit_proof.test // "") == "external_paper_engine::tests::execute_opportunity_recomputes_zero_adapter_fees_from_clob_metadata" and
    .checks.paper.execution_canary.ok == true and
    .checks.paper.execution_canary.live_trade_attempted == false and
    (.checks.paper.execution_canary.trade_count // 0) > 0 and
    .checks.paper.scanner_trade_proof.ok == true and
    .checks.paper.scanner_trade_proof.synthetic == true and
    .checks.paper.scanner_trade_proof.counts_for_profitability == false and
    .checks.paper.scanner_trade_proof.live_trade_attempted == false and
    (.checks.paper.scanner_trade_proof.synthetic_plan_hash // "" | length) >= 8 and
    .checks.paper.scanner_trade_proof.synthetic_plan_hash_algorithm == "fnv1a64" and
    .checks.paper.scanner_trade_proof.decision_path_parity.ok == true and
    .checks.paper.scanner_trade_proof.decision_path_parity.live_submit_attempted == false and
    .checks.paper.scanner_trade_proof.decision_path_parity.hash_algorithm == "fnv1a64" and
    (.checks.paper.scanner_trade_proof.decision_path_parity.paper_decision_hash // "" | length) >= 8 and
    .checks.paper.scanner_trade_proof.decision_path_parity.paper_decision_hash == .checks.paper.scanner_trade_proof.decision_path_parity.live_decision_hash and
    (.checks.paper.scanner_trade_proof.paper_ok_rows // 0) > 0 and
    .checks.paper.scanner_trade_proof.scanner_can_execute_on_polymarket == true and
    (.checks.paper.profitability_evidence.verified_profitable | type == "boolean") and
    .checks.paper.profitability_evidence.future_profit_guaranteed == false and
    .checks.paper.profitability_evidence.live_profitability_proven == false and
    .checks.hft.ready == true and
    .checks.ui.ready == true and
    .checks.live.no_submission.ok == true and
    .checks.live.no_submission.global_scan.ok == true and
    .checks.live.no_submission.global_scan.live_trade_row_hits == 0 and
    .checks.live.no_submission.global_scan.combo_execution_journal_hits == 0 and
    .checks.live.no_submission.global_scan.standard_execution_journal_hits == 0 and
    .checks.live.no_submission.global_scan.submit_marker_hits == 0 and
    .checks.live.code_ceiling.code_blocker_count == 0 and
    .checks.live.code_ceiling.source_static_blocker_count == 0 and
    .checks.live.fail_closed_guard.ok == true and
    .checks.runtime_panic_scan.ok == true and
    .checks.runtime_panic_scan.hit_count == 0 and
    .checks.artifact_secret_scan.ok == true and
    .checks.artifact_secret_scan.hit_count == 0 and
    .checks.protocol.state == "ready" and
    (.checks.protocol.detail | contains("source_count=3"))
  ' "$result_json" >/dev/null || fail "result_json readiness/no-live checks are not clean"

  if [[ "$require_live_ready" -eq 1 ]]; then
    jq -e '
      (.overall_state == "ready" or .overall_state == "live_blocked") and
      .checks.paper.profitability_evidence.verified_profitable == true and
      .checks.live.no_submission.ok == true and
      .checks.live.code_ceiling.code_blocker_count == 0
    ' "$result_json" >/dev/null || fail "result_json activation-readiness evidence is not true"
  fi
fi

if [[ -z "$paper_profitability_report" || ! -f "$paper_profitability_report" ]]; then
  fail "paper_profitability_report missing: ${paper_profitability_report:-<empty>}"
else
  jq -e '
    .schema_version == 1 and
    (.verified_profitable | type == "boolean") and
    .future_profit_guaranteed == false and
    .live_profitability_proven == false and
    .source_snapshot == true and
    .attempts_source_snapshot == true and
    (.checks | type == "array") and
    (.blockers | type == "array") and
    (.source_sha256 | type == "string") and
    (.source_sha256 | length) == 64 and
    (.attempts_source_sha256 | type == "string") and
    (.attempts_source_sha256 | length) == 64 and
    (.execution_attempts | type == "object") and
    (.paper_evidence_eligible | type == "boolean") and
    .activation_eligible == false and
    .live_route_compatible == false and
    (.execution_binding | type == "object") and
    (.execution_binding.profit_compatibility_fingerprints | type == "array") and
    (.execution_binding.profit_compatibility_fingerprints | length) <= 1 and
    all(.execution_binding.profit_compatibility_fingerprints[];
      type == "string" and test("^0x[0-9a-f]{64}$")) and
    (.thresholds.min_event_lower_mean_pnl_usd | type == "number")
  ' "$paper_profitability_report" >/dev/null || fail "paper profitability report schema is invalid"

  if [[ -n "$campaign_profit_compatibility_fingerprint" ]]; then
    jq -e \
      --arg campaign_fingerprint "$campaign_profit_compatibility_fingerprint" \
      --slurpfile profitability "$paper_profitability_report" \
      '
        .paper_execution_binding.campaign_profit_compatibility_fingerprint == $campaign_fingerprint and
        .paper_execution_binding.profit_compatibility_fingerprint_values ==
          ($profitability[0].execution_binding.profit_compatibility_fingerprints // []) and
        all(.paper_execution_binding.profit_compatibility_fingerprint_values[];
          . == $campaign_fingerprint)
      ' "$manifest_abs" >/dev/null \
      || fail "manifest paper evidence fingerprints do not match campaign/evidence artifacts"
  fi
fi

paper_adapter_path=""
paper_adapter_sha=""
if [[ -z "$paper_adapter_provenance" || ! -f "$paper_adapter_provenance" ]]; then
  fail "paper adapter provenance is missing"
else
  paper_adapter_path="$(jq -r '.canonical_path // empty' "$paper_adapter_provenance")"
  paper_adapter_sha="$(jq -r '.executable_sha256 // empty' "$paper_adapter_provenance")"
  if [[ -z "$paper_adapter_path" || ! -x "$paper_adapter_path" ]]; then
    fail "paper adapter canonical executable is missing"
  else
    current_adapter_path="$(perl -MCwd=realpath -e 'print realpath($ARGV[0])' "$paper_adapter_path")"
    current_adapter_sha="$(shasum -a 256 "$paper_adapter_path" | awk '{print $1}')"
    jq -e \
      --arg path "$current_adapter_path" \
      --arg sha "$current_adapter_sha" \
      '
        .schema_version == 1 and
        .command == "pm-trader" and
        .canonical_path == $path and
        .executable_sha256 == $sha and
        (.trust_boundary | type == "string" and length > 0)
      ' "$paper_adapter_provenance" >/dev/null || fail "paper adapter provenance does not match current executable"
    paper_adapter_path="$current_adapter_path"
    paper_adapter_sha="$current_adapter_sha"
  fi
fi

if [[ "$require_live_ready" -eq 1 \
  && -n "$paper_profitability_report" && -f "$paper_profitability_report" \
  && -n "${binary_sha:-}" && -n "$paper_adapter_sha" ]]; then
  jq -e \
    --arg binary_sha "$binary_sha" \
    --arg adapter_sha "$paper_adapter_sha" \
    --arg adapter_path "$paper_adapter_path" \
    '
      .execution_binding.expected_producer_binary_sha256 == $binary_sha and
      .execution_binding.producer_executable_sha256 == [$binary_sha] and
      .execution_binding.expected_producer_matches == true and
      .execution_binding.expected_adapter_executable_sha256 == $adapter_sha and
      .execution_binding.external_paper_executable_sha256 == [$adapter_sha] and
      .execution_binding.expected_adapter_matches == true and
      .execution_binding.execution_profile.external_paper_executable_path == $adapter_path and
      .execution_binding.execution_profile.external_paper_executable_sha256 == $adapter_sha and
      .execution_binding.execution_profile.producer_executable_sha256 == $binary_sha and
      .execution_binding.uniform_campaign_binding == true and
      .execution_binding.parity_safe_profiles == true and
      .execution_binding.official_endpoints == true and
      .execution_binding.binding_error_count == 0 and
      (.execution_binding.binding_errors | length) == 0 and
      (.execution_binding.profit_compatibility_fingerprints | length) == 1 and
      (.execution_binding.profit_compatibility_fingerprints[0] | test("^0x[0-9a-f]{64}$")) and
      (.execution_binding.paper_live_profile_config | type == "object") and
      (.execution_binding.paper_live_profile_config_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
    ' "$paper_profitability_report" >/dev/null || fail "paper producer/adapter/profile binding is not clean"

  profile_sha_actual="$(python3 -c 'import hashlib,json,sys; d=json.load(open(sys.argv[1], encoding="utf-8"))["execution_binding"]["paper_live_profile_config"]; print(hashlib.sha256(json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest())' "$paper_profitability_report")"
  [[ "$profile_sha_actual" == "$(jq -r '.execution_binding.paper_live_profile_config_sha256 // empty' "$paper_profitability_report")" ]] \
    || fail "paper live profile config canonical hash mismatch"

  jq -e \
    --arg binary_sha "$binary_sha" \
    --arg adapter_sha "$paper_adapter_sha" \
    --slurpfile profitability "$paper_profitability_report" \
    --slurpfile adapter "$paper_adapter_provenance" \
    '
      .paper_execution_binding.producer_binary_sha256_values == [$binary_sha] and
      .paper_execution_binding.expected_producer_binary_sha256 == $binary_sha and
      .paper_execution_binding.paper_adapter == $adapter[0] and
      .paper_execution_binding.execution_profile_sha256_values == $profitability[0].execution_binding.execution_profile_sha256 and
      .paper_execution_binding.execution_profile == $profitability[0].execution_binding.execution_profile and
      .paper_execution_binding.paper_live_profile_config == $profitability[0].execution_binding.paper_live_profile_config and
      .paper_execution_binding.paper_live_profile_config_sha256 == $profitability[0].execution_binding.paper_live_profile_config_sha256 and
      .paper_execution_binding.campaign_profit_compatibility_fingerprint == $profitability[0].execution_binding.profit_compatibility_fingerprints[0] and
      .paper_execution_binding.profit_compatibility_fingerprint_values == $profitability[0].execution_binding.profit_compatibility_fingerprints and
      .paper_execution_binding.paper_evidence_eligible == ($profitability[0].paper_evidence_eligible // false) and
      .paper_execution_binding.live_route_compatible == false and
      .paper_execution_binding.activation_eligible_from_paper_alone == false and
      .pass_summary.paper_evidence_eligible == ($profitability[0].paper_evidence_eligible // false) and
      .pass_summary.paper_producer_binary_bound == true and
      .pass_summary.paper_campaign_binding_uniform == true
    ' "$manifest_abs" >/dev/null || fail "manifest paper execution binding does not match evidence"
fi

paper_profitability_source_trades_abs=""
paper_profitability_source_attempts_abs=""
paper_profitability_source_trades_sha=""
paper_profitability_source_attempts_sha=""
if [[ -z "$paper_profitability_source_trades" || ! -f "$paper_profitability_source_trades" \
  || -z "$paper_profitability_source_attempts" || ! -f "$paper_profitability_source_attempts" \
  || -z "$paper_profitability_report" || ! -f "$paper_profitability_report" ]]; then
  fail "paper profitability source snapshot inputs are missing"
else
  paper_profitability_source_trades_abs="$(cd "$(dirname "$paper_profitability_source_trades")" && pwd)/$(basename "$paper_profitability_source_trades")"
  paper_profitability_source_attempts_abs="$(cd "$(dirname "$paper_profitability_source_attempts")" && pwd)/$(basename "$paper_profitability_source_attempts")"
  paper_profitability_source_trades_sha="$(shasum -a 256 "$paper_profitability_source_trades" | awk '{print $1}')"
  paper_profitability_source_attempts_sha="$(shasum -a 256 "$paper_profitability_source_attempts" | awk '{print $1}')"
  jq -e \
    --arg trades_path "$paper_profitability_source_trades_abs" \
    --arg attempts_path "$paper_profitability_source_attempts_abs" \
    --arg trades_sha "$paper_profitability_source_trades_sha" \
    --arg attempts_sha "$paper_profitability_source_attempts_sha" \
    '
      .source == $trades_path and
      .attempts_source == $attempts_path and
      .source_sha256 == $trades_sha and
      .attempts_source_sha256 == $attempts_sha
    ' "$paper_profitability_report" >/dev/null \
    || fail "paper profitability report source path/hash does not match bundled snapshots"
fi

if [[ "$require_live_ready" -eq 1 ]]; then
  if [[ -z "$paper_profitability_source_trades" || ! -f "$paper_profitability_source_trades" \
    || -z "$paper_profitability_source_attempts" || ! -f "$paper_profitability_source_attempts" ]]; then
    fail "activation-readiness evidence is not true: profitability source snapshots are missing"
  else
    activation_gate_tmp="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-activation-gate.XXXXXX")"
    activation_gate_report="$activation_gate_tmp/report.json"
    activation_gate_log="$activation_gate_tmp/gate.log"
    if python3 "$repo_root/scripts/paper_profitability_gate.py" \
      --trades-csv "$paper_profitability_source_trades" \
      --attempts-jsonl "$paper_profitability_source_attempts" \
      --source-snapshot "$activation_gate_tmp/trades.csv" \
      --attempts-snapshot "$activation_gate_tmp/attempts.jsonl" \
      --output "$activation_gate_report" \
      --expected-producer-binary-sha256 "$binary_sha" \
      --expected-adapter-executable-sha256 "$paper_adapter_sha" \
      --activation-thresholds >"$activation_gate_log" 2>&1; then
      jq -e \
        --arg trades_sha "$paper_profitability_source_trades_sha" \
        --arg attempts_sha "$paper_profitability_source_attempts_sha" \
        --arg binary_sha "$binary_sha" \
        --arg adapter_sha "$paper_adapter_sha" \
        '
          .verified_profitable == true and
          .paper_evidence_eligible == true and
          .activation_eligible == false and
          .live_route_compatible == false and
          .execution_binding.expected_producer_binary_sha256 == $binary_sha and
          .execution_binding.producer_executable_sha256 == [$binary_sha] and
          .execution_binding.expected_producer_matches == true and
          .execution_binding.expected_adapter_executable_sha256 == $adapter_sha and
          .execution_binding.external_paper_executable_sha256 == [$adapter_sha] and
          .execution_binding.expected_adapter_matches == true and
          .threshold_profile == "activation" and
          .source_sha256 == $trades_sha and
          .attempts_source_sha256 == $attempts_sha and
          .thresholds == {
            min_trades: 100,
            min_unique_events: 30,
            min_observation_hours: 168,
            max_evidence_age_hours: 24,
            min_total_pnl_usd: 25,
            min_weighted_roi_pct: 0.25,
            min_lower_mean_pnl_usd: 0,
            min_event_lower_mean_pnl_usd: 0,
            min_fill_success_rate: 0.8,
            min_positive_trade_rate: 0.8,
            max_drawdown_usd: 25,
            max_unhedged_notional_usd: 0
          }
        ' "$activation_gate_report" >/dev/null \
        || fail "activation-readiness evidence is not true: fixed profitability report mismatch"
    else
      activation_gate_detail="$(tail -n 1 "$activation_gate_log" 2>/dev/null || true)"
      fail "activation-readiness evidence is not true: fixed profitability gate failed ${activation_gate_detail:-without detail}"
    fi
    rm -rf "$activation_gate_tmp"
  fi
fi

if [[ -z "$paper_live_parity_audit" || ! -f "$paper_live_parity_audit" \
  || -z "$paper_profitability_report" || ! -f "$paper_profitability_report" \
  || -z "$result_json" || ! -f "$result_json" ]]; then
  fail "paper profitability/parity cross-check inputs are missing"
else
  jq -e \
    --slurpfile parity "$paper_live_parity_audit" \
    --slurpfile profitability "$paper_profitability_report" \
    --slurpfile result "$result_json" \
    --arg trades_path "$paper_profitability_source_trades_abs" \
    --arg attempts_path "$paper_profitability_source_attempts_abs" \
    '
    .pass_summary.paper_profitable_proven == ($parity[0].verdict.paper_profitable_proven // false) and
    .pass_summary.paper_profitable_proven == ($profitability[0].verified_profitable // false) and
    .pass_summary.paper_profitability_sample_count == ($profitability[0].sample.accepted_trades // 0) and
    .pass_summary.hft_fastest_path_proven == ($parity[0].verdict.hft_fastest_path_proven // false) and
    .pass_summary.paper_live_identical == ($parity[0].verdict.paper_live_identical // false) and
    ($parity[0].verdict.paper_operational // false) == true and
    ($parity[0].verdict.scanner_paper_execution_path_proven // false) == true and
    ($parity[0].verdict.scanner_live_decision_path_parity_proven // false) == true and
    ($parity[0].verdict.scanner_no_missed_positive_raw_edge // false) == true and
    ($parity[0].verdict.live_no_submit_guard_proven // false) == true and
    ($parity[0].verdict.final_rest_guard_seen // false) == true and
    (($parity[0].paper.scanner_trade_proof.synthetic_plan_hash // "") == ($result[0].checks.paper.scanner_trade_proof.synthetic_plan_hash // "")) and
    (($parity[0].paper.scanner_trade_proof.synthetic_plan_hash // "") | length) >= 8 and
    ($parity[0].paper.scanner_trade_proof.synthetic_plan_hash_algorithm // "") == "fnv1a64" and
    ($parity[0].paper.scanner_trade_proof.decision_path_parity.ok // false) == true and
    ($parity[0].paper.scanner_trade_proof.decision_path_parity.paper_decision_hash // "") == ($result[0].checks.paper.scanner_trade_proof.decision_path_parity.paper_decision_hash // "") and
    ($parity[0].paper.scanner_trade_proof.decision_path_parity.paper_decision_hash // "") == ($parity[0].paper.scanner_trade_proof.decision_path_parity.live_decision_hash // "") and
    ($parity[0].paper.profitability_evidence.source_sha256 // "") == ($profitability[0].source_sha256 // "") and
    ($parity[0].paper.profitability_evidence.attempts_source_sha256 // "") == ($profitability[0].attempts_source_sha256 // "") and
    ($result[0].checks.paper.profitability_evidence.source // "") == $trades_path and
    ($result[0].checks.paper.profitability_evidence.attempts_source // "") == $attempts_path and
    ($result[0].checks.paper.profitability_evidence.source_trades_csv // "") == $trades_path and
    ($result[0].checks.paper.profitability_evidence.source_attempts_jsonl // "") == $attempts_path
  ' "$manifest_abs" >/dev/null || fail "paper/live parity audit does not match manifest summary"

  if [[ "$require_live_ready" -eq 1 ]]; then
    jq -e '
      .verdict.paper_operational == true and
      .verdict.scanner_paper_execution_path_proven == true and
      .verdict.scanner_live_decision_path_parity_proven == true and
      .verdict.scanner_no_missed_positive_raw_edge == true and
      .verdict.paper_profitable_proven == true and
      .paper.profitability_evidence.verified_profitable == true and
      .paper.profitability_evidence.future_profit_guaranteed == false and
      .verdict.hft_fastest_path_proven == true and
      .verdict.final_rest_guard_seen == true and
      .verdict.live_no_submit_guard_proven == true
    ' "$paper_live_parity_audit" >/dev/null || fail "paper/live evidence is not activation-ready"
  fi
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

if [[ "$require_live_ready" -eq 1 ]]; then
  jq -e '.overall_state == "ready" or .overall_state == "live_blocked"' "$manifest_abs" >/dev/null \
    || fail "manifest overall_state is not activation-safe"
fi

if [[ "$failures" -ne 0 ]]; then
  exit 1
fi

printf 'readiness_bundle_ok=1 manifest=%s overall_state=%s files=%s\n' \
  "$manifest_abs" "$overall_state" "$file_count"
