#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/readiness-verifier-selftest.sh [--readiness-manifest PATH] [--operator-preflight-manifest PATH] [--output PATH]

Self-tests readiness verifiers without live trading:
  - clean readiness/operator bundles verify
  - required-live verifiers match blocked or live-ready input state
  - live-ready-gate JSON matches blocked or live-ready input state
  - tampered sourced protocol evidence is rejected
  - tampered release hashes, remap policy, isolated roots, compiler flags, and path scans are rejected
  - tampered campaign or paper-evidence profit-compatibility fingerprints are rejected
  - a paper adapter proof that ran zero tests is rejected
  - tampered live env template credential value is rejected
  - shell expansion in any live env template value is rejected
  - masked operator runtime panics are rejected
  - tampered launch config fingerprints are rejected
  - tampered underlying operator live-readiness evidence is rejected
  - tampered named Combo/RFQ replay calibration evidence is rejected
  - live activation packet verifier accepts clean packets and rejects tampering
  - activation packet live-start command cannot omit enforced --no-paper
  - guarded live launcher contains the fixed --no-paper argument and rejects --paper extras
  - guarded live launcher rejects unbound runtime CLI overrides
  - guarded live launcher fixes repository CWD and proof-bound runtime env
  - guarded live launcher refuses a deliberately tampered activation packet

If paths are omitted, latest /tmp readiness/operator manifests are used.
EOF
}

readiness_manifest=""
operator_manifest=""
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

need awk
need date
need find
need jq
need mktemp
need shasum
need sort
need stat
need wc

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_root="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-readiness-verifier-selftest.XXXXXX")"
checks_jsonl="$work_root/checks.jsonl"
report_json="$work_root/readiness-verifier-selftest-report.json"
: >"$checks_jsonl"
if [[ -z "$report_output" ]]; then
  report_output="${TMPDIR:-/tmp}/polymarket-readiness-verifier-selftest-report-$(date +%s).json"
fi

cleanup() {
  rm -rf "$work_root"
}
trap cleanup EXIT

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
if [[ ! -f "$readiness_manifest" ]]; then
  echo "missing readiness manifest: $readiness_manifest" >&2
  exit 2
fi
if [[ ! -f "$operator_manifest" ]]; then
  echo "missing operator preflight manifest: $operator_manifest" >&2
  exit 2
fi

json_bool() {
  if [[ "$1" -eq 0 ]]; then
    echo true
  else
    echo false
  fi
}

record_check() {
  local name="$1"
  local expected="$2"
  local rc="$3"
  local output_path="$4"
  local ok="$5"
  jq -n \
    --arg name "$name" \
    --arg expected "$expected" \
    --arg output_path "$output_path" \
    --argjson rc "$rc" \
    --argjson ok "$ok" \
    '{name: $name, expected: $expected, rc: $rc, ok: $ok, output: $output_path}' >>"$checks_jsonl"
}

run_expect_pass() {
  local name="$1"
  shift
  local output="$work_root/${name//[^A-Za-z0-9_.-]/_}.out"
  set +e
  "$@" >"$output" 2>&1
  local rc=$?
  set -e
  local ok=1
  if [[ "$rc" -ne 0 ]]; then
    ok=0
  fi
  record_check "$name" "pass" "$rc" "$output" "$(json_bool "$((1 - ok))")"
  if [[ "$ok" -ne 1 ]]; then
    echo "selftest failed: $name expected pass rc=$rc" >&2
    sed 's/^/  /' "$output" >&2
    exit 1
  fi
}

run_expect_fail() {
  local name="$1"
  local pattern="$2"
  shift 2
  local output="$work_root/${name//[^A-Za-z0-9_.-]/_}.out"
  set +e
  "$@" >"$output" 2>&1
  local rc=$?
  set -e
  local pattern_ok=1
  if [[ -n "$pattern" ]]; then
    pattern_ok=0
    if grep -E -- "$pattern" "$output" >/dev/null; then
      pattern_ok=1
    fi
  fi
  local ok=1
  if [[ "$rc" -eq 0 || "$pattern_ok" -ne 1 ]]; then
    ok=0
  fi
  record_check "$name" "fail" "$rc" "$output" "$(json_bool "$((1 - ok))")"
  if [[ "$ok" -ne 1 ]]; then
    echo "selftest failed: $name expected failure matching pattern '$pattern' rc=$rc" >&2
    sed 's/^/  /' "$output" >&2
    exit 1
  fi
}

run_expect_input_state() {
  local name="$1"
  local should_pass="$2"
  local failure_pattern="$3"
  shift 3
  if [[ "$should_pass" -eq 1 ]]; then
    run_expect_pass "$name" "$@"
  else
    run_expect_fail "$name" "$failure_pattern" "$@"
  fi
}

file_sha() {
  shasum -a 256 "$1" | awk '{print $1}'
}

file_size() {
  wc -c <"$1" | tr -d '[:space:]'
}

replace_manifest_file_entry() {
  local manifest="$1"
  local label="$2"
  local path="$3"
  local tmp="$manifest.tmp"
  local size sha
  size="$(file_size "$path")"
  sha="$(file_sha "$path")"
  jq \
    --arg label "$label" \
    --arg path "$path" \
    --argjson size "$size" \
    --arg sha "$sha" \
    '.files |= map(if .label == $label then .path = $path | .exists = true | .size_bytes = $size | .sha256 = $sha else . end)' \
    "$manifest" >"$tmp"
  mv "$tmp" "$manifest"
}

readiness_result_json="$(jq -r '.result_json // empty' "$readiness_manifest")"
readiness_parity_audit="$(jq -r '.files[]? | select(.label == "paper_live_parity_audit") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
readiness_activation_ready=0
if [[ -f "$readiness_result_json" && -f "$readiness_parity_audit" ]] \
  && jq -e '
    (.overall_state == "ready" or .overall_state == "live_blocked") and
    .pass_summary.paper_profitable_proven == true and
    .pass_summary.hft_fastest_path_proven == true
  ' "$readiness_manifest" >/dev/null \
  && jq -e '
    (.overall_state == "ready" or .overall_state == "live_blocked") and
    .checks.paper.profitability_evidence.verified_profitable == true and
    .checks.live.no_submission.ok == true and
    .checks.live.code_ceiling.code_blocker_count == 0
  ' "$readiness_result_json" >/dev/null \
  && jq -e '
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
  ' "$readiness_parity_audit" >/dev/null; then
  readiness_activation_ready=1
fi

operator_result_json="$(jq -r '.result_json // empty' "$operator_manifest")"
operator_live_readiness_report="$(jq -r '.files[]? | select(.label == "live_readiness_report") | .path' "$operator_manifest" | awk 'NR == 1 { print }')"
operator_combo_promotion_report="$(jq -r '.files[]? | select(.label == "combo_rfq_route_promotion_report") | .path' "$operator_manifest" | awk 'NR == 1 { print }')"
operator_route_calibration_report="$(jq -r '.files[]? | select(.label == "live_route_calibration_report") | .path' "$operator_manifest" | awk 'NR == 1 { print }')"
operator_launch_config_fingerprint="$(jq -r '.files[]? | select(.label == "launch_config_fingerprint") | .path' "$operator_manifest" | awk 'NR == 1 { print }')"
operator_activation_ready=0
if [[ -f "$operator_result_json" && -f "$operator_live_readiness_report" && -f "$operator_combo_promotion_report" && -f "$operator_route_calibration_report" && -f "$operator_launch_config_fingerprint" ]] \
  && jq -e '
    .live_ready == true and
    .pass_summary.live_ready == true and
    .env_audit.ready == true and
    .pass_summary.env_audit_ready == true and
    .pass_summary.env_audit_blocking_count == 0
  ' "$operator_manifest" >/dev/null \
  && jq -e '
    .live_ready == true and
    .live_submissions_supported == true and
    .all_live_checks_ready == true and
    .combo_promoted == true and
    (.live_not_ready_checks | type == "array" and length == 0) and
    (.combo_blockers | type == "array" and length == 0) and
    .env_audit.summary.ready == true and
    .env_audit.summary.blocking_count == 0
  ' "$operator_result_json" >/dev/null \
  && jq -e '
    .live_submissions_supported == true and
    (.checks | type == "array" and length > 0) and
    all(.checks[]; .state == "ready")
  ' "$operator_live_readiness_report" >/dev/null \
  && jq -e '
    .promoted == true and
    (.blockers | type == "array" and length == 0) and
    ([.checks[] | select(.key == "combo_rfq_replay_calibration")][0].state == "ready")
  ' "$operator_combo_promotion_report" >/dev/null \
  && jq -e '
    .min_required_samples >= 100 and
    .labeled_replay_samples >= 100 and
    .risk_gate_pass == true and
    ([.routes[] | select(.route == "combo_rfq_candidate" and .risk_gate_pass == true and .labeled_samples >= 100)] | length) == 1
  ' "$operator_route_calibration_report" >/dev/null; then
  operator_activation_ready=1
fi

input_live_ready=0
if [[ "$readiness_activation_ready" -eq 1 && "$operator_activation_ready" -eq 1 ]]; then
  input_live_ready=1
fi

run_expect_pass "readiness_bundle_baseline" \
  "$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_manifest"
run_expect_pass "operator_preflight_baseline" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" "$operator_manifest"
run_expect_input_state "readiness_bundle_require_live_ready_matches_input" "$readiness_activation_ready" "activation-readiness evidence is not true|paper/live evidence is not activation-ready|overall_state is not activation-safe" \
  "$repo_root/scripts/verify-readiness-bundle.sh" --require-live-ready "$readiness_manifest"
run_expect_input_state "operator_preflight_require_live_ready_matches_input" "$operator_activation_ready" "live_ready is not true|underlying live readiness is not clean|live readiness report has blocked checks|combo route promotion is not ready" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" --require-live-ready "$operator_manifest"

readiness_campaign_fingerprint_tamper_manifest="$work_root/readiness-campaign-fingerprint-tamper-manifest.json"
jq \
  '.paper_execution_binding.campaign_profit_compatibility_fingerprint =
    "0x0000000000000000000000000000000000000000000000000000000000000000"' \
  "$readiness_manifest" >"$readiness_campaign_fingerprint_tamper_manifest"
run_expect_fail "readiness_bundle_rejects_campaign_fingerprint_tamper" "campaign/evidence profit-compatibility fingerprints are not clean|paper evidence fingerprints do not match" \
  "$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_campaign_fingerprint_tamper_manifest"

readiness_evidence_fingerprint_tamper_manifest="$work_root/readiness-evidence-fingerprint-tamper-manifest.json"
jq \
  '.paper_execution_binding.profit_compatibility_fingerprint_values =
    ["0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"]' \
  "$readiness_manifest" >"$readiness_evidence_fingerprint_tamper_manifest"
run_expect_fail "readiness_bundle_rejects_evidence_fingerprint_tamper" "campaign/evidence profit-compatibility fingerprints are not clean|paper evidence fingerprints do not match" \
  "$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_evidence_fingerprint_tamper_manifest"

gate_output="$work_root/live-ready-gate.json"
set +e
"$repo_root/scripts/live-ready-gate.sh" \
  --json \
  --readiness-manifest "$readiness_manifest" \
  --operator-preflight-manifest "$operator_manifest" \
  --output "$gate_output" >"$work_root/live-ready-gate.stdout" 2>"$work_root/live-ready-gate.stderr"
gate_rc=$?
set -e
gate_ok=0
if jq -e \
  --argjson expected_gate_ok "$([[ "$input_live_ready" -eq 1 ]] && echo true || echo false)" \
  --argjson expected_readiness_ok "$([[ "$readiness_activation_ready" -eq 1 ]] && echo true || echo false)" \
  --argjson expected_operator_ok "$([[ "$operator_activation_ready" -eq 1 ]] && echo true || echo false)" \
  '
  .ok == $expected_gate_ok and
  ((.readiness.rc == 0) == $expected_readiness_ok) and
  ((.operator_preflight.rc == 0) == $expected_operator_ok) and
  .final_live_env.require_live_env_enabled == false and
  .final_live_env.ok == true
  ' "$gate_output" >/dev/null \
  && { [[ "$input_live_ready" -eq 1 && "$gate_rc" -eq 0 ]] || [[ "$input_live_ready" -eq 0 && "$gate_rc" -ne 0 ]]; }; then
  gate_ok=1
fi
gate_expectation="fail"
if [[ "$input_live_ready" -eq 1 ]]; then
  gate_expectation="pass"
fi
record_check "live_ready_gate_matches_input" "$gate_expectation" "$gate_rc" "$gate_output" "$(json_bool "$((1 - gate_ok))")"
if [[ "$gate_ok" -ne 1 ]]; then
  echo "selftest failed: live-ready-gate did not match input readiness" >&2
  cat "$gate_output" >&2
  exit 1
fi

readiness_tamper_manifest="$work_root/readiness-protocol-tamper-manifest.json"
readiness_tamper_live="$work_root/readiness-live-readiness-report.json"
cp "$readiness_manifest" "$readiness_tamper_manifest"
readiness_live_report="$(jq -r '.files[] | select(.label == "live_readiness_report") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
if [[ -z "$readiness_live_report" || ! -f "$readiness_live_report" ]]; then
  echo "missing live readiness report in readiness manifest" >&2
  exit 2
fi
jq '.protocol_drift.source_urls = []' "$readiness_live_report" >"$readiness_tamper_live"
replace_manifest_file_entry "$readiness_tamper_manifest" "live_readiness_report" "$readiness_tamper_live"
run_expect_fail "readiness_bundle_rejects_protocol_source_tamper" "protocol drift evidence is not clean" \
  "$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_tamper_manifest"

readiness_build_tamper_manifest="$work_root/readiness-build-tamper-manifest.json"
readiness_build_tamper_provenance="$work_root/readiness-build-tamper-provenance.json"
readiness_build_provenance="$(jq -r '.files[]? | select(.label == "build_provenance") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
if [[ -z "$readiness_build_provenance" || ! -f "$readiness_build_provenance" ]]; then
  echo "missing build provenance in readiness manifest" >&2
  exit 2
fi
cp "$readiness_manifest" "$readiness_build_tamper_manifest"
jq '.binary.sha256 = "0000000000000000000000000000000000000000000000000000000000000000"' \
  "$readiness_build_provenance" >"$readiness_build_tamper_provenance"
replace_manifest_file_entry "$readiness_build_tamper_manifest" "build_provenance" "$readiness_build_tamper_provenance"
run_expect_fail "readiness_bundle_rejects_build_provenance_tamper" "build provenance does not match binary|manifest build provenance" \
  "$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_build_tamper_manifest"

run_build_provenance_tamper() {
  local name="$1"
  local failure_pattern="$2"
  local jq_filter="$3"
  local manifest="$work_root/${name}-manifest.json"
  local provenance="$work_root/${name}-provenance.json"
  cp "$readiness_manifest" "$manifest"
  jq "$jq_filter" "$readiness_build_provenance" >"$provenance"
  replace_manifest_file_entry "$manifest" "build_provenance" "$provenance"
  run_expect_fail "$name" "$failure_pattern" \
    "$repo_root/scripts/verify-readiness-bundle.sh" "$manifest"
}

run_build_provenance_tamper \
  "readiness_bundle_rejects_remap_policy_tamper" \
  "build provenance schema is not clean" \
  '.build_environment.deterministic_path_remapping.normalized_mappings[0].virtual = "/tampered-build-root"'
run_build_provenance_tamper \
  "readiness_bundle_rejects_reused_build_root" \
  "build provenance schema is not clean" \
  '.build_environment.deterministic_path_remapping.builds[1].physical_build_root = .build_environment.deterministic_path_remapping.builds[0].physical_build_root'
run_build_provenance_tamper \
  "readiness_bundle_rejects_extra_rustflag" \
  "build provenance schema is not clean" \
  '.build_environment.deterministic_path_remapping.builds[0].encoded_rustflags_argv += ["--cfg=tampered"]'
run_build_provenance_tamper \
  "readiness_bundle_rejects_forged_path_scan" \
  "build provenance schema is not clean" \
  '.build_environment.deterministic_path_remapping.builds[0].ephemeral_path_scan.clean = false'
run_build_provenance_tamper \
  "readiness_bundle_rejects_embedded_build_path" \
  "release binary leaked attested ephemeral build path" \
  '(.source_root as $source_root |
    .build_environment.deterministic_path_remapping.builds[0] as $build |
    .build_environment.deterministic_path_remapping.builds[0] = ($build |
      .physical_build_root = "/polymarket-build" |
      .cargo_home = "/polymarket-build/cargo-home" |
      .target_dir = "/polymarket-build/target" |
      .encoded_rustflags_argv = [
        "--remap-path-prefix=/polymarket-build=/polymarket-build",
        ("--remap-path-prefix=" + $source_root + "=/polymarket-source")
      ] |
      .ephemeral_path_scan = {
        clean:true,
        scanned_prefixes:["/polymarket-build","/polymarket-build/cargo-home","/polymarket-build/target"]
      }
    ))'

readiness_adapter_zero_manifest="$work_root/readiness-adapter-zero-manifest.json"
readiness_adapter_zero_log="$work_root/readiness-adapter-zero.log"
readiness_adapter_log="$(jq -r '.files[]? | select(.label == "paper_adapter_test_log") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
if [[ -z "$readiness_adapter_log" || ! -f "$readiness_adapter_log" ]]; then
  echo "missing paper adapter unit-proof log in readiness manifest" >&2
  exit 2
fi
cp "$readiness_manifest" "$readiness_adapter_zero_manifest"
sed -e 's/^running 1 test$/running 0 tests/' -e 's/^test result: ok\. 1 passed;/test result: ok. 0 passed;/' \
  "$readiness_adapter_log" >"$readiness_adapter_zero_log"
replace_manifest_file_entry "$readiness_adapter_zero_manifest" "paper_adapter_test_log" "$readiness_adapter_zero_log"
run_expect_fail "readiness_bundle_rejects_zero_adapter_tests" "paper adapter unit proof did not execute exactly one passing test" \
  "$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_adapter_zero_manifest"

operator_tamper_manifest="$work_root/operator-template-tamper-manifest.json"
operator_tamper_result="$work_root/operator-preflight-result.json"
operator_tamper_template="$work_root/live-env-template.sh"
cp "$operator_manifest" "$operator_tamper_manifest"
operator_result="$(jq -r '.result_json // empty' "$operator_manifest")"
operator_template="$(jq -r '.env_audit.template // empty' "$operator_manifest")"
if [[ -z "$operator_result" || ! -f "$operator_result" ]]; then
  echo "missing operator result_json" >&2
  exit 2
fi
if [[ -z "$operator_template" || ! -f "$operator_template" ]]; then
  echo "missing operator env template" >&2
  exit 2
fi
cp "$operator_template" "$operator_tamper_template"
awk '
  $0 == "export POLYMARKET_PRIVATE_KEY=\"\"" {
    print "export POLYMARKET_PRIVATE_KEY=\"tampered-secret-value\"";
    next
  }
  { print }
' "$operator_template" >"$operator_tamper_template"
jq \
  --arg template "$operator_tamper_template" \
  '.env_template_sh = $template' \
  "$operator_result" >"$operator_tamper_result"
jq \
  --arg result "$operator_tamper_result" \
  --arg template "$operator_tamper_template" \
  '.result_json = $result | .env_audit.template = $template' \
  "$operator_tamper_manifest" >"$operator_tamper_manifest.tmp"
mv "$operator_tamper_manifest.tmp" "$operator_tamper_manifest"
replace_manifest_file_entry "$operator_tamper_manifest" "operator_preflight_result" "$operator_tamper_result"
replace_manifest_file_entry "$operator_tamper_manifest" "live_env_template" "$operator_tamper_template"
run_expect_fail "operator_preflight_rejects_template_secret_tamper" "non-template shell syntax|export is not the exact generated value" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" "$operator_tamper_manifest"

operator_expansion_tamper_manifest="$work_root/operator-template-expansion-tamper-manifest.json"
operator_expansion_tamper_result="$work_root/operator-template-expansion-result.json"
operator_expansion_tamper_template="$work_root/live-env-template-expansion.sh"
cp "$operator_manifest" "$operator_expansion_tamper_manifest"
awk '
  $0 == "export LIVE_SIGNATURE_TYPE=\"0\"" {
    print "export LIVE_SIGNATURE_TYPE=\"$(touch /tmp/must-not-run)\"";
    next
  }
  { print }
' "$operator_template" >"$operator_expansion_tamper_template"
jq \
  --arg template "$operator_expansion_tamper_template" \
  '.env_template_sh = $template' \
  "$operator_result" >"$operator_expansion_tamper_result"
jq \
  --arg result "$operator_expansion_tamper_result" \
  --arg template "$operator_expansion_tamper_template" \
  '.result_json = $result | .env_audit.template = $template' \
  "$operator_expansion_tamper_manifest" >"$operator_expansion_tamper_manifest.tmp"
mv "$operator_expansion_tamper_manifest.tmp" "$operator_expansion_tamper_manifest"
replace_manifest_file_entry "$operator_expansion_tamper_manifest" "operator_preflight_result" "$operator_expansion_tamper_result"
replace_manifest_file_entry "$operator_expansion_tamper_manifest" "live_env_template" "$operator_expansion_tamper_template"
run_expect_fail "operator_preflight_rejects_template_shell_expansion" "non-template shell syntax|export is not the exact generated value" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" "$operator_expansion_tamper_manifest"

operator_panic_tamper_manifest="$work_root/operator-runtime-panic-tamper-manifest.json"
operator_panic_tamper_result="$work_root/operator-runtime-panic-tamper-result.json"
cp "$operator_manifest" "$operator_panic_tamper_manifest"
jq '.runtime_panic_scan.ok = false | .runtime_panic_scan.hit_count = 1' \
  "$operator_result_json" >"$operator_panic_tamper_result"
jq --arg result "$operator_panic_tamper_result" '.result_json = $result' \
  "$operator_panic_tamper_manifest" >"$operator_panic_tamper_manifest.tmp"
mv "$operator_panic_tamper_manifest.tmp" "$operator_panic_tamper_manifest"
replace_manifest_file_entry "$operator_panic_tamper_manifest" "operator_preflight_result" "$operator_panic_tamper_result"
run_expect_fail "operator_preflight_rejects_runtime_panic_tamper" "result_json no-submit checks are not clean" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" "$operator_panic_tamper_manifest"

operator_config_tamper_manifest="$work_root/operator-launch-config-tamper-manifest.json"
operator_config_tamper_fingerprint="$work_root/operator-launch-config-tamper.json"
cp "$operator_manifest" "$operator_config_tamper_manifest"
jq '.combined_fingerprint = "0x0000000000000000000000000000000000000000000000000000000000000000"' \
  "$operator_launch_config_fingerprint" >"$operator_config_tamper_fingerprint"
replace_manifest_file_entry "$operator_config_tamper_manifest" "launch_config_fingerprint" "$operator_config_tamper_fingerprint"
run_expect_fail "operator_preflight_rejects_launch_config_tamper" "launch config fingerprint does not match artifact|result_json no-submit checks are not clean" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" "$operator_config_tamper_manifest"

operator_state_tamper_manifest="$work_root/operator-live-state-tamper-manifest.json"
operator_state_tamper_result="$work_root/operator-live-state-tamper-result.json"
operator_state_tamper_live_report="$work_root/operator-live-state-tamper-readiness-report.json"
cp "$operator_manifest" "$operator_state_tamper_manifest"
jq '.live_submissions_supported = false' "$operator_live_readiness_report" >"$operator_state_tamper_live_report"
jq \
  --arg live_report "$operator_state_tamper_live_report" \
  '.reports.live_readiness_report = $live_report' \
  "$operator_result_json" >"$operator_state_tamper_result"
jq \
  --arg result "$operator_state_tamper_result" \
  --arg live_report "$operator_state_tamper_live_report" \
  '.result_json = $result | .reports.live_readiness_report = $live_report' \
  "$operator_state_tamper_manifest" >"$operator_state_tamper_manifest.tmp"
mv "$operator_state_tamper_manifest.tmp" "$operator_state_tamper_manifest"
replace_manifest_file_entry "$operator_state_tamper_manifest" "operator_preflight_result" "$operator_state_tamper_result"
replace_manifest_file_entry "$operator_state_tamper_manifest" "live_readiness_report" "$operator_state_tamper_live_report"
run_expect_fail "operator_preflight_rejects_underlying_live_state_tamper" "live readiness report has blocked checks" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" --require-live-ready "$operator_state_tamper_manifest"

operator_calibration_tamper_manifest="$work_root/operator-calibration-tamper-manifest.json"
operator_calibration_tamper_result="$work_root/operator-calibration-tamper-result.json"
operator_calibration_tamper_combo="$work_root/operator-calibration-tamper-combo-report.json"
cp "$operator_manifest" "$operator_calibration_tamper_manifest"
jq '
  (.checks[] | select(.key == "combo_rfq_replay_calibration") | .state) |=
    (if . == "ready" then "blocked" else "ready" end)
' "$operator_combo_promotion_report" >"$operator_calibration_tamper_combo"
jq \
  --arg combo "$operator_calibration_tamper_combo" \
  '.reports.combo_rfq_route_promotion_report = $combo' \
  "$operator_result_json" >"$operator_calibration_tamper_result"
jq \
  --arg result "$operator_calibration_tamper_result" \
  --arg combo "$operator_calibration_tamper_combo" \
  '.result_json = $result | .reports.combo_rfq_route_promotion_report = $combo' \
  "$operator_calibration_tamper_manifest" >"$operator_calibration_tamper_manifest.tmp"
mv "$operator_calibration_tamper_manifest.tmp" "$operator_calibration_tamper_manifest"
replace_manifest_file_entry "$operator_calibration_tamper_manifest" "operator_preflight_result" "$operator_calibration_tamper_result"
replace_manifest_file_entry "$operator_calibration_tamper_manifest" "combo_rfq_route_promotion_report" "$operator_calibration_tamper_combo"
run_expect_fail "operator_preflight_rejects_replay_calibration_tamper" "named Combo/RFQ replay calibration check does not match" \
  "$repo_root/scripts/verify-live-operator-preflight.sh" "$operator_calibration_tamper_manifest"

run_expect_pass "guarded_live_start_enforces_no_paper" \
  awk '
    /^launch_command=\($/ { in_launch = 1 }
    in_launch && /^  env$/ { env_command = 1 }
    in_launch && /"\$release_binary"/ { binary = 1 }
    in_launch && /^  --no-paper$/ { no_paper = 1 }
    /^exec "\$\{launch_command\[@\]\}"$/ { exec_launch = 1 }
    END { exit(in_launch && env_command && binary && no_paper && exec_launch ? 0 : 1) }
  ' "$repo_root/scripts/guarded-live-start.sh"
run_expect_fail "guarded_live_start_rejects_paper_extra" "--paper conflicts with enforced --no-paper" \
  "$repo_root/scripts/guarded-live-start.sh" \
    --activation-packet "$work_root/never-read-packet.json" \
    --confirm-live --no-paper -- --paper
run_expect_fail "guarded_live_start_rejects_unbound_extra" "unbound extra arguments are not allowed" \
  "$repo_root/scripts/guarded-live-start.sh" \
    --activation-packet "$work_root/never-read-packet.json" \
    --confirm-live --no-paper -- --no-clob
run_expect_pass "guarded_live_start_fixes_cwd_and_effective_env" \
  awk '
    /^cd "\$repo_root"$/ { cwd = 1 }
    /"DIAGNOSTICS_DIR=\$operator_live_dir"/ { diagnostics = 1 }
    /LIVE_DIAGNOSTICS_ENABLED=true/ { live_diagnostics = 1 }
    /PAPER_TRADING_ENABLED=false/ { paper = 1 }
    END { exit(cwd && diagnostics && live_diagnostics && paper ? 0 : 1) }
  ' "$repo_root/scripts/guarded-live-start.sh"

selftest_snapshot_report="$work_root/readiness-verifier-selftest-snapshot.json"
jq_activation_bootstrap='
  .checks += [
    {
      "name": "activation_packet_baseline",
      "expected": "pass",
      "rc": 0,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_provided_selftest_without_allow",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_require_live_ready_matches_input",
      "expected": "matches_input",
      "rc": 0,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_gate_tamper",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_protocol_tamper",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_launch_config_tamper",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_no_submit_tamper",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_standard_no_submit_tamper",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_live_start_tamper",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "activation_packet_rejects_missing_no_paper",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    },
    {
      "name": "guarded_live_start_rejects_tampered_packet",
      "expected": "fail",
      "rc": 1,
      "ok": true,
      "output": "bootstrap"
    }
  ]
'
jq -s \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg readiness_manifest "$readiness_manifest" \
  --arg operator_preflight_manifest "$operator_manifest" \
  --arg work_root "$work_root" \
  '{
    generated_at: $generated_at,
    ok: (all(.[]; .ok == true)),
    readiness_manifest: $readiness_manifest,
    operator_preflight_manifest: $operator_preflight_manifest,
    work_root: $work_root,
    checks: .
  }' "$checks_jsonl" >"$selftest_snapshot_report"
jq "$jq_activation_bootstrap" "$selftest_snapshot_report" >"$selftest_snapshot_report.tmp"
mv "$selftest_snapshot_report.tmp" "$selftest_snapshot_report"

activation_packet_dir="$work_root/live-activation-packet"
activation_packet="$activation_packet_dir/live-activation-packet.json"
run_expect_pass "activation_packet_baseline" \
  "$repo_root/scripts/live-activation-packet.sh" \
  --readiness-manifest "$readiness_manifest" \
  --operator-preflight-manifest "$operator_manifest" \
  --output-dir "$activation_packet_dir" \
  --selftest-report "$selftest_snapshot_report"
run_expect_fail "activation_packet_rejects_provided_selftest_without_allow" "selftest report was not generated" \
  "$repo_root/scripts/verify-live-activation-packet.sh" "$activation_packet"
run_expect_input_state "activation_packet_require_live_ready_matches_input" "$input_live_ready" "packet live-ready fields are not true|activation-readiness evidence is not true|live_ready is not true" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest --require-live-ready "$activation_packet"

activation_gate_tamper="$work_root/live-activation-packet-gate-tamper.json"
jq \
  --arg packet "$activation_gate_tamper" \
  '.artifacts.packet_json = $packet | .gate.readiness_blockers = ((.gate.readiness_blockers // 0) + 1)' \
  "$activation_packet" >"$activation_gate_tamper"
run_expect_fail "activation_packet_rejects_gate_tamper" "packet summary or gate fields" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$activation_gate_tamper"

activation_protocol_tamper="$work_root/live-activation-packet-protocol-tamper.json"
jq \
  --arg packet "$activation_protocol_tamper" \
  '.artifacts.packet_json = $packet | .protocol_drift.source_urls = []' \
  "$activation_packet" >"$activation_protocol_tamper"
run_expect_fail "activation_packet_rejects_protocol_tamper" "embedded protocol drift evidence is not clean" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$activation_protocol_tamper"

activation_config_tamper="$work_root/live-activation-packet-config-tamper.json"
jq \
  --arg packet "$activation_config_tamper" \
  '.artifacts.packet_json = $packet | .launch_config.combined_fingerprint = "0x0000000000000000000000000000000000000000000000000000000000000000"' \
  "$activation_packet" >"$activation_config_tamper"
run_expect_fail "activation_packet_rejects_launch_config_tamper" "embedded launch config fingerprint is not clean|packet summary or gate fields" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$activation_config_tamper"

activation_no_submit_tamper="$work_root/live-activation-packet-no-submit-tamper.json"
jq \
  --arg packet "$activation_no_submit_tamper" \
  '.artifacts.packet_json = $packet | .no_submit.operator_no_live_submission.live_trade_row_hits = 1' \
  "$activation_packet" >"$activation_no_submit_tamper"
run_expect_fail "activation_packet_rejects_no_submit_tamper" "embedded no-submit evidence is not clean" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$activation_no_submit_tamper"

activation_standard_no_submit_tamper="$work_root/live-activation-packet-standard-no-submit-tamper.json"
jq \
  --arg packet "$activation_standard_no_submit_tamper" \
  '.artifacts.packet_json = $packet | .no_submit.readiness_global_scan.standard_execution_journal_hits = 1' \
  "$activation_packet" >"$activation_standard_no_submit_tamper"
run_expect_fail "activation_packet_rejects_standard_no_submit_tamper" "embedded no-submit evidence is not clean" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$activation_standard_no_submit_tamper"

activation_live_start_tamper="$work_root/live-activation-packet-live-start-tamper.json"
jq \
  --arg packet "$activation_live_start_tamper" \
  '.artifacts.packet_json = $packet | .final_required_commands.live_start = "LIVE_TRADING_ENABLED=true cargo run -- --live"' \
  "$activation_packet" >"$activation_live_start_tamper"
run_expect_fail "activation_packet_rejects_live_start_tamper" "final required commands are not clean" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$activation_live_start_tamper"

activation_no_paper_tamper="$work_root/live-activation-packet-no-paper-tamper.json"
jq \
  --arg packet "$activation_no_paper_tamper" \
  '.artifacts.packet_json = $packet | .final_required_commands.live_start |= sub(" --no-paper$"; "")' \
  "$activation_packet" >"$activation_no_paper_tamper"
run_expect_fail "activation_packet_rejects_missing_no_paper" "final required commands are not clean" \
  "$repo_root/scripts/verify-live-activation-packet.sh" --allow-provided-selftest "$activation_no_paper_tamper"

run_expect_fail "guarded_live_start_rejects_tampered_packet" "final required commands are not clean|packet live-ready fields are not true|activation-readiness evidence is not true|live_ready is not true" \
  env LIVE_TRADING_ENABLED=true "$repo_root/scripts/guarded-live-start.sh" \
    --activation-packet "$activation_live_start_tamper" \
    --confirm-live \
    --gate-output "$work_root/guarded-live-gate.json"

jq -s \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg readiness_manifest "$readiness_manifest" \
  --arg operator_preflight_manifest "$operator_manifest" \
  --arg work_root "$work_root" \
  '{
    generated_at: $generated_at,
    ok: (all(.[]; .ok == true)),
    readiness_manifest: $readiness_manifest,
    operator_preflight_manifest: $operator_preflight_manifest,
    work_root: $work_root,
    checks: .
  }' "$checks_jsonl" >"$report_json"

cp "$report_json" "$report_output"

printf 'readiness_verifier_selftest_ok=1 readiness_manifest=%s operator_preflight_manifest=%s report=%s\n' \
  "$readiness_manifest" "$operator_manifest" "$report_output"
