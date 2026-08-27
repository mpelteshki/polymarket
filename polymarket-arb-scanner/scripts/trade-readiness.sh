#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage: scripts/trade-readiness.sh [--allow-live-blocked]

Runs Rust tests, dashboard lint/build, paper, HFT, live no-submit, and dashboard readiness checks.

Default exit code requires live readiness. Use --allow-live-blocked to accept
paper/HFT/UI readiness while reporting live blockers.

Writes machine-readable result to:
  $READINESS_ROOT/trade_readiness_result.json
or:
  /tmp/polymarket-trade-readiness-*/trade_readiness_result.json
EOF
}

allow_live_blocked=0
while [[ $# -gt 0 ]]; do
  case "$1" in
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

need cargo
need cc
need cmp
need cp
need curl
need find
need jq
need awk
need head
need ln
need lsof
need mktemp
need npm
need pm-trader
need browse
need rg
need rustc
need perl
need realpath
need sed
need shasum
need sort
need python3
need wc

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${READINESS_ROOT:-}" ]]; then
  run_root="$READINESS_ROOT"
  if [[ "$run_root" != /* ]]; then
    run_root="$PWD/$run_root"
  fi
  if [[ -L "$run_root" ]]; then
    echo "readiness root must not be a symlink: $run_root" >&2
    exit 2
  fi
  mkdir -p "$run_root"
else
  run_root="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-trade-readiness-XXXXXX")"
fi
run_root="$(realpath "$run_root")"
chmod 700 "$run_root"
paper_dir="$run_root/paper"
hft_dir="$run_root/hft"
live_dir="$run_root/live"
code_dir="$run_root/live-code-ceiling"
live_guard_dir="$run_root/live-fail-closed-diagnostics"
dashboard_log="$run_root/dashboard.log"
browse_log="$run_root/browse.log"
live_diag_log="$run_root/live-diagnostics.log"
code_ceiling_log="$run_root/live-code-ceiling.log"
live_guard_log="$run_root/live-fail-closed.log"
rust_tests_log="$run_root/rust-tests.log"
paper_adapter_test_log="$run_root/paper-adapter-test.log"
paper_smoke_log="$run_root/paper-smoke.log"
paper_execution_canary_log="$run_root/paper-execution-canary.log"
paper_scanner_trade_proof_log="$run_root/paper-scanner-trade-proof.log"
hft_smoke_log="$run_root/hft-smoke.log"
runtime_panic_hits="$run_root/runtime-panic-hits.txt"
live_guard_trade_log="$live_guard_dir/trades.csv"
live_combo_execution_journal="$live_dir/combo_rfq_execution_journal.jsonl"
live_standard_execution_journal="$live_dir/live_execution_journal.jsonl"
live_engine_mode_report="$live_dir/engine_mode_report.json"
live_engine_mode_state="$live_dir/engine_mode_state.json"
live_engine_mode_journal="$live_dir/engine_mode_journal.jsonl"
code_combo_execution_journal="$code_dir/combo_rfq_execution_journal.jsonl"
code_standard_execution_journal="$code_dir/live_execution_journal.jsonl"
code_trade_log="$code_dir/trades.csv"
code_ceiling_json="$code_dir/live_code_ceiling_report.json"
source_static_blockers="$code_dir/source_static_blockers.txt"
source_static_blockers_json="$code_dir/source_static_blockers.json"
code_ceiling_hft_json="$hft_dir/live_code_ceiling_report.json"
live_guard_combo_execution_journal="$live_guard_dir/combo_rfq_execution_journal.jsonl"
live_guard_standard_execution_journal="$live_guard_dir/live_execution_journal.jsonl"
result_json="$run_root/trade_readiness_result.json"
paper_balance_json="$run_root/paper-balance.json"
paper_history_json="$run_root/paper-history.json"
paper_execution_canary_json="$run_root/paper-execution-canary.json"
paper_scanner_trade_proof_json="$run_root/paper-scanner-trade-proof.json"
paper_profitability_report_json="$run_root/paper-profitability-report.json"
paper_profitability_input_trades_csv="${PAPER_PROFITABILITY_TRADES_CSV:-$run_root/paper-diagnostics/trades.csv}"
paper_profitability_input_attempts_jsonl="${PAPER_PROFITABILITY_ATTEMPTS_JSONL:-$(dirname "$paper_profitability_input_trades_csv")/paper_execution_attempts.jsonl}"
paper_profitability_trades_csv="$run_root/paper-profitability-trades-source.csv"
paper_profitability_attempts_jsonl="$run_root/paper-profitability-attempts-source.jsonl"
paper_adapter_provenance_json="$run_root/paper-adapter-provenance.json"
ui_body="$run_root/ui-body.txt"
ui_snapshot="$run_root/ui-snapshot.json"
ui_after_pause_snapshot="$run_root/ui-after-pause-snapshot.json"
ui_screenshot="$run_root/ui-dashboard.png"
ui_desktop_overflow="$run_root/ui-desktop-overflow.json"
ui_mobile_body="$run_root/ui-mobile-body.txt"
ui_mobile_snapshot="$run_root/ui-mobile-snapshot.json"
ui_mobile_screenshot="$run_root/ui-mobile-dashboard.png"
ui_mobile_overflow="$run_root/ui-mobile-overflow.json"
artifact_secret_hits="$run_root/artifact-secret-hits.txt"
artifact_secret_scan_json="$run_root/artifact-secret-scan.json"
paper_live_parity_audit_json="$run_root/paper-live-parity-audit.json"
live_unblock_plan_json="$run_root/live-unblock-plan.json"
global_no_live_submit_scan_json="$run_root/no-live-submission-scan.json"
global_live_trade_hits="$run_root/no-live-trade-row-hits.txt"
global_combo_journal_hits="$run_root/no-live-combo-journal-hits.txt"
global_standard_journal_hits="$run_root/no-live-standard-journal-hits.txt"
global_submit_marker_hits="$run_root/no-live-submit-marker-hits.txt"
readiness_bundle_manifest_json="$run_root/readiness-bundle-manifest.json"
readiness_bundle_files_json="$run_root/readiness-bundle-files.json"
readiness_bundle_verification_txt="$run_root/readiness-bundle-verification.txt"
release_binary="$run_root/release/polymarket-arb-scanner"
build_provenance_json="$run_root/build-provenance.json"
build_inputs_before_json="$run_root/build-inputs-before.json"
build_inputs_after_json="$run_root/build-inputs-after.json"
no_live_identity_fingerprint_json="$run_root/no-live-identity-fingerprint.json"
release_build_target="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-release-build.XXXXXX")"
repro_build_target=""
paper_adapter_unit_proof_test="external_paper_engine::tests::execute_opportunity_recomputes_zero_adapter_fees_from_clob_metadata"
paper_adapter_unit_proof_ok=0
mkdir -p "$paper_dir" "$hft_dir" "$live_dir" "$code_dir"

dashboard_pid=""
cleanup() {
  if [[ -n "${release_build_target:-}" ]]; then
    rm -rf "$release_build_target"
  fi
  if [[ -n "${repro_build_target:-}" ]]; then
    rm -rf "$repro_build_target"
  fi
  browse stop >/dev/null 2>>"$browse_log" || true
  if [[ -n "$dashboard_pid" ]]; then
    kill "$dashboard_pid" >/dev/null 2>&1 || true
    wait "$dashboard_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

pick_port() {
  local start="${DASHBOARD_PORT:-5173}"
  local port="$start"
  while [[ "$port" -lt $((start + 50)) ]]; do
    if ! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "$port"
      return 0
    fi
    port=$((port + 1))
  done
  echo "no free dashboard port from $start" >&2
  exit 2
}

section() {
  printf '\n== %s ==\n' "$1"
}

json_bool() {
  if [[ "$1" -eq 1 ]]; then
    echo true
  else
    echo false
  fi
}

no_live_secret_env_names=(
  POLYMARKET_PRIVATE_KEY
  POLYMARKET_API_KEY
  POLYMARKET_API_SECRET
  POLYMARKET_API_PASSPHRASE
  CLOB_API_KEY
  CLOB_SECRET
  CLOB_PASS_PHRASE
  CLOB_PASSPHRASE
  LIVE_SIGNER_ADDRESS
  COMBO_RFQ_BEARER_TOKEN
  COMBO_RFQ_PARTICIPANT_ID
  COMBO_RFQ_STREAM_BEARER_TOKEN
  RELAYER_API_KEY
  RELAYER_API_KEY_ADDRESS
  LIVE_FUNDER_ADDRESS
  POLYGON_RPC_URL
  WEBHOOK_URL
  BETDEX_AUTH_TOKEN
)
no_live_secret_env_unset_args=()
for secret_env_name in "${no_live_secret_env_names[@]}"; do
  # An explicitly empty value prevents dotenvy from repopulating credentials
  # from the repository .env inside the child process.
  no_live_secret_env_unset_args+=("${secret_env_name}=")
done
no_live_secret_env_list="$(printf '%s\n' "${no_live_secret_env_names[@]}")"

safe_no_live_env() {
  env "${no_live_secret_env_unset_args[@]}" "$@"
}

write_bundle_file_entry() {
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
      '{label: $label, path: $path, exists: true, size_bytes: ($size | tonumber? // 0), sha256: $sha}'
  else
    jq -n \
      --arg label "$label" \
      --arg path "$path" \
      '{label: $label, path: $path, exists: false, size_bytes: 0, sha256: null}'
  fi
}

collect_build_inputs() {
  local output="$1"
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
    } | LC_ALL=C sort -u | while IFS= read -r relative_path; do
      if [[ ! -f "$relative_path" ]]; then
        echo "missing build/safety input: $relative_path" >&2
        exit 1
      fi
      size="$(wc -c <"$relative_path" | tr -d '[:space:]')"
      sha="$(shasum -a 256 "$relative_path" | awk '{print $1}')"
      jq -n \
        --arg path "$relative_path" \
        --argjson size "$size" \
        --arg sha "$sha" \
        '{path:$path,size_bytes:$size,sha256:$sha}'
    done
  ) | jq -s '.' >"$output"
}

section "locked release build provenance"
collect_build_inputs "$build_inputs_before_json"
build_env_clear_names=(
  CARGO_HOME
  CARGO_BUILD_RUSTFLAGS
  CARGO_BUILD_RUSTC
  CARGO_BUILD_RUSTC_WRAPPER
  CARGO_BUILD_TARGET
  CARGO_ENCODED_RUSTFLAGS
  CARGO_INCREMENTAL
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS
  CARGO_PROFILE_RELEASE_DEBUG
  CARGO_PROFILE_RELEASE_INCREMENTAL
  CARGO_PROFILE_RELEASE_LTO
  CARGO_PROFILE_RELEASE_OPT_LEVEL
  CARGO_PROFILE_RELEASE_PANIC
  CARGO_PROFILE_RELEASE_STRIP
  CARGO_TARGET_DIR
  RUSTC
  RUSTC_WRAPPER
  RUSTC_WORKSPACE_WRAPPER
  RUSTFLAGS
  RUSTUP_TOOLCHAIN
  SOURCE_DATE_EPOCH
  CC
  CFLAGS
  CXX
  CXXFLAGS
  AR
  MACOSX_DEPLOYMENT_TARGET
  SDKROOT
)
while IFS= read -r build_env_name; do
  case "$build_env_name" in
    CARGO_BUILD_*|CARGO_PROFILE_RELEASE_*|CARGO_TARGET_*_RUSTFLAGS|CARGO_TARGET_*_LINKER|CARGO_TARGET_*_RUNNER|CC_*|CFLAGS_*|CXX_*|CXXFLAGS_*|AR_*)
      build_env_name_seen=0
      for existing_build_env_name in "${build_env_clear_names[@]}"; do
        if [[ "$existing_build_env_name" == "$build_env_name" ]]; then
          build_env_name_seen=1
          break
        fi
      done
      if [[ "$build_env_name_seen" -eq 0 ]]; then
        build_env_clear_names+=("$build_env_name")
      fi
      ;;
  esac
done < <(compgen -e | LC_ALL=C sort)
build_env_unset_args=()
ambient_build_override_names=()
for build_env_name in "${build_env_clear_names[@]}"; do
  build_env_unset_args+=("-u" "$build_env_name")
  if [[ -n "${!build_env_name+x}" ]]; then
    ambient_build_override_names+=("$build_env_name")
  fi
done
build_env_clear_list="$(printf '%s\n' "${build_env_clear_names[@]}")"
if [[ "${#ambient_build_override_names[@]}" -gt 0 ]]; then
  ambient_build_override_list="$(printf '%s\n' "${ambient_build_override_names[@]}")"
else
  ambient_build_override_list=""
fi
source_cargo_home="$HOME/.cargo"
virtual_build_root="/polymarket-build"
virtual_source_root="/polymarket-source"
rustflags_separator=$'\x1f'
first_build_root="$release_build_target"
isolated_cargo_home="$release_build_target/cargo-home"
isolated_target_dir="$release_build_target/target"
mkdir -p "$isolated_cargo_home"
for cargo_cache_dir in registry git; do
  if [[ -d "$source_cargo_home/$cargo_cache_dir" ]]; then
    ln -s "$source_cargo_home/$cargo_cache_dir" "$isolated_cargo_home/$cargo_cache_dir"
  fi
done
release_encoded_rustflags="--remap-path-prefix=${release_build_target}=${virtual_build_root}${rustflags_separator}--remap-path-prefix=${repo_root}=${virtual_source_root}"
release_remap_build_arg="--remap-path-prefix=${release_build_target}=${virtual_build_root}"
release_remap_source_arg="--remap-path-prefix=${repo_root}=${virtual_source_root}"
sanitized_build_env=(
  env "${build_env_unset_args[@]}"
  "${no_live_secret_env_unset_args[@]}"
  CARGO_HOME="$isolated_cargo_home"
  CARGO_TARGET_DIR="$isolated_target_dir"
  CARGO_ENCODED_RUSTFLAGS="$release_encoded_rustflags"
  CARGO_INCREMENTAL=0
  SOURCE_DATE_EPOCH=0
)
build_rustc_verbose_version="$("${sanitized_build_env[@]}" rustc -vV)"
build_host_triple="$(sed -n 's/^host: //p' <<<"$build_rustc_verbose_version")"
build_cargo_version="$("${sanitized_build_env[@]}" cargo --version)"
build_cargo_verbose_version="$("${sanitized_build_env[@]}" cargo -vV)"
build_rustc_version="$("${sanitized_build_env[@]}" rustc --version)"
native_cc_path="$(command -v cc || true)"
native_cc_version="$(cc --version 2>/dev/null | head -n 1 || true)"
if [[ -z "$build_host_triple" ]]; then
  echo "could not determine sanitized rustc host triple" >&2
  exit 1
fi
(
  cd "$repo_root"
  "${sanitized_build_env[@]}" \
    cargo build --locked --release --bin polymarket-arb-scanner --target "$build_host_triple"
)
collect_build_inputs "$build_inputs_after_json"
if ! cmp -s "$build_inputs_before_json" "$build_inputs_after_json"; then
  echo "build/safety inputs changed during locked release build" >&2
  exit 1
fi
mkdir -p "$(dirname "$release_binary")"
isolated_release_binary="$isolated_target_dir/$build_host_triple/release/polymarket-arb-scanner"
if [[ ! -x "$isolated_release_binary" ]]; then
  echo "isolated release build did not produce $isolated_release_binary" >&2
  exit 1
fi
cp "$isolated_release_binary" "$release_binary"
release_binary_size="$(wc -c <"$release_binary" | tr -d '[:space:]')"
release_binary_sha="$(shasum -a 256 "$release_binary" | awk '{print $1}')"
for ephemeral_path in "$first_build_root" "$isolated_cargo_home" "$isolated_target_dir"; do
  if LC_ALL=C rg -aFq -- "$ephemeral_path" "$isolated_release_binary"; then
    echo "first release binary leaked ephemeral build path: $ephemeral_path" >&2
    exit 1
  else
    path_scan_status=$?
    if [[ "$path_scan_status" -ne 1 ]]; then
      echo "could not scan first release binary for ephemeral path: $ephemeral_path" >&2
      exit 1
    fi
  fi
done
rm -rf "$release_build_target"
release_build_target=""

repro_build_target="$(mktemp -d "${TMPDIR:-/tmp}/polymarket-release-repro.XXXXXX")"
second_build_root="$repro_build_target"
repro_cargo_home="$repro_build_target/cargo-home"
repro_target_dir="$repro_build_target/target"
mkdir -p "$repro_cargo_home"
for cargo_cache_dir in registry git; do
  if [[ -d "$source_cargo_home/$cargo_cache_dir" ]]; then
    ln -s "$source_cargo_home/$cargo_cache_dir" "$repro_cargo_home/$cargo_cache_dir"
  fi
done
repro_encoded_rustflags="--remap-path-prefix=${repro_build_target}=${virtual_build_root}${rustflags_separator}--remap-path-prefix=${repo_root}=${virtual_source_root}"
repro_remap_build_arg="--remap-path-prefix=${repro_build_target}=${virtual_build_root}"
repro_remap_source_arg="--remap-path-prefix=${repo_root}=${virtual_source_root}"
repro_build_env=(
  env "${build_env_unset_args[@]}"
  "${no_live_secret_env_unset_args[@]}"
  CARGO_HOME="$repro_cargo_home"
  CARGO_TARGET_DIR="$repro_target_dir"
  CARGO_ENCODED_RUSTFLAGS="$repro_encoded_rustflags"
  CARGO_INCREMENTAL=0
  SOURCE_DATE_EPOCH=0
)
(
  cd "$repo_root"
  "${repro_build_env[@]}" \
    cargo build --locked --release --bin polymarket-arb-scanner --target "$build_host_triple"
)
repro_release_binary="$repro_target_dir/$build_host_triple/release/polymarket-arb-scanner"
repro_binary_sha="$(shasum -a 256 "$repro_release_binary" | awk '{print $1}')"
if [[ "$repro_binary_sha" != "$release_binary_sha" ]]; then
  echo "fresh isolated release builds were not byte-identical" >&2
  exit 1
fi
if [[ "$first_build_root" == "$second_build_root" ]]; then
  echo "reproducibility check reused the same physical build root" >&2
  exit 1
fi
for ephemeral_path in "$second_build_root" "$repro_cargo_home" "$repro_target_dir"; do
  if LC_ALL=C rg -aFq -- "$ephemeral_path" "$repro_release_binary"; then
    echo "second release binary leaked ephemeral build path: $ephemeral_path" >&2
    exit 1
  else
    path_scan_status=$?
    if [[ "$path_scan_status" -ne 1 ]]; then
      echo "could not scan second release binary for ephemeral path: $ephemeral_path" >&2
      exit 1
    fi
  fi
done
rm -rf "$repro_build_target"
repro_build_target=""
jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg source_root "$repo_root" \
  --arg binary_path "$release_binary" \
  --argjson binary_size "$release_binary_size" \
  --arg binary_sha "$release_binary_sha" \
  --arg repro_binary_sha "$repro_binary_sha" \
  --arg cargo_version "$build_cargo_version" \
  --arg cargo_verbose_version "$build_cargo_verbose_version" \
  --arg rustc_version "$build_rustc_version" \
  --arg rustc_verbose_version "$build_rustc_verbose_version" \
  --arg host_target "$build_host_triple" \
  --arg native_cc_path "$native_cc_path" \
  --arg native_cc_version "$native_cc_version" \
  --arg build_env_clear_list "$build_env_clear_list" \
  --arg ambient_build_override_list "$ambient_build_override_list" \
  --arg no_live_secret_env_list "$no_live_secret_env_list" \
  --arg virtual_build_root "$virtual_build_root" \
  --arg virtual_source_root "$virtual_source_root" \
  --arg first_build_root "$first_build_root" \
  --arg first_cargo_home "$isolated_cargo_home" \
  --arg first_target_dir "$isolated_target_dir" \
  --arg first_remap_build_arg "$release_remap_build_arg" \
  --arg first_remap_source_arg "$release_remap_source_arg" \
  --arg second_build_root "$second_build_root" \
  --arg second_cargo_home "$repro_cargo_home" \
  --arg second_target_dir "$repro_target_dir" \
  --arg second_remap_build_arg "$repro_remap_build_arg" \
  --arg second_remap_source_arg "$repro_remap_source_arg" \
  --slurpfile inputs "$build_inputs_before_json" \
  '{
    schema_version:2,
    generated_at:$generated_at,
    source_root:$source_root,
    build_command:["cargo","build","--locked","--release","--bin","polymarket-arb-scanner","--target",$host_target],
    cargo_version:$cargo_version,
    cargo_verbose_version:$cargo_verbose_version,
    rustc_version:$rustc_version,
    rustc_verbose_version:$rustc_verbose_version,
    host_target:$host_target,
    build_environment:{
      isolated_target_dir:true,
      isolated_cargo_home:true,
      cargo_incremental:"0",
      source_date_epoch:"0",
      deterministic_path_remapping:{
        transport:"CARGO_ENCODED_RUSTFLAGS",
        compiler_option:"--remap-path-prefix",
        scope:"rustc_default_all",
        explicit_scope_flag:false,
        physical_build_roots_recorded:true,
        source_root_portable:false,
        embedded_source_root_runtime_dependency:true,
        normalized_mappings:[
          {physical_role:"isolated_build_root",virtual:$virtual_build_root},
          {physical_role:"source_root",virtual:$virtual_source_root}
        ],
        builds:[
          {
            ordinal:1,
            physical_build_root:$first_build_root,
            cargo_home:$first_cargo_home,
            target_dir:$first_target_dir,
            encoded_rustflags_argv:[$first_remap_build_arg,$first_remap_source_arg],
            ephemeral_path_scan:{clean:true,scanned_prefixes:[$first_build_root,$first_cargo_home,$first_target_dir]}
          },
          {
            ordinal:2,
            physical_build_root:$second_build_root,
            cargo_home:$second_cargo_home,
            target_dir:$second_target_dir,
            encoded_rustflags_argv:[$second_remap_build_arg,$second_remap_source_arg],
            ephemeral_path_scan:{clean:true,scanned_prefixes:[$second_build_root,$second_cargo_home,$second_target_dir]}
          }
        ]
      },
      cleared_ambient_names:($build_env_clear_list | split("\n") | map(select(length > 0))),
      ambient_overrides_detected_and_cleared:($ambient_build_override_list | split("\n") | map(select(length > 0)))
      ,secret_env_names_cleared:($no_live_secret_env_list | split("\n") | map(select(length > 0)))
    },
    native_toolchain:{cc_path:$native_cc_path,cc_version:$native_cc_version,fully_attested:false},
    inputs_unchanged_during_build:true,
    binary:{path:$binary_path,size_bytes:$binary_size,sha256:$binary_sha},
    reproducibility_check:{fresh_isolated_builds:2,byte_identical:true,second_binary_sha256:$repro_binary_sha},
    inputs:$inputs[0]
  }' >"$build_provenance_json"

section "paper adapter provenance"
paper_adapter_command_path="$(command -v pm-trader)"
paper_adapter_path="$(perl -MCwd=realpath -e 'print realpath($ARGV[0])' "$paper_adapter_command_path")"
if [[ -z "$paper_adapter_path" || ! -x "$paper_adapter_path" ]]; then
  echo "could not resolve canonical pm-trader executable" >&2
  exit 1
fi
paper_adapter_sha="$(shasum -a 256 "$paper_adapter_path" | awk '{print $1}')"
jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg command "pm-trader" \
  --arg canonical_path "$paper_adapter_path" \
  --arg sha "$paper_adapter_sha" \
  '{
    schema_version:1,
    generated_at:$generated_at,
    command:$command,
    canonical_path:$canonical_path,
    executable_sha256:$sha,
    trust_boundary:"entry executable path/bytes only; transitive Python/venv dependencies and native system libraries are not attested"
  }' >"$paper_adapter_provenance_json"

section "dotenv-resistant no-live identity proof"
(
  cd "$repo_root"
  safe_no_live_env "$release_binary" \
    --launch-config-fingerprint-output "$no_live_identity_fingerprint_json"
)
jq -e '
  (.direct_live_identities | length) > 0 and
  all(.direct_live_identities[]; .present == false)
' "$no_live_identity_fingerprint_json" >/dev/null \
  || { echo "no-live environment allowed dotenv credentials to repopulate" >&2; exit 1; }

section "rust tests"
(
  cd "$repo_root"
  safe_no_live_env cargo test --quiet
) 2>&1 | tee "$rust_tests_log"

section "python evidence and shadow tests"
(
  cd "$repo_root"
  python3 -m py_compile scripts/*.py tests/*.py
  python3 -m unittest discover -s tests -p 'test_*.py'
)

section "paper adapter unit proof"
(
  cd "$repo_root"
  safe_no_live_env cargo test --quiet "$paper_adapter_unit_proof_test" -- --exact
) 2>&1 | tee "$paper_adapter_test_log"
paper_adapter_running_count="$(rg -c '^running 1 test$' "$paper_adapter_test_log" || true)"
if [[ "${paper_adapter_running_count:-0}" -ne 1 ]] \
  || ! rg -q '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out;' "$paper_adapter_test_log"; then
  echo "paper adapter unit proof did not execute exactly one passing test" >&2
  exit 1
fi
paper_adapter_unit_proof_ok=1

section "dashboard lint"
(
  cd "$repo_root/dashboard"
  npm run lint
)

section "dashboard build"
(
  cd "$repo_root/dashboard"
  npm run build
)

csv_data_rows() {
  local file="$1"
  if [[ ! -s "$file" ]]; then
    echo 0
    return
  fi
  awk 'NR > 1 && $0 !~ /^[[:space:]]*$/ { count++ } END { print count + 0 }' "$file"
}

live_submit_rows() {
  local file="$1"
  if [[ ! -s "$file" ]]; then
    echo 0
    return
  fi
  awk -F, '
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
      count++
    }
    END { print count + 0 }
  ' "$file"
}

nonempty_rows() {
  local file="$1"
  if [[ ! -s "$file" ]]; then
    echo 0
    return
  fi
  awk '$0 !~ /^[[:space:]]*$/ { count++ } END { print count + 0 }' "$file"
}

section "paper smoke"
(
  cd "$repo_root"
  safe_no_live_env \
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=false \
  PAPER_TRADING_ENABLED=true \
  DRY_RUN_PROVIDER=external \
  EXTERNAL_PAPER_COMMAND="$paper_adapter_path" \
  EXTERNAL_PAPER_DATA_DIR="$paper_dir" \
  EXTERNAL_PAPER_ACCOUNT=smoke-arb \
  EXTERNAL_PAPER_INIT_BALANCE_USD=10000 \
  LOG_LEVEL=warn \
  DIAGNOSTICS_DIR="$run_root/paper-diagnostics" \
  "$release_binary" --once
) 2>&1 | tee "$paper_smoke_log"
"$paper_adapter_path" --data-dir "$paper_dir" --account smoke-arb balance >"$paper_balance_json"
"$paper_adapter_path" --data-dir "$paper_dir" --account smoke-arb history >"$paper_history_json"

section "paper execution canary"
(
  cd "$repo_root"
  safe_no_live_env \
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=false \
  DRY_RUN_PROVIDER=external \
  EXTERNAL_PAPER_COMMAND="$paper_adapter_path" \
  EXTERNAL_PAPER_DATA_DIR="$run_root/paper-canary-data" \
  EXTERNAL_PAPER_ACCOUNT=paper-canary \
  EXTERNAL_PAPER_INIT_BALANCE_USD="${READINESS_PAPER_CANARY_BALANCE_USD:-100.00}" \
  LOG_LEVEL=warn \
  "$release_binary" \
    --paper-execution-canary \
    --paper-execution-canary-output "$paper_execution_canary_json" \
    --paper-execution-canary-amount-usd "${READINESS_PAPER_CANARY_AMOUNT_USD:-1.00}"
) 2>&1 | tee "$paper_execution_canary_log"

section "paper scanner trade proof"
(
  cd "$repo_root"
  safe_no_live_env \
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=false \
  LOG_LEVEL=warn \
  "$release_binary" \
    --paper-scanner-trade-proof \
    --paper-scanner-trade-proof-output "$paper_scanner_trade_proof_json"
) 2>&1 | tee "$paper_scanner_trade_proof_log"

section "paper profitability evidence"
paper_profitability_gate_exit=0
python3 "$repo_root/scripts/paper_profitability_gate.py" \
  --trades-csv "$paper_profitability_input_trades_csv" \
  --attempts-jsonl "$paper_profitability_input_attempts_jsonl" \
  --source-snapshot "$paper_profitability_trades_csv" \
  --attempts-snapshot "$paper_profitability_attempts_jsonl" \
  --expected-producer-binary-sha256 "$release_binary_sha" \
  --expected-adapter-executable-sha256 "$paper_adapter_sha" \
  --output "$paper_profitability_report_json" \
  || paper_profitability_gate_exit=$?
if [[ "$paper_profitability_gate_exit" -gt 1 ]]; then
  echo "paper profitability evidence evaluator failed: exit=$paper_profitability_gate_exit" >&2
  exit "$paper_profitability_gate_exit"
fi

section "hft smoke"
(
  cd "$repo_root"
  safe_no_live_env \
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=false \
  PAPER_TRADING_ENABLED=false \
  MAX_EVENTS_TO_FETCH="${READINESS_MAX_EVENTS_TO_FETCH:-80}" \
  SCAN_NEG_RISK_EVENT_BUDGET="${READINESS_SCAN_NEG_RISK_EVENT_BUDGET:-32}" \
  SCAN_BUNDLE_EVENT_BUDGET="${READINESS_SCAN_BUNDLE_EVENT_BUDGET:-32}" \
  QUOTE_REFRESH_TOKEN_BUDGET_PER_SCAN="${READINESS_QUOTE_REFRESH_TOKEN_BUDGET_PER_SCAN:-800}" \
  ACTIVE_QUOTE_TOKEN_BUDGET_PER_SCAN="${READINESS_ACTIVE_QUOTE_TOKEN_BUDGET_PER_SCAN:-800}" \
  USE_WEBSOCKET=true \
  WS_INITIAL_SNAPSHOT_TIMEOUT_MS="${READINESS_WS_INITIAL_SNAPSHOT_TIMEOUT_MS:-750}" \
  LOG_LEVEL=warn \
  DIAGNOSTICS_DIR="$hft_dir" \
  "$release_binary" --duration "${READINESS_HFT_DURATION_SECONDS:-8}" --interval "${READINESS_HFT_INTERVAL_SECONDS:-1}" --no-paper
) 2>&1 | tee "$hft_smoke_log"

section "live no-submit diagnostics"
(
  cd "$repo_root"
  safe_no_live_env \
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=true \
  LIVE_COMBO_RFQ_ROUTE_ENABLED=false \
  LOG_LEVEL=warn \
  DIAGNOSTICS_DIR="$live_dir" \
  "$release_binary" --live-diagnostics --once --no-paper
) 2>&1 | perl -pe 's/ip=[^ ]+/ip=<redacted>/g; s/\b(?:\d{1,3}\.){3}\d{1,3}\b/<ipv4-redacted>/g' | tee "$live_diag_log"

section "live code-ceiling diagnostics"
(
  cd "$repo_root"
  safe_no_live_env \
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=true \
  LIVE_COMBO_RFQ_ROUTE_ENABLED=true \
  COMBO_RFQ_REQUESTER_ENABLED=true \
  COMBO_RFQ_ACCEPT_ENABLED=true \
  COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED=true \
  COMBO_RFQ_BEARER_TOKEN="readiness-redacted-token" \
  COMBO_RFQ_PARTICIPANT_ID="readiness-redacted-participant" \
  LIVE_USER_WS_ENABLED=true \
  POLYMARKET_API_KEY="readiness-redacted-api-key" \
  POLYMARKET_API_SECRET="readiness-redacted-api-secret" \
  POLYMARKET_API_PASSPHRASE="readiness-redacted-api-passphrase" \
  SETTLEMENT_MONITOR_ENABLED=true \
  SETTLEMENT_REVERT_HAZARD_MIN_SAMPLES=1 \
  LIVE_CLOSEOUT_ENABLED=true \
  LIVE_CLOSEOUT_DRY_RUN=false \
  LOG_LEVEL=warn \
  DIAGNOSTICS_DIR="$code_dir" \
  "$release_binary" --live-diagnostics --once --no-paper
) 2>&1 | perl -pe 's/ip=[^ ]+/ip=<redacted>/g; s/\b(?:\d{1,3}\.){3}\d{1,3}\b/<ipv4-redacted>/g' | tee "$code_ceiling_log"
(
  cd "$repo_root"
  rg -n -S 'todo!\s*\(|unimplemented!\s*\(|panic!\s*\(\s*"[^"]*(TODO|not implemented)|bail!\s*\(\s*"[^"]*not implemented' src || true
) >"$source_static_blockers"
jq -R -s '
  split("\n")
  | map(select(length > 0))
  | map(capture("^(?<file>[^:]+):(?<line>[0-9]+):(?<text>.*)$") // { raw: . })
' <"$source_static_blockers" >"$source_static_blockers_json"
jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg code_dir "$code_dir" \
  --slurpfile live "$code_dir/live_readiness_report.json" \
  --slurpfile combo "$code_dir/combo_rfq_route_promotion_report.json" \
  --slurpfile source_static "$source_static_blockers_json" \
  '
  def hard_code_blocker:
    tostring
    | test("not_implemented|not implemented|no_live_arbitrage_routes_supported|not currently supported|currently supports EOA only|unsupported"; "i");
  ([
    $live[0].checks[]?
    | select(.state != "ready" and .key != "live_route_matrix")
    | { key, state, detail }
  ] + [
    $combo[0].blockers[]?
    | { key: "combo_rfq_route_promotion", state: "blocked", detail: . }
  ] | unique_by(.key + ":" + (.detail | tostring))) as $not_ready
  | ($not_ready | map(select(.detail | hard_code_blocker))) as $code_blockers
  | ($source_static[0] | map({
      key: "source_static_scan",
      state: "blocked",
      detail: (
        if has("file") then
          (.file + ":" + .line + ": " + .text)
        else
          .raw
        end
      )
    })) as $source_code_blockers
  | {
      generated_at: $generated_at,
      diagnostics_dir: $code_dir,
      promoted: ($combo[0].promoted // false),
      code_blockers: ($code_blockers + $source_code_blockers),
      code_blocker_count: (($code_blockers | length) + ($source_code_blockers | length)),
      runtime_code_blockers: $code_blockers,
      source_static_blockers: $source_code_blockers,
      source_static_blocker_count: ($source_code_blockers | length),
      not_ready_checks: $not_ready
    }
  ' >"$code_ceiling_json"

section "live fail-closed guard"
set +e
(
  cd "$repo_root"
  safe_no_live_env \
  LIVE_TRADING_ENABLED=false \
  LIVE_DIAGNOSTICS_ENABLED=false \
  LIVE_COMBO_RFQ_ROUTE_ENABLED=false \
  LOG_LEVEL=warn \
  DIAGNOSTICS_DIR="$live_guard_dir" \
  "$release_binary" --live --once --no-paper
) 2>&1 | perl -pe 's/ip=[^ ]+/ip=<redacted>/g; s/\b(?:\d{1,3}\.){3}\d{1,3}\b/<ipv4-redacted>/g' >"$live_guard_log"
live_guard_exit=${PIPESTATUS[0]}
set -e
live_fail_closed_ok=0
live_guard_reason_ok=0
live_guard_panic_free=0
if rg -qi "live execution disabled: LIVE_TRADING_ENABLED=false" "$live_guard_log"; then
  live_guard_reason_ok=1
fi
if ! rg -qi "panic|panicked|stack backtrace|thread '.*' panicked|segmentation fault|fatal runtime" "$live_guard_log"; then
  live_guard_panic_free=1
fi
if [[ "$live_guard_exit" -ne 0 && "$live_guard_reason_ok" -eq 1 && "$live_guard_panic_free" -eq 1 ]]; then
  live_fail_closed_ok=1
fi

: >"$runtime_panic_hits"
rg -n -H -i 'panicked at|CryptoProvider' \
  "$rust_tests_log" \
  "$paper_adapter_test_log" \
  "$paper_smoke_log" \
  "$paper_execution_canary_log" \
  "$paper_scanner_trade_proof_log" \
  "$hft_smoke_log" \
  "$live_diag_log" \
  "$code_ceiling_log" \
  "$live_guard_log" >"$runtime_panic_hits" || true
runtime_panic_hit_count="$(awk 'END { print NR + 0 }' "$runtime_panic_hits")"
runtime_panic_free=0
if [[ "$runtime_panic_hit_count" -eq 0 ]]; then
  runtime_panic_free=1
fi

cp "$live_dir/live_readiness_report.json" "$hft_dir/live_readiness_report.json"
cp "$live_dir/combo_rfq_route_promotion_report.json" "$hft_dir/combo_rfq_route_promotion_report.json"
cp "$code_ceiling_json" "$code_ceiling_hft_json"

section "dashboard readiness"
dashboard_port="$(pick_port)"
(
  cd "$repo_root/dashboard"
  safe_no_live_env \
  DIAGNOSTICS_DIR="$hft_dir" \
  EXTERNAL_PAPER_COMMAND="$paper_adapter_path" \
  EXTERNAL_PAPER_DATA_DIR="$paper_dir" \
  EXTERNAL_PAPER_ACCOUNT=smoke-arb \
  npm run dev -- --host 127.0.0.1 --port "$dashboard_port" --strictPort >"$dashboard_log" 2>&1
) &
dashboard_pid=$!

dashboard_url="http://127.0.0.1:$dashboard_port"
for _ in $(seq 1 80); do
  if curl -fsS "$dashboard_url/api/readiness" >"$run_root/readiness.json" 2>/dev/null; then
    break
  fi
  sleep 0.25
done
if [[ ! -s "$run_root/readiness.json" ]]; then
  echo "dashboard readiness API did not start; log=$dashboard_log" >&2
  exit 1
fi

section "rendered ui smoke"
browse stop >/dev/null 2>>"$browse_log" || true
browse open "$dashboard_url" --local >/dev/null 2>>"$browse_log"
browse viewport 1440 900 >/dev/null 2>>"$browse_log"
browse wait load >/dev/null 2>>"$browse_log" || true
browse get text body >"$ui_body" 2>>"$browse_log"
browse snapshot >"$ui_snapshot" 2>>"$browse_log"
browse screenshot --path "$ui_screenshot" >/dev/null 2>>"$browse_log"
browse eval '({innerWidth: window.innerWidth, scrollWidth: document.documentElement.scrollWidth, bodyScrollWidth: document.body.scrollWidth, overflowX: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) > window.innerWidth + 1})' >"$ui_desktop_overflow" 2>>"$browse_log"

ui_render_ok=0
ui_pause_ok=0
ui_desktop_overflow_ok=0
ui_mobile_render_ok=0
ui_mobile_overflow_ok=0
pause_ref="$(
  sed -n 's/.*\[\([^]]*\)\] switch: Pause auto refresh.*/\1/p' "$ui_snapshot" | head -n 1
)"
if [[ -n "$pause_ref" ]]; then
  browse click "$pause_ref" >/dev/null 2>>"$browse_log"
  browse snapshot >"$ui_after_pause_snapshot" 2>>"$browse_log"
  if rg -q "switch: Pause auto refresh \\[checked\\]" "$ui_after_pause_snapshot"; then
    ui_pause_ok=1
  fi
fi
if jq -e '.result.overflowX == false' "$ui_desktop_overflow" >/dev/null 2>&1; then
  ui_desktop_overflow_ok=1
fi
browse viewport 390 844 --scale 2 >/dev/null 2>>"$browse_log"
browse reload >/dev/null 2>>"$browse_log" || true
browse wait load >/dev/null 2>>"$browse_log" || true
browse get text body >"$ui_mobile_body" 2>>"$browse_log"
browse snapshot >"$ui_mobile_snapshot" 2>>"$browse_log"
browse screenshot --path "$ui_mobile_screenshot" >/dev/null 2>>"$browse_log"
browse eval '({innerWidth: window.innerWidth, scrollWidth: document.documentElement.scrollWidth, bodyScrollWidth: document.body.scrollWidth, overflowX: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) > window.innerWidth + 1})' >"$ui_mobile_overflow" 2>>"$browse_log"
if jq -e '.result.overflowX == false' "$ui_mobile_overflow" >/dev/null 2>&1; then
  ui_mobile_overflow_ok=1
fi
if rg -q "Trade readiness monitor" "$ui_mobile_body" \
  && rg -q "Live unblock path" "$ui_mobile_body" \
  && rg -q "Live code gates" "$ui_mobile_body" \
  && rg -q "Paper.*ready|Paperready" "$ui_mobile_body" \
  && rg -q "Live.*blocked|Liveblocked" "$ui_mobile_body" \
  && rg -q "Live submit.*ready|Live submitready" "$ui_mobile_body" \
  && rg -q "HFT.*ok|HFTreadyok" "$ui_mobile_body" \
  && rg -q "UI.*dashboard online|UIdashboard online" "$ui_mobile_body" \
  && ! rg -q "Internal Server Error|plugin:vite|Pre-transform error|Error Overlay" "$ui_mobile_body" \
  && [[ "$ui_mobile_overflow_ok" -eq 1 ]]; then
  ui_mobile_render_ok=1
fi
if rg -q "Trade readiness monitor" "$ui_body" \
  && rg -q "Live unblock path" "$ui_body" \
  && rg -q "Live code gates" "$ui_body" \
  && rg -q "Paper.*ready|Paperready" "$ui_body" \
  && rg -q "Live.*blocked|Liveblocked" "$ui_body" \
  && rg -q "Live submit.*ready|Live submitready" "$ui_body" \
  && rg -q "HFT.*ok|HFTreadyok" "$ui_body" \
  && rg -q "UI.*dashboard online|UIdashboard online" "$ui_body" \
  && ! rg -q "Internal Server Error|plugin:vite|Pre-transform error|Error Overlay" "$ui_body" \
  && [[ "$ui_pause_ok" -eq 1 ]] \
  && [[ "$ui_desktop_overflow_ok" -eq 1 ]] \
  && [[ "$ui_mobile_render_ok" -eq 1 ]]; then
  ui_render_ok=1
fi

hft_latest="$(tail -n 1 "$hft_dir/latency_budget.csv")"
hft_status="$(printf '%s\n' "$hft_latest" | awk -F, '{print $3}')"
hft_blockers="$(printf '%s\n' "$hft_latest" | awk -F, '{print $4}')"
hft_latency_ms="$(printf '%s\n' "$hft_latest" | awk -F, '{print $5}')"
hft_quote_tokens_unique_selected="$(printf '%s\n' "$hft_latest" | awk -F, '{print $12}')"
hft_quote_rest_requested="$(printf '%s\n' "$hft_latest" | awk -F, '{print $14}')"
hft_quote_rest_resolved="$(printf '%s\n' "$hft_latest" | awk -F, '{print $15}')"
hft_quote_rest_resolution_pct="$(printf '%s\n' "$hft_latest" | awk -F, '{print $17}')"
hft_quote_hard_unresolved_tokens="$(printf '%s\n' "$hft_latest" | awk -F, '{print $19}')"
hft_scan_latest="$(tail -n 1 "$hft_dir/scan_summary.csv")"
hft_yes_selected_events="$(printf '%s\n' "$hft_scan_latest" | awk -F, '{print $14}')"
hft_no_selected_events="$(printf '%s\n' "$hft_scan_latest" | awk -F, '{print $15}')"
hft_bundle_markets_scanned="$(printf '%s\n' "$hft_scan_latest" | awk -F, '{print $16}')"
hft_quote_no_ask_tokens="$(printf '%s\n' "$hft_scan_latest" | awk -F, '{print $23}')"
hft_quote_missing_book_tokens="$(printf '%s\n' "$hft_scan_latest" | awk -F, '{print $24}')"
hft_candidate_evaluation_rows="$(csv_data_rows "$hft_dir/candidate_evaluations.csv")"
hft_candidate_rejection_rows="$(csv_data_rows "$hft_dir/candidate_rejections.csv")"
hft_diagnostics_ok="$(
  awk \
    -v unique="$hft_quote_tokens_unique_selected" \
    -v missing_book="$hft_quote_missing_book_tokens" \
    -v yes="$hft_yes_selected_events" \
    -v no="$hft_no_selected_events" \
    -v bundle="$hft_bundle_markets_scanned" \
    -v evaluations="$hft_candidate_evaluation_rows" \
    'BEGIN { print ((unique + 0) > 0 && (missing_book + 0) == 0 && ((yes + 0) + (no + 0) + (bundle + 0)) > 0 && (evaluations + 0) > 0) ? 1 : 0 }'
)"

paper_state="$(jq -r '.items[] | select(.key=="paper") | .state' "$run_root/readiness.json")"
paper_balance_ok="$(jq -r '.ok // false' "$paper_balance_json")"
paper_history_ok="$(jq -r '.ok // false' "$paper_history_json")"
paper_cash="$(jq -r '.data.cash // empty' "$paper_balance_json")"
paper_starting_balance="$(jq -r '.data.starting_balance // empty' "$paper_balance_json")"
paper_positions_value="$(jq -r '.data.positions_value // empty' "$paper_balance_json")"
paper_total_value="$(jq -r '.data.total_value // empty' "$paper_balance_json")"
paper_pnl="$(jq -r '.data.pnl // empty' "$paper_balance_json")"
paper_trade_count="$(jq -r '(.data // []) | length' "$paper_history_json")"
paper_execution_canary_ok="$(jq -r '.ok // false' "$paper_execution_canary_json")"
paper_execution_canary_trade_count="$(jq -r '.trade_count // 0' "$paper_execution_canary_json")"
paper_execution_canary_live_attempted="$(
  jq -r 'if has("live_trade_attempted") then .live_trade_attempted else true end' "$paper_execution_canary_json"
)"
paper_scanner_trade_proof_ok="$(jq -r '.ok // false' "$paper_scanner_trade_proof_json")"
paper_scanner_trade_proof_paper_ok_rows="$(jq -r '.paper_ok_rows // 0' "$paper_scanner_trade_proof_json")"
paper_scanner_trade_proof_live_attempted="$(
  jq -r 'if has("live_trade_attempted") then .live_trade_attempted else true end' "$paper_scanner_trade_proof_json"
)"
paper_scanner_trade_proof_plan_hash="$(jq -r '.synthetic_plan_hash // empty' "$paper_scanner_trade_proof_json")"
paper_scanner_trade_proof_decision_parity_ok="$(
  jq -r '.decision_path_parity.ok // false' "$paper_scanner_trade_proof_json"
)"
hft_state="$(jq -r '.items[] | select(.key=="hft") | .state' "$run_root/readiness.json")"
ui_state="$(jq -r '.items[] | select(.key=="ui") | .state' "$run_root/readiness.json")"
live_state="$(jq -r '.items[] | select(.key=="live") | .state' "$run_root/readiness.json")"
live_supported="$(jq -r '.live_submissions_supported // false' "$live_dir/live_readiness_report.json")"
combo_promoted="$(jq -r '.promoted // false' "$live_dir/combo_rfq_route_promotion_report.json")"
live_trade_rows="$(live_submit_rows "$live_dir/trades.csv")"
code_trade_rows="$(live_submit_rows "$code_trade_log")"
live_guard_trade_rows="$(live_submit_rows "$live_guard_trade_log")"
live_combo_journal_rows="$(nonempty_rows "$live_combo_execution_journal")"
code_combo_journal_rows="$(nonempty_rows "$code_combo_execution_journal")"
live_guard_combo_journal_rows="$(nonempty_rows "$live_guard_combo_execution_journal")"
live_standard_journal_rows="$(nonempty_rows "$live_standard_execution_journal")"
code_standard_journal_rows="$(nonempty_rows "$code_standard_execution_journal")"
live_guard_standard_journal_rows="$(nonempty_rows "$live_guard_standard_execution_journal")"
live_submit_log_markers=0
if rg -qi "sdk_post_orders|post_orders|submit_orders|submitted live|live order submitted|placing live order|CLOB order submit|order submission succeeded|accepted_pending_finality" "$live_diag_log" "$code_ceiling_log" "$live_guard_log"; then
  live_submit_log_markers=1
fi

section "global no-live submission scan"
: >"$global_live_trade_hits"
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
      print file ":" NR ":" $0
    }
  ' "$trade_file" >>"$global_live_trade_hits"
done < <(find "$run_root" -type f -name 'trades.csv' -print0)

: >"$global_combo_journal_hits"
while IFS= read -r -d '' journal_file; do
  awk -v file="$journal_file" '
    $0 !~ /^[[:space:]]*$/ {
      line = tolower($0)
      if (line ~ /"status"[[:space:]]*:[[:space:]]*"blocked/) next
      print file ":" FNR ":" $0
    }
  ' "$journal_file" >>"$global_combo_journal_hits"
done < <(find "$run_root" -type f -name 'combo_rfq_execution_journal.jsonl' -print0)

: >"$global_standard_journal_hits"
while IFS= read -r -d '' journal_file; do
  awk -v file="$journal_file" '
    $0 !~ /^[[:space:]]*$/ {
      line = tolower($0)
      if (line ~ /"status"[[:space:]]*:[[:space:]]*"blocked/) next
      print file ":" FNR ":" $0
    }
  ' "$journal_file" >>"$global_standard_journal_hits"
done < <(find "$run_root" -type f -name 'live_execution_journal.jsonl' -print0)

: >"$global_submit_marker_hits"
find "$run_root" -type f \
  \( -name '*.log' -o -name '*.txt' -o -name '*.json' -o -name '*.jsonl' \) \
  ! -name "$(basename "$global_no_live_submit_scan_json")" \
  ! -name "$(basename "$global_live_trade_hits")" \
  ! -name "$(basename "$global_combo_journal_hits")" \
  ! -name "$(basename "$global_standard_journal_hits")" \
  ! -name "$(basename "$global_submit_marker_hits")" \
  ! -name "$(basename "$result_json")" \
  ! -name "$(basename "$readiness_bundle_manifest_json")" \
  ! -name "$(basename "$readiness_bundle_files_json")" \
  -print0 \
  | xargs -0 rg -H -n -I -i -e 'sdk_post_orders|post_orders|submit_orders|submitted live|live order submitted|placing live order|CLOB order submit|order submission succeeded|accepted_pending_finality' >"$global_submit_marker_hits" || true
global_live_trade_hit_count="$(awk 'END { print NR + 0 }' "$global_live_trade_hits")"
global_combo_journal_hit_count="$(awk 'END { print NR + 0 }' "$global_combo_journal_hits")"
global_standard_journal_hit_count="$(awk 'END { print NR + 0 }' "$global_standard_journal_hits")"
global_submit_marker_hit_count="$(awk 'END { print NR + 0 }' "$global_submit_marker_hits")"
global_no_live_submit_ok=0
if [[ "$global_live_trade_hit_count" -eq 0 \
  && "$global_combo_journal_hit_count" -eq 0 \
  && "$global_standard_journal_hit_count" -eq 0 \
  && "$global_submit_marker_hit_count" -eq 0 ]]; then
  global_no_live_submit_ok=1
fi
jq -n \
  --arg live_trade_hits "$global_live_trade_hits" \
  --arg combo_journal_hits "$global_combo_journal_hits" \
  --arg standard_journal_hits "$global_standard_journal_hits" \
  --arg submit_marker_hits "$global_submit_marker_hits" \
  --arg live_trade_hit_count "$global_live_trade_hit_count" \
  --arg combo_journal_hit_count "$global_combo_journal_hit_count" \
  --arg standard_journal_hit_count "$global_standard_journal_hit_count" \
  --arg submit_marker_hit_count "$global_submit_marker_hit_count" \
  --argjson ok "$(json_bool "$global_no_live_submit_ok")" \
  '{
    ok: $ok,
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
  }' >"$global_no_live_submit_scan_json"

section "live unblock plan"
jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg live_dir "$live_dir" \
  --slurpfile live_report "$live_dir/live_readiness_report.json" \
  --slurpfile combo_report "$live_dir/combo_rfq_route_promotion_report.json" \
  --slurpfile code_ceiling "$code_ceiling_json" \
  '
  def blockers:
    ([
      $live_report[0].checks[]?
      | select(.state != "ready")
      | {source: "live_readiness", key, state, detail}
    ] + [
      $combo_report[0].blockers[]?
      | {source: "combo_rfq_route_promotion", key: "combo_rfq_blocker", state: "blocked", detail: .}
    ]);
  def pick($re):
    blockers
    | map(select((.key + " " + (.detail | tostring)) | test($re; "i")));
  def env_item($name; $credential; $note):
    {name: $name, credential: $credential, value_recorded: false, note: $note};
  {
    generated_at: $generated_at,
    live_dir: $live_dir,
    credential_values_recorded: false,
    purpose: "operator-safe live unblock packet; env names and evidence only",
    current_state: {
      live_submissions_supported: ($live_report[0].live_submissions_supported // false),
      combo_rfq_promoted: ($combo_report[0].promoted // false),
      code_blocker_count: ($code_ceiling[0].code_blocker_count // null),
      source_static_blocker_count: ($code_ceiling[0].source_static_blocker_count // null)
    },
    required_envs: [
      env_item("POLYMARKET_PRIVATE_KEY"; true; "signer key; provide from shell or secret manager only"),
      env_item("LIVE_SIGNATURE_TYPE"; false; "0 EOA, 1 Proxy, 2 Safe, 3 Poly1271 deposit wallet"),
      env_item("LIVE_FUNDER_ADDRESS"; false; "required for non-EOA wallet modes"),
      env_item("POLYMARKET_API_KEY"; true; "authenticated CLOB REST/user-channel key"),
      env_item("POLYMARKET_API_SECRET"; true; "authenticated CLOB REST/user-channel credential"),
      env_item("POLYMARKET_API_PASSPHRASE"; true; "authenticated CLOB REST/user-channel credential"),
      env_item("LIVE_USER_WS_ENABLED"; false; "must be true before live submit"),
      env_item("LIVE_COMBO_RFQ_ROUTE_ENABLED"; false; "must be true after protocol proof"),
      env_item("COMBO_RFQ_REQUESTER_ENABLED"; false; "must be true after beta API proof"),
      env_item("COMBO_RFQ_BEARER_TOKEN"; true; "Combo/RFQ requester credential"),
      env_item("COMBO_RFQ_PARTICIPANT_ID"; false; "Combo/RFQ participant id"),
      env_item("COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED"; false; "operator attests create/query/accept/finality proof"),
      env_item("COMBO_RFQ_ACCEPT_ENABLED"; false; "must be true after accept-path proof"),
      env_item("COMBO_RFQ_STREAM_ENABLED"; false; "must be true for RFQ finality stream"),
      env_item("COMBO_RFQ_STREAM_BEARER_TOKEN"; true; "Combo/RFQ stream credential"),
      env_item("LIVE_CLOSEOUT_ENABLED"; false; "must be true after closeout preflight proof"),
      env_item("LIVE_CLOSEOUT_DRY_RUN"; false; "must be false only after closeout proof"),
      env_item("ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED"; false; "must be true for finalized-fill evidence"),
      env_item("SETTLEMENT_MONITOR_ENABLED"; false; "must be true for settlement hazard evidence"),
      env_item("POLYGON_RPC_URL"; true; "RPC URL may contain provider token; provide outside artifacts"),
      env_item("COMBO_RFQ_EXCHANGE_V3_ADDRESS"; false; "exchange v3 spender for allowance probes"),
      env_item("RELAYER_API_URL"; false; "needed for Poly1271 deposit wallet closeout"),
      env_item("RELAYER_API_KEY"; true; "needed for Poly1271 deposit wallet relayer"),
      env_item("RELAYER_API_KEY_ADDRESS"; false; "needed for Poly1271 deposit wallet relayer")
    ],
    operator_sequence: [
      {
        step: 1,
        name: "identity_and_clob_auth",
        goal: "live account, CLOB auth, and user-channel connected",
        evidence_needed: [
          "account_identity ready",
          "authenticated_clob_client ready",
          "closed_only_status ready",
          "user_channel_config ready",
          "user_channel_ready ready"
        ],
        blockers: pick("account_identity|authenticated_clob|closed_only|user_channel")
      },
      {
        step: 2,
        name: "funding_and_approvals",
        goal: "PUSD/POL balances and exchange approvals verified",
        evidence_needed: [
          "native_pol_balance ready or gasless proxy proof",
          "pusd_balance ready",
          "pusd allowance probes ready",
          "exchange v3 allowance ready",
          "ERC1155 operator approvals ready"
        ],
        blockers: pick("native_pol|pusd|allowance|erc1155|exchange_v3")
      },
      {
        step: 3,
        name: "combo_rfq_protocol",
        goal: "Combo/RFQ requester, accept path, stream, calibration, and maker scorecard proven",
        evidence_needed: [
          "requester create/query/accept/finality proof",
          "COMBO_RFQ_REQUESTER_PROTOCOL_VERIFIED true",
          "fresh stream/finality records",
          "calibration bucket present",
          "maker score samples present"
        ],
        blockers: pick("combo_rfq|rfq_|maker_score|calibration|requester|accept_gate|stream")
      },
      {
        step: 4,
        name: "settlement_and_closeout",
        goal: "closeout executor, finalized fill collection, and settlement hazard evidence ready",
        evidence_needed: [
          "closeout_execution ready",
          "settlement_revert_hazard ready",
          "polygon_finalized_block ready",
          "rfq_finality_stream ready"
        ],
        blockers: pick("closeout|settlement|finality|polygon_finalized|onchain")
      },
      {
        step: 5,
        name: "clean_start_and_live_flip",
        goal: "clean account, normal CLOB engine mode, no code blockers, then enable LIVE_TRADING_ENABLED",
        evidence_needed: [
          "clean_startup_account ready",
          "accounting_snapshot ready",
          "clob_engine_mode ready",
          "code_blocker_count 0",
          "artifact secret scan ok",
          "no-submit proof ok"
        ],
        blockers: pick("clean_startup|accounting_snapshot|clob_engine_mode")
      }
    ],
    raw_blockers: blockers
  }
  ' >"$live_unblock_plan_json"

section "artifact secret scan"
artifact_secret_pattern='(mpelteshki@gmail\.com|-----BEGIN[[:space:]]+(RSA[[:space:]]+|EC[[:space:]]+|OPENSSH[[:space:]]+)?PRIVATE[[:space:]]+KEY-----|authorization:?[[:space:]]+bearer[[:space:]]+[A-Za-z0-9_./+=-]{12,}|(private[_-]?key|api[_-]?secret|passphrase|secret|bearer[_-]?token|access[_-]?token|refresh[_-]?token|session[_-]?token|auth[_-]?token|api[_-]?token|resume[_-]?token|dropcopy[_-]?resume[_-]?token)[A-Z0-9_.-]{0,40}["=:][[:space:]]*"?[A-Za-z0-9_./+=-]{12,})'
find "$run_root" -type f \
  ! -name "$(basename "$artifact_secret_hits")" \
  ! -name "$(basename "$artifact_secret_scan_json")" \
  -print0 \
  | xargs -0 rg -H -n -I -i -e "$artifact_secret_pattern" >"$artifact_secret_hits" || true
artifact_secret_hit_count="$(awk 'END { print NR + 0 }' "$artifact_secret_hits")"
artifact_secret_scan_ok=0
if [[ "$artifact_secret_hit_count" -eq 0 ]]; then
  artifact_secret_scan_ok=1
fi
jq -n \
  --arg hits "$artifact_secret_hits" \
  --arg count "$artifact_secret_hit_count" \
  --arg pattern "redacted-readiness-artifact-secret-pattern" \
  --argjson ok "$(json_bool "$artifact_secret_scan_ok")" \
  '{
    ok: $ok,
    hit_count: ($count | tonumber? // 0),
    hits_path: $hits,
    pattern: $pattern
  }' >"$artifact_secret_scan_json"

paper_ok=0
hft_ok=0
ui_ok=0
live_ok=0
no_live_submit_ok=0

if [[ "$paper_state" == "ready" \
  && "$paper_balance_ok" == "true" \
  && "$paper_history_ok" == "true" \
  && "$paper_execution_canary_ok" == "true" \
  && "$paper_execution_canary_live_attempted" == "false" \
  && "$paper_execution_canary_trade_count" -gt 0 \
  && "$paper_scanner_trade_proof_ok" == "true" \
  && "$paper_scanner_trade_proof_live_attempted" == "false" \
  && "$paper_scanner_trade_proof_paper_ok_rows" -gt 0 ]]; then
  paper_ok=1
fi
[[ "$hft_state" == "ready" && "$hft_status" == "ok" && -z "$hft_blockers" && "$hft_diagnostics_ok" -eq 1 ]] && hft_ok=1
[[ "$ui_state" == "ready" && "$ui_render_ok" -eq 1 ]] && ui_ok=1
if [[ "$live_state" == "ready" && "$live_supported" == "true" && "$combo_promoted" == "true" ]]; then
  live_ok=1
fi
if [[ "$live_trade_rows" -eq 0 \
  && "$code_trade_rows" -eq 0 \
  && "$live_guard_trade_rows" -eq 0 \
  && "$live_combo_journal_rows" -eq 0 \
  && "$code_combo_journal_rows" -eq 0 \
  && "$live_guard_combo_journal_rows" -eq 0 \
  && "$live_standard_journal_rows" -eq 0 \
  && "$code_standard_journal_rows" -eq 0 \
  && "$live_guard_standard_journal_rows" -eq 0 \
  && "$live_submit_log_markers" -eq 0 \
  && "$runtime_panic_free" -eq 1 \
  && "$global_no_live_submit_ok" -eq 1 ]]; then
  no_live_submit_ok=1
fi

overall_state="blocked"
if [[ "$paper_ok" -eq 1 && "$hft_ok" -eq 1 && "$ui_ok" -eq 1 && "$live_ok" -eq 1 && "$live_fail_closed_ok" -eq 1 && "$no_live_submit_ok" -eq 1 && "$artifact_secret_scan_ok" -eq 1 ]]; then
  overall_state="ready"
elif [[ "$paper_ok" -eq 1 && "$hft_ok" -eq 1 && "$ui_ok" -eq 1 && "$live_fail_closed_ok" -eq 1 && "$no_live_submit_ok" -eq 1 && "$artifact_secret_scan_ok" -eq 1 ]]; then
  overall_state="live_blocked"
fi

would_exit_zero=0
if [[ "$paper_ok" -eq 1 && "$hft_ok" -eq 1 && "$ui_ok" -eq 1 && "$live_fail_closed_ok" -eq 1 && "$no_live_submit_ok" -eq 1 && "$artifact_secret_scan_ok" -eq 1 ]]; then
  if [[ "$live_ok" -eq 1 || "$allow_live_blocked" -eq 1 ]]; then
    would_exit_zero=1
  fi
fi

jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg overall_state "$overall_state" \
  --arg run_root "$run_root" \
  --arg paper_dir "$paper_dir" \
  --arg paper_balance_json "$paper_balance_json" \
  --arg paper_history_json "$paper_history_json" \
  --arg paper_cash "$paper_cash" \
  --arg paper_starting_balance "$paper_starting_balance" \
  --arg paper_positions_value "$paper_positions_value" \
  --arg paper_total_value "$paper_total_value" \
  --arg paper_pnl "$paper_pnl" \
  --arg paper_trade_count "$paper_trade_count" \
  --arg paper_execution_canary_json "$paper_execution_canary_json" \
  --arg paper_execution_canary_trade_count "$paper_execution_canary_trade_count" \
  --arg paper_scanner_trade_proof_json "$paper_scanner_trade_proof_json" \
  --arg paper_profitability_report_json "$paper_profitability_report_json" \
  --arg paper_profitability_trades_csv "$paper_profitability_trades_csv" \
  --arg paper_profitability_attempts_jsonl "$paper_profitability_attempts_jsonl" \
  --arg paper_scanner_trade_proof_paper_ok_rows "$paper_scanner_trade_proof_paper_ok_rows" \
  --arg paper_adapter_unit_proof_test "$paper_adapter_unit_proof_test" \
  --arg hft_dir "$hft_dir" \
  --arg live_dir "$live_dir" \
  --arg dashboard_url "$dashboard_url" \
  --arg dashboard_log "$dashboard_log" \
  --arg browse_log "$browse_log" \
  --arg live_diag_log "$live_diag_log" \
  --arg code_ceiling_log "$code_ceiling_log" \
  --arg live_guard_log "$live_guard_log" \
  --arg live_guard_trade_log "$live_guard_trade_log" \
  --arg live_combo_execution_journal "$live_combo_execution_journal" \
  --arg live_standard_execution_journal "$live_standard_execution_journal" \
  --arg live_engine_mode_report "$live_engine_mode_report" \
  --arg live_engine_mode_state "$live_engine_mode_state" \
  --arg live_engine_mode_journal "$live_engine_mode_journal" \
  --arg code_ceiling_json "$code_ceiling_json" \
  --arg live_unblock_plan_json "$live_unblock_plan_json" \
  --arg no_live_secret_env_list "$no_live_secret_env_list" \
  --arg global_no_live_submit_scan_json "$global_no_live_submit_scan_json" \
  --arg global_live_trade_hits "$global_live_trade_hits" \
  --arg global_combo_journal_hits "$global_combo_journal_hits" \
  --arg global_standard_journal_hits "$global_standard_journal_hits" \
  --arg global_submit_marker_hits "$global_submit_marker_hits" \
  --arg global_live_trade_hit_count "$global_live_trade_hit_count" \
  --arg global_combo_journal_hit_count "$global_combo_journal_hit_count" \
  --arg global_standard_journal_hit_count "$global_standard_journal_hit_count" \
  --arg global_submit_marker_hit_count "$global_submit_marker_hit_count" \
  --arg artifact_secret_hits "$artifact_secret_hits" \
  --arg artifact_secret_scan_json "$artifact_secret_scan_json" \
  --arg artifact_secret_hit_count "$artifact_secret_hit_count" \
  --arg code_trade_log "$code_trade_log" \
  --arg code_combo_execution_journal "$code_combo_execution_journal" \
  --arg code_standard_execution_journal "$code_standard_execution_journal" \
  --arg live_guard_combo_execution_journal "$live_guard_combo_execution_journal" \
  --arg live_guard_standard_execution_journal "$live_guard_standard_execution_journal" \
  --arg live_guard_exit "$live_guard_exit" \
  --argjson live_guard_reason_ok "$(json_bool "$live_guard_reason_ok")" \
  --argjson live_guard_panic_free "$(json_bool "$live_guard_panic_free")" \
  --arg live_trade_rows "$live_trade_rows" \
  --arg code_trade_rows "$code_trade_rows" \
  --arg live_guard_trade_rows "$live_guard_trade_rows" \
  --arg live_combo_journal_rows "$live_combo_journal_rows" \
  --arg code_combo_journal_rows "$code_combo_journal_rows" \
  --arg live_guard_combo_journal_rows "$live_guard_combo_journal_rows" \
  --arg live_standard_journal_rows "$live_standard_journal_rows" \
  --arg code_standard_journal_rows "$code_standard_journal_rows" \
  --arg live_guard_standard_journal_rows "$live_guard_standard_journal_rows" \
  --arg live_submit_log_markers "$live_submit_log_markers" \
  --arg ui_body "$ui_body" \
  --arg ui_snapshot "$ui_snapshot" \
  --arg ui_after_pause_snapshot "$ui_after_pause_snapshot" \
  --arg ui_screenshot "$ui_screenshot" \
  --arg ui_desktop_overflow "$ui_desktop_overflow" \
  --arg ui_mobile_body "$ui_mobile_body" \
  --arg ui_mobile_snapshot "$ui_mobile_snapshot" \
  --arg ui_mobile_screenshot "$ui_mobile_screenshot" \
  --arg ui_mobile_overflow "$ui_mobile_overflow" \
  --arg hft_status "$hft_status" \
  --arg hft_blockers "$hft_blockers" \
  --arg hft_latency_ms "$hft_latency_ms" \
  --arg hft_quote_tokens_unique_selected "$hft_quote_tokens_unique_selected" \
  --arg hft_quote_rest_requested "$hft_quote_rest_requested" \
  --arg hft_quote_rest_resolved "$hft_quote_rest_resolved" \
  --arg hft_quote_rest_resolution_pct "$hft_quote_rest_resolution_pct" \
  --arg hft_quote_hard_unresolved_tokens "$hft_quote_hard_unresolved_tokens" \
  --arg hft_yes_selected_events "$hft_yes_selected_events" \
  --arg hft_no_selected_events "$hft_no_selected_events" \
  --arg hft_bundle_markets_scanned "$hft_bundle_markets_scanned" \
  --arg hft_quote_no_ask_tokens "$hft_quote_no_ask_tokens" \
  --arg hft_quote_missing_book_tokens "$hft_quote_missing_book_tokens" \
  --arg hft_candidate_evaluation_rows "$hft_candidate_evaluation_rows" \
  --arg hft_candidate_rejection_rows "$hft_candidate_rejection_rows" \
  --arg runtime_panic_hits "$runtime_panic_hits" \
  --arg runtime_panic_hit_count "$runtime_panic_hit_count" \
  --argjson allow_live_blocked "$(json_bool "$allow_live_blocked")" \
  --argjson would_exit_zero "$(json_bool "$would_exit_zero")" \
  --argjson paper_ready "$(json_bool "$paper_ok")" \
  --argjson paper_adapter_unit_proof "$(json_bool "$paper_adapter_unit_proof_ok")" \
  --argjson hft_ready "$(json_bool "$hft_ok")" \
  --argjson hft_diagnostics_ready "$(json_bool "$hft_diagnostics_ok")" \
  --argjson ui_ready "$(json_bool "$ui_ok")" \
  --argjson ui_rendered "$(json_bool "$ui_render_ok")" \
  --argjson ui_pause_switch "$(json_bool "$ui_pause_ok")" \
  --argjson ui_desktop_overflow_ok "$(json_bool "$ui_desktop_overflow_ok")" \
  --argjson ui_mobile_rendered "$(json_bool "$ui_mobile_render_ok")" \
  --argjson ui_mobile_overflow_ok "$(json_bool "$ui_mobile_overflow_ok")" \
  --argjson live_ready "$(json_bool "$live_ok")" \
  --argjson live_fail_closed "$(json_bool "$live_fail_closed_ok")" \
  --argjson no_live_submit "$(json_bool "$no_live_submit_ok")" \
  --argjson global_no_live_submit "$(json_bool "$global_no_live_submit_ok")" \
  --argjson artifact_secret_scan_ok "$(json_bool "$artifact_secret_scan_ok")" \
  --argjson runtime_panic_free "$(json_bool "$runtime_panic_free")" \
  --argjson live_supported "$live_supported" \
  --argjson combo_promoted "$combo_promoted" \
  --slurpfile readiness "$run_root/readiness.json" \
  --slurpfile paper_execution_canary "$paper_execution_canary_json" \
  --slurpfile paper_scanner_trade_proof "$paper_scanner_trade_proof_json" \
  --slurpfile paper_profitability "$paper_profitability_report_json" \
  --slurpfile live_report "$live_dir/live_readiness_report.json" \
  --slurpfile combo_report "$live_dir/combo_rfq_route_promotion_report.json" \
  --slurpfile code_ceiling "$code_ceiling_json" \
  --slurpfile live_unblock_plan "$live_unblock_plan_json" \
  '
  def live_action:
    if .key == "live_route_matrix" then
      "Promote one live route. For Combo/RFQ set LIVE_COMBO_RFQ_ROUTE_ENABLED=true and clear combo_rfq_blockers."
    elif .key == "clob_engine_mode" then
      "Run live diagnostics until CLOB/status-page engine mode has a fresh normal observation."
    elif .key == "protocol_drift" then
      "Resolve protocol drift report for listed checks before live submit."
    elif .key == "user_channel_config" then
      "Set LIVE_USER_WS_ENABLED=true and configure authenticated CLOB user websocket credentials."
    elif .key == "user_channel_ready" then
      "Start authenticated user-channel supervision and wait for a fresh connected same-process status."
    elif .key == "closeout_execution" then
      "Enable executable closeout path with LIVE_CLOSEOUT_ENABLED=true and LIVE_CLOSEOUT_DRY_RUN=false only after closeout action preflight is safe."
    elif .key == "erc1155_operator_approval" then
      "Configure live account and exchange spender, then pass ERC1155 operator approval probe."
    elif .key == "account_identity" then
      "Set POLYMARKET_PRIVATE_KEY and matching live signature/funder settings."
    elif .key == "accounting_snapshot" then
      "Pass live account accounting snapshot with no disallowed retained positions."
    elif .key == "native_pol_balance" then
      "Fund live account with enough POL for required gas path, or prove gasless proxy mode applies."
    elif .key == "authenticated_clob_client" then
      "Authenticate CLOB SDK client with POLYMARKET_PRIVATE_KEY and live signature/funder settings."
    elif .key == "closed_only_status" then
      "Pass authenticated CLOB closed-only account probe."
    elif (.key | startswith("pusd_allowance")) then
      "Approve or verify enough PUSD allowance for required exchange contract."
    elif .key == "pusd_balance" then
      "Fund live account with enough PUSD collateral for configured trade size."
    elif .key == "exchange_v3_allowance" then
      "Set COMBO_RFQ_EXCHANGE_V3_ADDRESS and approve or verify exchange v3 allowance."
    elif .key == "clean_startup_account" then
      "Clear open orders and retained positions before live startup."
    else
      "Inspect readiness detail and clear this gate before live submit."
    end;
  def mentioned_envs:
    ([.detail | scan("[A-Z][A-Z0-9_]{2,}")])
    | map(gsub("_+$"; ""))
    | map(select(test("^(LIVE|COMBO|POLYMARKET|POLYGON|ONCHAIN|SETTLEMENT|CLOB)_")));
  {
    generated_at: $generated_at,
    overall_state: $overall_state,
    exit_policy: {
      allow_live_blocked: $allow_live_blocked,
      would_exit_zero: $would_exit_zero
    },
    checks: {
      static: {
        ready: true,
        rust_tests: true,
        profitability_gate_tests: true,
        dashboard_lint: true,
        dashboard_build: true
      },
      runtime_panic_scan: {
        ok: $runtime_panic_free,
        hit_count: ($runtime_panic_hit_count | tonumber? // null),
        hits_path: $runtime_panic_hits
      },
      protocol: (([
        $live_report[0].checks[]?
        | select(.key == "protocol_drift")
      ] | first) // {
        key: "protocol_drift",
        state: "unknown",
        detail: "protocol_drift_check_missing"
      }),
      artifact_secret_scan: {
        ok: $artifact_secret_scan_ok,
        hit_count: ($artifact_secret_hit_count | tonumber? // null),
        hits_path: $artifact_secret_hits,
        report: $artifact_secret_scan_json
      },
      paper: {
        ready: $paper_ready,
        account: "smoke-arb",
        adapter_unit_proof: {
          ok: $paper_adapter_unit_proof,
          test: $paper_adapter_unit_proof_test,
          proves: "ExternalPaperEngine.execute_opportunity invokes external paper buy commands and parses filled legs using mock pm-trader and mock CLOB"
        },
        balance: {
          cash: ($paper_cash | tonumber? // null),
          starting_balance: ($paper_starting_balance | tonumber? // null),
          positions_value: ($paper_positions_value | tonumber? // null),
          total_value: ($paper_total_value | tonumber? // null),
          pnl: ($paper_pnl | tonumber? // null)
        },
        trade_count: ($paper_trade_count | tonumber? // null),
        execution_canary: {
          ok: ($paper_execution_canary[0].ok // false),
          trade_count: ($paper_execution_canary_trade_count | tonumber? // 0),
          live_trade_attempted: ($paper_execution_canary[0] | if has("live_trade_attempted") then .live_trade_attempted else true end),
          market: ($paper_execution_canary[0].market // null),
          trade_id: ($paper_execution_canary[0].trade_id // null),
          order_type: ($paper_execution_canary[0].order_type // null),
          avg_price: ($paper_execution_canary[0].avg_price // null),
          shares: ($paper_execution_canary[0].shares // null),
          report: $paper_execution_canary_json
        },
        scanner_trade_proof: {
          ok: ($paper_scanner_trade_proof[0].ok // false),
          synthetic: ($paper_scanner_trade_proof[0].synthetic // true),
          counts_for_profitability: ($paper_scanner_trade_proof[0].counts_for_profitability // false),
          live_trade_attempted: ($paper_scanner_trade_proof[0] | if has("live_trade_attempted") then .live_trade_attempted else true end),
          synthetic_plan_hash: ($paper_scanner_trade_proof[0].synthetic_plan_hash // null),
          synthetic_plan_hash_algorithm: ($paper_scanner_trade_proof[0].synthetic_plan_hash_algorithm // null),
          decision_path_parity: ($paper_scanner_trade_proof[0].decision_path_parity // {}),
          paper_ok_rows: ($paper_scanner_trade_proof_paper_ok_rows | tonumber? // 0),
          trade_rows: ($paper_scanner_trade_proof[0].trade_rows // 0),
          scanner_can_execute_on_polymarket: ($paper_scanner_trade_proof[0].scanner_can_execute_on_polymarket // false),
          conservative_pnl_usd: ($paper_scanner_trade_proof[0].conservative_pnl_usd // null),
          fill_count: ($paper_scanner_trade_proof[0].fill_count // null),
          trades_csv: ($paper_scanner_trade_proof[0].trades_csv // null),
          report: $paper_scanner_trade_proof_json
        },
        profitability_evidence: ($paper_profitability[0] + {
          report: $paper_profitability_report_json,
          source_trades_csv: $paper_profitability_trades_csv,
          source_attempts_jsonl: $paper_profitability_attempts_jsonl
        }),
        balance_json: $paper_balance_json,
        history_json: $paper_history_json
      },
      hft: {
        ready: $hft_ready,
        status: $hft_status,
        blockers: (if $hft_blockers == "" then [] else ($hft_blockers | split(" | ")) end),
        latency_ms: ($hft_latency_ms | tonumber? // null),
        diagnostics_ready: $hft_diagnostics_ready,
        quote_tokens_unique_selected: ($hft_quote_tokens_unique_selected | tonumber? // null),
        quote_rest_requested: ($hft_quote_rest_requested | tonumber? // null),
        quote_rest_resolved: ($hft_quote_rest_resolved | tonumber? // null),
        quote_rest_resolution_pct: ($hft_quote_rest_resolution_pct | tonumber? // null),
        quote_hard_unresolved_tokens: ($hft_quote_hard_unresolved_tokens | tonumber? // null),
        quote_no_ask_tokens: ($hft_quote_no_ask_tokens | tonumber? // null),
        quote_missing_book_tokens: ($hft_quote_missing_book_tokens | tonumber? // null),
        yes_selected_events: ($hft_yes_selected_events | tonumber? // null),
        no_selected_events: ($hft_no_selected_events | tonumber? // null),
        bundle_markets_scanned: ($hft_bundle_markets_scanned | tonumber? // null),
        candidate_evaluation_rows: ($hft_candidate_evaluation_rows | tonumber? // null),
        candidate_rejection_rows: ($hft_candidate_rejection_rows | tonumber? // null)
      },
      ui: {
        ready: $ui_ready,
        rendered: $ui_rendered,
        pause_switch_checked_after_click: $ui_pause_switch,
        screenshot: $ui_screenshot,
        desktop_viewport: {
          width: 1440,
          height: 900,
          no_horizontal_overflow: $ui_desktop_overflow_ok,
          overflow: $ui_desktop_overflow
        },
        mobile_viewport: {
          width: 390,
          height: 844,
          rendered: $ui_mobile_rendered,
          no_horizontal_overflow: $ui_mobile_overflow_ok,
          screenshot: $ui_mobile_screenshot,
          body_text: $ui_mobile_body,
          snapshot: $ui_mobile_snapshot,
          overflow: $ui_mobile_overflow
        },
        body_text: $ui_body,
        snapshot: $ui_snapshot,
        after_pause_snapshot: $ui_after_pause_snapshot
      },
      live: {
        ready: $live_ready,
        live_submissions_supported: $live_supported,
        combo_rfq_promoted: $combo_promoted,
        no_live_secret_isolation: {
          ok: true,
          credential_values_recorded: false,
          isolated_envs: ($no_live_secret_env_list | split("\n") | map(select(length > 0))),
          policy: "no-live verifier commands run through env -u; code-ceiling credentials use fixed redacted dummy values"
        },
        fail_closed_guard: {
          ok: $live_fail_closed,
          exit_code: ($live_guard_exit | tonumber? // null),
          expected_reason_seen: $live_guard_reason_ok,
          panic_free: $live_guard_panic_free,
          log: $live_guard_log
        },
        no_submission: {
          ok: $no_live_submit,
          live_trades_rows: ($live_trade_rows | tonumber? // null),
          code_ceiling_trades_rows: ($code_trade_rows | tonumber? // null),
          fail_closed_trades_rows: ($live_guard_trade_rows | tonumber? // null),
          combo_rfq_execution_journal_rows: ($live_combo_journal_rows | tonumber? // null),
          code_ceiling_combo_rfq_execution_journal_rows: ($code_combo_journal_rows | tonumber? // null),
          fail_closed_combo_rfq_execution_journal_rows: ($live_guard_combo_journal_rows | tonumber? // null),
          standard_execution_journal_rows: ($live_standard_journal_rows | tonumber? // null),
          code_ceiling_standard_execution_journal_rows: ($code_standard_journal_rows | tonumber? // null),
          fail_closed_standard_execution_journal_rows: ($live_guard_standard_journal_rows | tonumber? // null),
          submit_log_markers_seen: ($live_submit_log_markers | tonumber? // null),
          global_scan: {
            ok: $global_no_live_submit,
            report: $global_no_live_submit_scan_json,
            live_trade_row_hits: ($global_live_trade_hit_count | tonumber? // null),
            combo_execution_journal_hits: ($global_combo_journal_hit_count | tonumber? // null),
            standard_execution_journal_hits: ($global_standard_journal_hit_count | tonumber? // null),
            submit_marker_hits: ($global_submit_marker_hit_count | tonumber? // null),
            hit_files: {
              live_trade_rows: $global_live_trade_hits,
              combo_execution_journals: $global_combo_journal_hits,
              standard_execution_journals: $global_standard_journal_hits,
              submit_markers: $global_submit_marker_hits
            }
          },
          checked_files: [
            $live_dir + "/trades.csv",
            $code_trade_log,
            $live_guard_trade_log,
            $live_combo_execution_journal,
            $live_standard_execution_journal,
            $code_combo_execution_journal,
            $code_standard_execution_journal,
            $live_guard_combo_execution_journal,
            $live_guard_standard_execution_journal,
            $live_diag_log,
            $code_ceiling_log,
            $live_guard_log
          ]
        },
        code_ceiling: $code_ceiling[0],
        unblock_plan: $live_unblock_plan[0],
        not_ready_checks: [
          $live_report[0].checks[]?
          | select(.state != "ready")
          | { key, state, detail, action: live_action, mentioned_envs: mentioned_envs }
        ],
        combo_rfq_blockers: ($combo_report[0].blockers // []),
        next_actions: [
          $live_report[0].checks[]?
          | select(.state != "ready")
          | { key, state, action: live_action, mentioned_envs: mentioned_envs }
        ],
        required_envs: ([
          $live_report[0].checks[]?
          | select(.state != "ready")
          | mentioned_envs[]
        ] + [
          $combo_report[0].blockers[]?
          | scan("[A-Z][A-Z0-9_]{2,}")
          | gsub("_+$"; "")
          | select(test("^(LIVE|COMBO|POLYMARKET|POLYGON|ONCHAIN|SETTLEMENT|CLOB)_"))
        ] | unique)
      }
    },
    next_live_actions: [
      $live_report[0].checks[]?
      | select(.state != "ready")
      | { key, state, action: live_action, mentioned_envs: mentioned_envs }
    ],
    dashboard_readiness: ($readiness[0].items // []),
    evidence: {
    run_root: $run_root,
    paper_dir: $paper_dir,
    paper_balance_json: $paper_balance_json,
    paper_history_json: $paper_history_json,
    paper_profitability_report_json: $paper_profitability_report_json,
    paper_profitability_trades_csv: $paper_profitability_trades_csv,
    paper_profitability_attempts_jsonl: $paper_profitability_attempts_jsonl,
    hft_dir: $hft_dir,
      live_dir: $live_dir,
    dashboard_url: $dashboard_url,
    dashboard_log: $dashboard_log,
    live_diag_log: $live_diag_log,
    code_ceiling_log: $code_ceiling_log,
      live_guard_log: $live_guard_log,
      code_ceiling_json: $code_ceiling_json,
      live_unblock_plan_json: $live_unblock_plan_json,
      global_no_live_submit_scan_json: $global_no_live_submit_scan_json,
      global_live_trade_hits: $global_live_trade_hits,
      global_combo_journal_hits: $global_combo_journal_hits,
      global_standard_journal_hits: $global_standard_journal_hits,
      global_submit_marker_hits: $global_submit_marker_hits,
      artifact_secret_hits: $artifact_secret_hits,
      artifact_secret_scan_json: $artifact_secret_scan_json,
      live_guard_trade_log: $live_guard_trade_log,
      live_combo_execution_journal: $live_combo_execution_journal,
      live_standard_execution_journal: $live_standard_execution_journal,
      live_engine_mode_report: $live_engine_mode_report,
      live_engine_mode_state: $live_engine_mode_state,
      live_engine_mode_journal: $live_engine_mode_journal,
      code_combo_execution_journal: $code_combo_execution_journal,
      code_standard_execution_journal: $code_standard_execution_journal,
      live_guard_combo_execution_journal: $live_guard_combo_execution_journal,
      live_guard_standard_execution_journal: $live_guard_standard_execution_journal,
    browse_log: $browse_log,
    ui_screenshot: $ui_screenshot,
    ui_mobile_screenshot: $ui_mobile_screenshot,
    ui_snapshot: $ui_snapshot,
    ui_mobile_snapshot: $ui_mobile_snapshot,
    ui_after_pause_snapshot: $ui_after_pause_snapshot
  }
  }' >"$result_json"

section "paper/live parity audit"
"$repo_root/scripts/paper-live-parity-audit.sh" \
  --result-json "$result_json" \
  --profitability-report "$paper_profitability_report_json" \
  --latency-csv "$hft_dir/latency_budget.csv" \
  --scan-summary "$hft_dir/scan_summary.csv" \
  --no-activation-packet \
  --output "$paper_live_parity_audit_json"

if ! curl -fsS "$dashboard_url/api/readiness" >"$run_root/readiness.json.tmp" 2>/dev/null; then
  echo "dashboard readiness API did not refresh after parity audit; log=$dashboard_log" >&2
  exit 1
fi
mv "$run_root/readiness.json.tmp" "$run_root/readiness.json"
if ! jq -e '
  .items[]
  | select(.key == "paper_live_parity")
  | (.state == "blocked" or .state == "ready")
    and ((.detail | tostring) | contains("unavailable") | not)
' "$run_root/readiness.json" >/dev/null; then
  echo "dashboard readiness API did not expose paper/live parity audit" >&2
  exit 1
fi
jq \
  --slurpfile readiness "$run_root/readiness.json" \
  '.dashboard_readiness = ($readiness[0].items // [])' \
  "$result_json" >"$result_json.tmp"
mv "$result_json.tmp" "$result_json"
browse viewport 1440 900 >/dev/null 2>>"$browse_log"
browse reload >/dev/null 2>>"$browse_log" || true
browse wait load >/dev/null 2>>"$browse_log" || true
browse get text body >"$ui_body" 2>>"$browse_log"
browse snapshot >"$ui_snapshot" 2>>"$browse_log"
browse screenshot --path "$ui_screenshot" >/dev/null 2>>"$browse_log"
browse viewport 390 844 --scale 2 >/dev/null 2>>"$browse_log"
browse reload >/dev/null 2>>"$browse_log" || true
browse wait load >/dev/null 2>>"$browse_log" || true
browse get text body >"$ui_mobile_body" 2>>"$browse_log"
browse snapshot >"$ui_mobile_snapshot" 2>>"$browse_log"
browse screenshot --path "$ui_mobile_screenshot" >/dev/null 2>>"$browse_log"

section "readiness bundle manifest"
: >"$readiness_bundle_files_json"
write_bundle_file_entry "release_binary" "$release_binary" >>"$readiness_bundle_files_json"
write_bundle_file_entry "build_provenance" "$build_provenance_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "no_live_identity_fingerprint" "$no_live_identity_fingerprint_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "trade_readiness_result" "$result_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "readiness_api_snapshot" "$run_root/readiness.json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_live_parity_audit" "$paper_live_parity_audit_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_profitability_report" "$paper_profitability_report_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_profitability_source_trades" "$paper_profitability_trades_csv" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_profitability_source_attempts" "$paper_profitability_attempts_jsonl" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_adapter_provenance" "$paper_adapter_provenance_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_balance" "$paper_balance_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_history" "$paper_history_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_execution_canary" "$paper_execution_canary_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_scanner_trade_proof" "$paper_scanner_trade_proof_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "hft_latency_budget" "$hft_dir/latency_budget.csv" >>"$readiness_bundle_files_json"
write_bundle_file_entry "hft_scan_summary" "$hft_dir/scan_summary.csv" >>"$readiness_bundle_files_json"
write_bundle_file_entry "hft_candidate_evaluations" "$hft_dir/candidate_evaluations.csv" >>"$readiness_bundle_files_json"
write_bundle_file_entry "hft_candidate_rejections" "$hft_dir/candidate_rejections.csv" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_readiness_report" "$live_dir/live_readiness_report.json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "combo_rfq_route_promotion_report" "$live_dir/combo_rfq_route_promotion_report.json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_engine_mode_report" "$live_engine_mode_report" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_engine_mode_state" "$live_engine_mode_state" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_engine_mode_journal" "$live_engine_mode_journal" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_code_ceiling_report" "$code_ceiling_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_unblock_plan" "$live_unblock_plan_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "global_no_live_submission_scan" "$global_no_live_submit_scan_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "global_live_trade_hits" "$global_live_trade_hits" >>"$readiness_bundle_files_json"
write_bundle_file_entry "global_combo_journal_hits" "$global_combo_journal_hits" >>"$readiness_bundle_files_json"
write_bundle_file_entry "global_standard_journal_hits" "$global_standard_journal_hits" >>"$readiness_bundle_files_json"
write_bundle_file_entry "global_submit_marker_hits" "$global_submit_marker_hits" >>"$readiness_bundle_files_json"
write_bundle_file_entry "artifact_secret_scan" "$artifact_secret_scan_json" >>"$readiness_bundle_files_json"
write_bundle_file_entry "artifact_secret_hits" "$artifact_secret_hits" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_diagnostics_log" "$live_diag_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_code_ceiling_log" "$code_ceiling_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "live_fail_closed_log" "$live_guard_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "rust_tests_log" "$rust_tests_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_adapter_test_log" "$paper_adapter_test_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_smoke_log" "$paper_smoke_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_execution_canary_log" "$paper_execution_canary_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "paper_scanner_trade_proof_log" "$paper_scanner_trade_proof_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "hft_smoke_log" "$hft_smoke_log" >>"$readiness_bundle_files_json"
write_bundle_file_entry "runtime_panic_hits" "$runtime_panic_hits" >>"$readiness_bundle_files_json"
write_bundle_file_entry "ui_desktop_screenshot" "$ui_screenshot" >>"$readiness_bundle_files_json"
write_bundle_file_entry "ui_mobile_screenshot" "$ui_mobile_screenshot" >>"$readiness_bundle_files_json"
write_bundle_file_entry "ui_desktop_snapshot" "$ui_snapshot" >>"$readiness_bundle_files_json"
write_bundle_file_entry "ui_mobile_snapshot" "$ui_mobile_snapshot" >>"$readiness_bundle_files_json"
jq -s '.' "$readiness_bundle_files_json" >"$readiness_bundle_files_json.tmp"
mv "$readiness_bundle_files_json.tmp" "$readiness_bundle_files_json"
jq -n \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg run_root "$run_root" \
  --arg dashboard_url "$dashboard_url" \
  --arg result_json "$result_json" \
  --arg release_binary "$release_binary" \
  --arg build_provenance_json "$build_provenance_json" \
  --arg no_live_secret_env_list "$no_live_secret_env_list" \
  --slurpfile result "$result_json" \
  --slurpfile parity "$paper_live_parity_audit_json" \
  --slurpfile build_provenance "$build_provenance_json" \
  --slurpfile no_live_identity "$no_live_identity_fingerprint_json" \
  --slurpfile profitability "$paper_profitability_report_json" \
  --slurpfile paper_adapter "$paper_adapter_provenance_json" \
  --slurpfile files "$readiness_bundle_files_json" \
  '{
    generated_at: $generated_at,
    run_root: $run_root,
    result_json: $result_json,
    dashboard_url: $dashboard_url,
    overall_state: ($result[0].overall_state // "unknown"),
    exit_policy: ($result[0].exit_policy // {}),
    build: {
      binary_path: $release_binary,
      provenance_path: $build_provenance_json,
      binary_sha256: ($build_provenance[0].binary.sha256 // null),
      inputs_unchanged_during_build: ($build_provenance[0].inputs_unchanged_during_build // false)
    },
    paper_execution_binding: {
      producer_binary_sha256_values: ($profitability[0].execution_binding.producer_executable_sha256 // []),
      expected_producer_binary_sha256: ($profitability[0].execution_binding.expected_producer_binary_sha256 // null),
      paper_adapter: ($paper_adapter[0] // {}),
      execution_profile_sha256_values: ($profitability[0].execution_binding.execution_profile_sha256 // []),
      execution_profile: ($profitability[0].execution_binding.execution_profile // null),
      paper_live_profile_config: ($profitability[0].execution_binding.paper_live_profile_config // null),
      paper_live_profile_config_sha256: ($profitability[0].execution_binding.paper_live_profile_config_sha256 // null),
      campaign_profit_compatibility_fingerprint: ($no_live_identity[0].profit_compatibility_fingerprint // null),
      profit_compatibility_fingerprint_values: ($profitability[0].execution_binding.profit_compatibility_fingerprints // []),
      paper_evidence_eligible: ($profitability[0].paper_evidence_eligible // false),
      live_route_compatible: ($profitability[0].live_route_compatible // false),
      activation_eligible_from_paper_alone: ($profitability[0].activation_eligible // false)
    },
    no_live_policy: {
      live_trade_attempted: false,
      account_created: false,
      ambient_secret_envs_stripped: ($no_live_secret_env_list | split("\n") | map(select(length > 0))),
      credential_values_recorded: false
    },
    pass_summary: {
      paper_ready: ($result[0].checks.paper.ready // false),
      paper_execution_canary_ok: ($result[0].checks.paper.execution_canary.ok // false),
      paper_adapter_unit_proof_ok: ($result[0].checks.paper.adapter_unit_proof.ok // false),
      paper_scanner_trade_proof_ok: ($result[0].checks.paper.scanner_trade_proof.ok // false),
      paper_live_decision_path_parity_ok: ($result[0].checks.paper.scanner_trade_proof.decision_path_parity.ok // false),
      hft_ready: ($result[0].checks.hft.ready // false),
      hft_fastest_path_proven: ($parity[0].verdict.hft_fastest_path_proven // false),
      ui_ready: ($result[0].checks.ui.ready // false),
      paper_profitable_proven: ($parity[0].verdict.paper_profitable_proven // false),
      paper_evidence_eligible: ($profitability[0].paper_evidence_eligible // false),
      paper_producer_binary_bound: ($profitability[0].execution_binding.expected_producer_matches // false),
      paper_campaign_binding_uniform: ($profitability[0].execution_binding.uniform_campaign_binding // false),
      paper_profitability_sample_count: ($parity[0].paper.profitability_evidence.sample.accepted_trades // 0),
      paper_live_identical: ($parity[0].verdict.paper_live_identical // false),
      live_no_submission_ok: ($result[0].checks.live.no_submission.ok // false),
      global_no_live_scan_ok: ($result[0].checks.live.no_submission.global_scan.ok // false),
      live_code_blocker_count: ($result[0].checks.live.code_ceiling.code_blocker_count // null),
      source_static_blocker_count: ($result[0].checks.live.code_ceiling.source_static_blocker_count // null),
      fail_closed_ok: ($result[0].checks.live.fail_closed_guard.ok // false),
      runtime_panic_free: ($result[0].checks.runtime_panic_scan.ok // false),
      artifact_secret_scan_ok: ($result[0].checks.artifact_secret_scan.ok // false)
    },
    live_unblock: {
      required_env_count: (($result[0].checks.live.unblock_plan.required_envs // []) | length),
      operator_step_count: (($result[0].checks.live.unblock_plan.operator_sequence // []) | length),
      raw_blocker_count: (($result[0].checks.live.unblock_plan.raw_blockers // []) | length),
      credential_values_recorded: (
        if ($result[0].checks.live.unblock_plan | has("credential_values_recorded")) then
          $result[0].checks.live.unblock_plan.credential_values_recorded
        else
          null
        end
      )
    },
    files: $files[0]
  }' >"$readiness_bundle_manifest_json"
if rg -q -I -i -e "$artifact_secret_pattern" "$readiness_bundle_manifest_json" "$readiness_bundle_files_json"; then
  echo "readiness bundle manifest secret scan failed; inspect $readiness_bundle_manifest_json" >&2
  exit 1
fi

section "readiness bundle verification"
"$repo_root/scripts/verify-readiness-bundle.sh" "$readiness_bundle_manifest_json" | tee "$readiness_bundle_verification_txt"

section "summary"
jq -r '.items[] | "\(.label): \(.state) / \(.value) / \(.detail)"' "$run_root/readiness.json"
echo "hft_latency_ms=$hft_latency_ms"
echo "live_fail_closed_exit=$live_guard_exit"
echo "live_no_submission=$no_live_submit_ok"
echo "global_no_live_submission=$global_no_live_submit_ok trade_hits=$global_live_trade_hit_count combo_journal_hits=$global_combo_journal_hit_count standard_journal_hits=$global_standard_journal_hit_count marker_hits=$global_submit_marker_hit_count"
echo "artifact_secret_scan=$artifact_secret_scan_ok hits=$artifact_secret_hit_count"
echo "runtime_panic_free=$runtime_panic_free hits=$runtime_panic_hit_count report=$runtime_panic_hits"
echo "paper_execution_canary=$paper_execution_canary_json ok=$paper_execution_canary_ok trades=$paper_execution_canary_trade_count live_attempted=$paper_execution_canary_live_attempted"
echo "paper_scanner_trade_proof=$paper_scanner_trade_proof_json ok=$paper_scanner_trade_proof_ok paper_ok_rows=$paper_scanner_trade_proof_paper_ok_rows live_attempted=$paper_scanner_trade_proof_live_attempted plan_hash=${paper_scanner_trade_proof_plan_hash:-missing} decision_parity=$paper_scanner_trade_proof_decision_parity_ok"
echo "paper_profitability_report=$paper_profitability_report_json verified=$(jq -r '.verified_profitable // false' "$paper_profitability_report_json") samples=$(jq -r '.sample.accepted_trades // 0' "$paper_profitability_report_json")"
echo "paper_live_parity_audit=$paper_live_parity_audit_json"
echo "ui_screenshot=$ui_screenshot"
echo "dashboard_url=$dashboard_url"
echo "run_root=$run_root"
echo "result_json=$result_json"
echo "readiness_bundle_manifest=$readiness_bundle_manifest_json"
echo "readiness_bundle_verification=$readiness_bundle_verification_txt"

if [[ "$paper_ok" -ne 1 ]]; then
  echo "paper readiness failed" >&2
  exit 1
fi
if [[ "$hft_ok" -ne 1 ]]; then
  echo "hft readiness failed" >&2
  exit 1
fi
if [[ "$ui_ok" -ne 1 ]]; then
  echo "ui readiness failed" >&2
  exit 1
fi
if [[ "$live_fail_closed_ok" -ne 1 ]]; then
  echo "live fail-closed guard failed; see $live_guard_log" >&2
  exit 1
fi
if [[ "$runtime_panic_free" -ne 1 ]]; then
  echo "runtime panic detected during readiness; inspect $runtime_panic_hits" >&2
  exit 1
fi
if [[ "$no_live_submit_ok" -ne 1 ]]; then
  echo "live no-submit proof failed; inspect live trades/journals under $run_root" >&2
  exit 1
fi
if [[ "$artifact_secret_scan_ok" -ne 1 ]]; then
  echo "artifact secret scan failed; inspect $artifact_secret_hits" >&2
  exit 1
fi
if [[ "$live_ok" -ne 1 && "$allow_live_blocked" -ne 1 ]]; then
  echo "live readiness failed; rerun with --allow-live-blocked to verify paper/HFT/UI only" >&2
  exit 1
fi

if [[ "$live_ok" -ne 1 ]]; then
  echo "live readiness blocked; paper/HFT/UI passed"
else
  echo "all readiness gates passed"
fi
