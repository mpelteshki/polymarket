#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage: scripts/guarded-live-start.sh [--activation-packet PATH] [--gate-output PATH] --confirm-live [--no-paper] [-- --live-reconcile-run --confirm-live-closeout]

Verifies final live-readiness artifacts before starting live mode.

Requires:
  - activation packet verifies with --require-live-ready
  - copied release binary matches verified build provenance
  - current launch config matches the operator fingerprint (rechecked by Rust)
  - live-ready-gate passes with --require-live-env-enabled
  - LIVE_TRADING_ENABLED=true
  - explicit --confirm-live
  - paper execution is unconditionally disabled in the verified process

No live process starts unless every gate above passes.
EOF
}

activation_packet=""
gate_output=""
confirm_live=0
extra_args=()
extra_arg_count=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --activation-packet)
      activation_packet="${2:-}"
      if [[ -z "$activation_packet" ]]; then
        echo "--activation-packet requires a path" >&2
        exit 2
      fi
      shift 2
      ;;
    --gate-output)
      gate_output="${2:-}"
      if [[ -z "$gate_output" ]]; then
        echo "--gate-output requires a path" >&2
        exit 2
      fi
      shift 2
      ;;
    --confirm-live)
      confirm_live=1
      shift
      ;;
    --no-paper)
      # Accepted so the recorded operator command states the effective mode. The
      # release invocation below applies this flag even if the caller omits it.
      shift
      ;;
    --)
      shift
      extra_args=("$@")
      extra_arg_count=$#
      break
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

if [[ "$extra_arg_count" -ne 0 ]]; then
  for extra_arg in "${extra_args[@]}"; do
    case "$extra_arg" in
      --paper|--paper=*)
        echo "guarded live start refused: --paper conflicts with enforced --no-paper" >&2
        exit 2
        ;;
    esac
  done
  if [[ "$extra_arg_count" -ne 2 \
    || "${extra_args[0]}" != "--live-reconcile-run" \
    || "${extra_args[1]}" != "--confirm-live-closeout" ]]; then
    echo "guarded live start refused: unbound extra arguments are not allowed" >&2
    exit 2
  fi
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

need awk
need find
need jq
need mktemp
need sort
need shasum
need stat

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

latest_packet() {
  local found
  found="$(
    find -L /tmp -path '/tmp/polymarket-live-activation-packet-*/live-activation-packet.json' -type f -print 2>/dev/null \
      | while IFS= read -r path; do
          printf '%s\t%s\n' "$(stat -f '%m' "$path" 2>/dev/null || stat -c '%Y' "$path")" "$path"
        done \
      | sort -nr \
      | awk -F '\t' 'NR == 1 { print $2 }'
  )"
  if [[ -z "$found" ]]; then
    echo "no live activation packet found under /tmp" >&2
    exit 2
  fi
  echo "$found"
}

if [[ -z "$activation_packet" ]]; then
  activation_packet="$(latest_packet)"
fi
if [[ ! -f "$activation_packet" ]]; then
  echo "missing activation packet: $activation_packet" >&2
  exit 2
fi
activation_packet="$(cd "$(dirname "$activation_packet")" && pwd)/$(basename "$activation_packet")"
if [[ -z "$gate_output" ]]; then
  gate_output="$(mktemp "${TMPDIR:-/tmp}/polymarket-guarded-live-gate.XXXXXX.json")"
else
  gate_output="$(cd "$(dirname "$gate_output")" && pwd)/$(basename "$gate_output")"
fi
cd "$repo_root"

readiness_manifest="$(jq -r '.artifacts.readiness_manifest // empty' "$activation_packet")"
operator_manifest="$(jq -r '.artifacts.operator_preflight_manifest // empty' "$activation_packet")"
if [[ -z "$readiness_manifest" || ! -f "$readiness_manifest" ]]; then
  echo "activation packet missing readiness manifest path" >&2
  exit 2
fi
if [[ -z "$operator_manifest" || ! -f "$operator_manifest" ]]; then
  echo "activation packet missing operator preflight manifest path" >&2
  exit 2
fi
operator_result="$(jq -r '.result_json // empty' "$operator_manifest")"
operator_live_dir="$(jq -r '.live_dir // empty' "$operator_result" 2>/dev/null || true)"
paper_adapter_path="$(jq -r '.paper_execution_binding.paper_adapter.canonical_path // empty' "$readiness_manifest")"
if [[ -z "$operator_result" || ! -f "$operator_result" \
  || -z "$operator_live_dir" || ! -d "$operator_live_dir" || -L "$operator_live_dir" \
  || -z "$paper_adapter_path" || ! -x "$paper_adapter_path" ]]; then
  echo "activation operator manifest lacks a safe diagnostics directory" >&2
  exit 2
fi
release_binary="$(jq -r '.files[]? | select(.label == "release_binary") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
build_provenance="$(jq -r '.files[]? | select(.label == "build_provenance") | .path' "$readiness_manifest" | awk 'NR == 1 { print }')"
if [[ -z "$release_binary" || ! -x "$release_binary" ]]; then
  echo "activation readiness manifest missing executable release binary" >&2
  exit 2
fi
if [[ -z "$build_provenance" || ! -f "$build_provenance" ]]; then
  echo "activation readiness manifest missing build provenance" >&2
  exit 2
fi

"$repo_root/scripts/verify-live-activation-packet.sh" --require-live-ready "$activation_packet"
"$repo_root/scripts/live-ready-gate.sh" \
  --require-live-env-enabled \
  --json \
  --output "$gate_output" \
  --readiness-manifest "$readiness_manifest" \
  --operator-preflight-manifest "$operator_manifest" >/dev/null

if [[ "${LIVE_TRADING_ENABLED:-false}" != "true" ]]; then
  echo "guarded live start refused: LIVE_TRADING_ENABLED is not true" >&2
  exit 1
fi
if [[ "$confirm_live" -ne 1 ]]; then
  echo "guarded live start refused: --confirm-live is required" >&2
  exit 1
fi

expected_binary_sha="$(jq -r '.binary.sha256 // empty' "$build_provenance")"
manifest_binary_sha="$(jq -r --arg binary "$release_binary" '.files[]? | select(.label == "release_binary" and .path == $binary) | .sha256 // empty' "$readiness_manifest" | awk 'NR == 1 { print }')"
actual_binary_sha="$(shasum -a 256 "$release_binary" | awk '{print $1}')"
if [[ -z "$expected_binary_sha" \
  || "$expected_binary_sha" != "$manifest_binary_sha" \
  || "$expected_binary_sha" != "$actual_binary_sha" ]]; then
  echo "guarded live start refused: release binary hash does not match verified provenance" >&2
  exit 1
fi

launch_command=(
  env
  "DIAGNOSTICS_DIR=$operator_live_dir"
  LIVE_DIAGNOSTICS_ENABLED=true
  PAPER_TRADING_ENABLED=false
  "EXTERNAL_PAPER_COMMAND=$paper_adapter_path"
  "$release_binary"
  --live
  --no-paper
  --guarded-live-confirmed
  --activation-packet
  "$activation_packet"
)
if [[ "$extra_arg_count" -ne 0 ]]; then
  launch_command+=("${extra_args[@]}")
fi
exec "${launch_command[@]}"
