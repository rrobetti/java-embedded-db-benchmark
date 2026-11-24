#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
offline=false
benchmark_args=()
heap="512m"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --offline) offline=true; shift ;;
    --jvm-heap)
      [[ $# -ge 2 ]] || { echo "--jvm-heap needs a value" >&2; exit 2; }
      heap="$2"; benchmark_args+=("$1" "$2"); shift 2 ;;
    --jvm-heap=*) heap="${1#*=}"; benchmark_args+=("$1"); shift ;;
    *) benchmark_args+=("$1"); shift ;;
  esac
done
[[ "$heap" =~ ^[1-9][0-9]*[kKmMgG]?$ ]] || { echo "Invalid JVM heap: $heap" >&2; exit 2; }
if [[ -n "${JAVA_HOME:-}" ]]; then export PATH="$JAVA_HOME/bin:$PATH"; fi
source scripts/java-version.sh
require_java_25
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux)
    cpus="$(getconf _NPROCESSORS_ONLN)"
    ram="$(awk '/MemTotal:/ {printf "%.0f", $2 * 1024}' /proc/meminfo)"
    ;;
  Darwin) cpus="$(sysctl -n hw.logicalcpu)"; ram="$(sysctl -n hw.memsize)" ;;
  *) cpus="unknown"; ram="unknown" ;;
esac
printf 'OS=%s arch=%s logicalCPUs=%s physicalRAMBytes=%s\n' "$os" "$arch" "$cpus" "$ram"
echo "Resource policy: Java heap cap and supervisor safety checks; no OS hard limit is installed."
if [[ ! -f target/benchmark.jar ]]; then
  if "$offline"; then bash scripts/setup.sh --offline; else bash scripts/setup.sh; fi
fi
native_tmp="${BENCHMARK_NATIVE_TMP:-$PWD/target/native-tmp}"
mkdir -p "$native_tmp"
native_tmp="$(cd "$native_tmp" && pwd)"
[[ -w "$native_tmp" && -x "$native_tmp" ]] || { echo "Native temp permissions denied: $native_tmp" >&2; exit 1; }
if command -v findmnt >/dev/null && findmnt -no OPTIONS -T "$native_tmp" | grep -qw noexec; then
  echo "Set BENCHMARK_NATIVE_TMP to a writable executable filesystem (current mount is noexec)." >&2; exit 1
fi
if [[ ${#benchmark_args[@]} -eq 0 ]]; then benchmark_args=(--profile quick); fi
exec java "-Xmx$heap" "-Djava.io.tmpdir=$native_tmp" "-Dorg.sqlite.tmpdir=$native_tmp" -jar target/benchmark.jar "${benchmark_args[@]}"
