#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

offline=false
case "${1:-}" in
  "") ;;
  --offline) offline=true ;;
  *) echo "Usage: bash scripts/setup.sh [--offline]" >&2; exit 2 ;;
esac
[[ $# -le 1 ]] || { echo "Too many arguments" >&2; exit 2; }
if [[ -n "${JAVA_HOME:-}" ]]; then export PATH="$JAVA_HOME/bin:$PATH"; fi
command -v java >/dev/null || { echo "Install a JDK 21 manually and set JAVA_HOME." >&2; exit 1; }
java_version="$(java -version 2>&1)"
[[ "$java_version" =~ version\ \"21([.\"]|-) ]] || {
  printf 'JDK 21 is required, not the active runtime:\n%s\nSet JAVA_HOME to a JDK 21 installation.\n' "$java_version" >&2
  exit 1
}
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux)
    cpus="$(getconf _NPROCESSORS_ONLN)"
    ram="$(awk '/MemTotal:/ {printf "%.0f", $2 * 1024}' /proc/meminfo)"
    ;;
  Darwin)
    cpus="$(sysctl -n hw.logicalcpu)"
    ram="$(sysctl -n hw.memsize)"
    ;;
  *) cpus="unknown"; ram="unknown" ;;
esac
printf 'OS=%s arch=%s logicalCPUs=%s physicalRAMBytes=%s\n' "$os" "$arch" "$cpus" "$ram"
printf '%s\n' "$java_version"
native_tmp="${BENCHMARK_NATIVE_TMP:-$PWD/target/native-tmp}"
mkdir -p "$native_tmp"
native_tmp="$(cd "$native_tmp" && pwd)"
[[ -w "$native_tmp" && -x "$native_tmp" ]] || {
  echo "Native temp directory needs write and directory-search permissions: $native_tmp" >&2; exit 1;
}
if command -v findmnt >/dev/null && findmnt -no OPTIONS -T "$native_tmp" | grep -qw noexec; then
  echo "Native temp is on a noexec filesystem. Set BENCHMARK_NATIVE_TMP to a writable executable filesystem." >&2
  exit 1
fi
[[ -f mvnw ]] || { echo "Maven wrapper is missing; restore mvnw and .mvn/wrapper." >&2; exit 1; }
if "$offline"; then
  bash ./mvnw -B -o package
else
  bash ./mvnw -B dependency:go-offline
  bash ./mvnw -B package
fi
if ! java "-Djava.io.tmpdir=$native_tmp" -cp target/benchmark.jar io.github.embeddedbench.db.DatabaseSmoke; then
  echo "Connectivity failed. Check the complete error above, JDK 21, OS/architecture support and native extraction permissions." >&2
  echo "For native loader failures set BENCHMARK_NATIVE_TMP to a user-owned writable executable filesystem; no elevated installation is performed." >&2
  exit 1
fi
