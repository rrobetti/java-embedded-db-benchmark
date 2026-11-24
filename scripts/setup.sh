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
source scripts/java-version.sh
prepare_java_build
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
  distribution_url="$(sed -n 's/^distributionUrl=//p' .mvn/wrapper/maven-wrapper.properties | tr -d '[:space:]')"
  [[ -n "$distribution_url" ]] || { echo "Missing wrapper distributionUrl." >&2; exit 1; }
  if [[ -n "${MVNW_REPOURL:-}" ]]; then
    distribution_url="$MVNW_REPOURL/org/apache/maven/${distribution_url#*/org/apache/maven/}"
  fi
  distribution_name="${distribution_url##*/}"
  distribution_name="${distribution_name%.zip}"
  distribution_name="${distribution_name%-bin}"
  hash=0
  for ((i=0; i<${#distribution_url}; i++)); do
    printf -v char_code '%d' "'${distribution_url:i:1}"
    hash=$(((hash * 31 + char_code) % 4294967296))
  done
  printf -v cache_key '%x' "$hash"
  cached_maven="${MAVEN_USER_HOME:-$HOME/.m2}/wrapper/dists/$distribution_name/$cache_key/bin/mvn"
  [[ -x "$cached_maven" ]] || {
    echo "Offline setup requires the pinned Maven distribution already cached. Run online setup first." >&2; exit 1;
  }
  bash ./mvnw -B -o package
else
  bash ./mvnw -B dependency:go-offline
  bash ./mvnw -B package
fi
if ! java -Xmx1024m "-Djava.io.tmpdir=$native_tmp" "-Dorg.sqlite.tmpdir=$native_tmp" -cp target/benchmark.jar io.github.embeddedbench.db.DatabaseSmoke; then
  echo "Connectivity failed. Check the complete error above, Java version, OS/architecture support and native extraction permissions." >&2
  echo "For native loader failures set BENCHMARK_NATIVE_TMP to a user-owned writable executable filesystem; no elevated installation is performed." >&2
  exit 1
fi
