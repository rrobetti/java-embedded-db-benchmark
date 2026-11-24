#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/java-version.sh

java() { printf 'openjdk version "%s"\n' "$test_version" >&2; }

for test_version in 21 21.0.5 25.0.5.1 25-ea 27+35; do
  require_java_21 || { echo "Expected $test_version to be accepted" >&2; exit 1; }
done
for test_version in 1.8.0 17.0.12 20 210invalid invalid; do
  if require_java_21 2>/dev/null; then
    echo "Expected $test_version to be rejected" >&2; exit 1
  fi
done
echo "Java version checks passed."
