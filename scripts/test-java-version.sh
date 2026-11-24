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

test_dir="$(mktemp -d)"
trap 'rm -rf -- "$test_dir"' EXIT
mkdir -p "$test_dir/bin"
cat > "$test_dir/bin/java" <<'JAVA'
#!/usr/bin/env bash
printf 'openjdk version "25.0.5"\n' >&2
JAVA
chmod +x "$test_dir/bin/java"
if output="$(env -u JAVA_HOME PATH="$test_dir/bin:$PATH" MAVEN_USER_HOME="$test_dir/maven" bash scripts/setup.sh --offline 2>&1)"; then
  echo "Expected offline setup to require a cached Maven distribution" >&2; exit 1
fi
[[ "$output" == *"Offline setup requires the pinned Maven distribution"* ]] || {
  printf 'Setup rejected PATH Java before reaching Maven:\n%s\n' "$output" >&2; exit 1;
}
echo "Java version checks passed."
