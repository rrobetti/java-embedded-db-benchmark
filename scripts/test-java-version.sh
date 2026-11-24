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

unset -f java
test_dir="$(mktemp -d)"
trap 'rm -rf -- "$test_dir"' EXIT
export TEST_JAVA_HOME="$test_dir/jdk with spaces"
mkdir -p "$TEST_JAVA_HOME/bin"
cat > "$TEST_JAVA_HOME/bin/java" <<'JAVA'
#!/usr/bin/env bash
if [[ "${1:-}" == -XshowSettings:properties ]]; then
  printf '    java.home = %s\n' "$TEST_JAVA_HOME" >&2
fi
printf 'openjdk version "21.0.5"\n' >&2
JAVA
cat > "$TEST_JAVA_HOME/bin/javac" <<'JAVAC'
#!/usr/bin/env bash
printf 'javac %s\n' "${TEST_COMPILER_VERSION:-21.0.5}"
exit "${TEST_COMPILER_EXIT:-0}"
JAVAC
chmod +x "$TEST_JAVA_HOME/bin/java" "$TEST_JAVA_HOME/bin/javac"

(
  unset JAVA_HOME MAVEN_SKIP_RC
  export PATH="$TEST_JAVA_HOME/bin:$PATH"
  prepare_java_build
  [[ "$JAVA_HOME" == "$TEST_JAVA_HOME" && "$MAVEN_SKIP_RC" == true ]]
  [[ "${PATH%%:*}" == "$JAVA_HOME/bin" ]]
)
for test_version in 21 21.0.5 25.0.5.1 25-ea 27+35; do
  (export JAVA_HOME="$TEST_JAVA_HOME" TEST_COMPILER_VERSION="$test_version"; prepare_java_build)
done
for test_version in 17.0.12 20 210invalid invalid; do
  if (export JAVA_HOME="$TEST_JAVA_HOME" TEST_COMPILER_VERSION="$test_version"; prepare_java_build) 2>/dev/null; then
    echo "Expected compiler $test_version to be rejected" >&2; exit 1
  fi
done
if (export JAVA_HOME="$TEST_JAVA_HOME" TEST_COMPILER_EXIT=1; prepare_java_build) 2>/dev/null; then
  echo "Expected a failing compiler to be rejected" >&2; exit 1
fi
if (export JAVA_HOME="$test_dir/missing"; prepare_java_build) 2>/dev/null; then
  echo "Expected invalid JAVA_HOME to be rejected instead of using PATH" >&2; exit 1
fi
rm "$TEST_JAVA_HOME/bin/javac"
if (export JAVA_HOME="$TEST_JAVA_HOME"; prepare_java_build) 2>/dev/null; then
  echo "Expected a runtime without javac to be rejected" >&2; exit 1
fi
echo "Java version checks passed."
