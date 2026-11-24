#!/usr/bin/env bash

require_java_21() {
  command -v java >/dev/null || { echo "Install JDK 21 or later and set JAVA_HOME." >&2; return 1; }
  java_version="$(java -version 2>&1)" || return 1
  if [[ "$java_version" =~ version\ \"([0-9]+)([.\"+-]) ]] && (( 10#${BASH_REMATCH[1]} >= 21 )); then
    return 0
  fi
  printf 'JDK 21 or later is required, not the active runtime:\n%s\nSet JAVA_HOME to a JDK 21 or later installation.\n' "$java_version" >&2
  return 1
}

prepare_java_build() {
  if [[ -z "${JAVA_HOME:-}" ]]; then
    local settings
    settings="$(java -XshowSettings:properties -version 2>&1)" || {
      echo "Install JDK 21 or later and set JAVA_HOME." >&2; return 1;
    }
    JAVA_HOME="$(printf '%s\n' "$settings" | sed -n 's/^[[:space:]]*java.home = //p')"
  fi
  [[ -n "$JAVA_HOME" && -x "$JAVA_HOME/bin/java" && -x "$JAVA_HOME/bin/javac" ]] || {
    printf 'JAVA_HOME must point to a complete JDK 21 or later (with bin/java and bin/javac): %s\n' "$JAVA_HOME" >&2
    return 1
  }
  export JAVA_HOME
  export PATH="$JAVA_HOME/bin:$PATH"
  require_java_21 || return 1
  local compiler_version
  compiler_version="$(javac -version 2>&1)" || {
    echo "Cannot run the compiler in JAVA_HOME: $JAVA_HOME" >&2; return 1;
  }
  if [[ ! "$compiler_version" =~ ^javac\ ([0-9]+)([.\"+-]|$) ]] || (( 10#${BASH_REMATCH[1]} < 21 )); then
    printf 'JDK 21 or later is required, not the active compiler:\n%s\nSet JAVA_HOME to a complete JDK 21 or later installation.\n' "$compiler_version" >&2
    return 1
  fi
  # Maven startup files can replace JAVA_HOME after the checks above.
  export MAVEN_SKIP_RC=true
}
