#!/usr/bin/env bash

require_java_25() {
  command -v java >/dev/null || { echo "Install JDK 25 or later and set JAVA_HOME." >&2; return 1; }
  java_version="$(java -version 2>&1)" || return 1
  if [[ "$java_version" =~ version\ \"([0-9]+)([.\"+-]) ]] && (( 10#${BASH_REMATCH[1]} >= 25 )); then
    return 0
  fi
  printf 'JDK 25 or later is required, not the active runtime:\n%s\nSet JAVA_HOME to a JDK 25 or later installation.\n' "$java_version" >&2
  return 1
}
