#!/usr/bin/env bash
set -eu
PROJECT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
RUNTIME=$(cat "$PROJECT/verification/wsl-runtime-path.txt")
case "$RUNTIME" in /tmp/credit-exp2.*) ;; *) echo 'Unexpected runtime path'; exit 1;; esac
mkdir -p "$PROJECT/verification/wsl-diagnostics"
cp -r "$RUNTIME/logs" "$PROJECT/verification/wsl-diagnostics/"
cp -r "$RUNTIME/conf" "$PROJECT/verification/wsl-diagnostics/"
jps -lv > "$PROJECT/verification/wsl-diagnostics/processes.txt"
echo 'Diagnostic logs copied. Return to the chat.'
