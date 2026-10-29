#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ne 3 ]; then echo "Usage: bash scripts/run.sh TRAIN_CSV TEST_CSV NEW_OUTPUT_DIR" >&2; exit 2; fi
mvn -q package
hadoop jar target/credit-mapreduce.jar -Dcredit.reducers=2 "$1" "$2" "$3"
