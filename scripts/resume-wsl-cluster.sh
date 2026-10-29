#!/usr/bin/env bash
set -euo pipefail
PROJECT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
VERIFY="$PROJECT/verification"
exec > >(tee "$VERIFY/wsl-cluster-resume-$(date +%Y%m%d-%H%M%S).log") 2>&1
RUNTIME=$(cat "$VERIFY/wsl-runtime-path.txt")
case "$RUNTIME" in /tmp/credit-exp2.*) ;; *) echo 'Unexpected runtime path'; exit 1;; esac
export HADOOP_CONF_DIR="$RUNTIME/conf"
source "$HADOOP_CONF_DIR/hadoop-env.sh"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export YARN_LOG_DIR="$HADOOP_LOG_DIR" YARN_PID_DIR="$HADOOP_PID_DIR"
export HADOOP_MAPRED_HOME="$HADOOP_HOME" HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME" HADOOP_YARN_HOME="$HADOOP_HOME"
trap 'bash "$PROJECT/scripts/collect-wsl-diagnostics.sh" || true' EXIT
cp "$PROJECT/scripts/capacity-scheduler.xml" "$HADOOP_CONF_DIR/"
echo 'Starting YARN with an explicitly configured default queue.'
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
mapred --daemon start historyserver
for i in $(seq 1 60); do
  if yarn node -list 2>/dev/null | grep -q RUNNING; then break; fi
  sleep 2
done
hdfs dfsadmin -safemode wait
yarn node -list
hdfs dfs -mkdir -p /credit-input
TRAIN=/mnt/c/Users/75566/Downloads/train.csv
TEST=/mnt/c/Users/75566/Downloads/test.csv
if ! hdfs dfs -test -e /credit-input/train.csv; then hdfs dfs -put "$TRAIN" /credit-input/train.csv; fi
if ! hdfs dfs -test -e /credit-input/test.csv; then hdfs dfs -put "$TEST" /credit-input/test.csv; fi
RESULT=/credit-result-$(date +%Y%m%d-%H%M%S)
hadoop jar "$PROJECT/target/credit-mapreduce.jar" \
  -Dcredit.reducers=2 -Dmapreduce.input.fileinputformat.split.maxsize=4194304 \
  /credit-input/train.csv /credit-input/test.csv "$RESULT"
DEST="$PROJECT/cluster-output-$(date +%Y%m%d-%H%M%S)"
hdfs dfs -get "$RESULT" "$DEST"
python3 "$PROJECT/scripts/verify.py" "$TRAIN" "$TEST" "$DEST" "$VERIFY/cluster-validation.json"
curl --fail http://127.0.0.1:18088/ws/v1/cluster/apps > "$VERIFY/yarn-applications.json"
cp -r "$HADOOP_CONF_DIR" "$VERIFY/wsl-hadoop-conf"
echo 'CLUSTER RUN COMPLETE. Keep WSL running for screenshots.'
echo 'ResourceManager: http://localhost:18088/cluster/apps'
echo 'JobHistory: http://localhost:19888/jobhistory'
echo "Output: $DEST"
