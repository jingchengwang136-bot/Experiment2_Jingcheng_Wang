#!/usr/bin/env bash
# Isolated real HDFS + YARN cluster. Never formats or changes an existing cluster.
set -euo pipefail
PROJECT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
VERIFY="$PROJECT/verification"
mkdir -p "$VERIFY"
exec > >(tee "$VERIFY/wsl-cluster-run-$(date +%Y%m%d-%H%M%S).log") 2>&1
echo "Started $(date -Is)"
TRAIN=${1:-/mnt/c/Users/75566/Downloads/train.csv}
TEST=${2:-/mnt/c/Users/75566/Downloads/test.csv}
test -r "$TRAIN" && test -r "$TEST" && test -r "$PROJECT/target/credit-mapreduce.jar"
command -v java >/dev/null || { echo 'Java is required.'; exit 1; }
export JAVA_HOME=${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")}
java -version
for port in 19000 19870 18088 18032 18030 18031 18033 18042 19888 11020 17333; do
  if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
    echo "Port $port is already occupied; no services were changed. Choose free ports in this script."; exit 1
  fi
done
RUNTIME=$(mktemp -d /tmp/credit-exp2.XXXXXXXX)
printf '%s\n' "$RUNTIME" > "$VERIFY/wsl-runtime-path.txt"
echo "Isolated runtime: $RUNTIME"
HADOOP_BIN=$(command -v hadoop || true)
if [[ -n "$HADOOP_BIN" ]]; then
  export HADOOP_HOME=$(cd "$(dirname "$(readlink -f "$HADOOP_BIN")")/.." && pwd)
else
  for candidate in "${HADOOP_HOME:-/nonexistent}" "$HOME/hadoop-3.3.6" "$HOME/hadoop" /usr/local/hadoop /opt/hadoop /opt/hadoop-3.3.6; do
    if [[ -x "$candidate/bin/hadoop" ]]; then export HADOOP_HOME="$candidate"; break; fi
  done
fi
if [[ -z "${HADOOP_HOME:-}" || ! -x "$HADOOP_HOME/bin/hadoop" ]]; then
  command -v curl >/dev/null || { echo 'curl is required to download Hadoop.'; exit 1; }
  URL=https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
  ARCHIVE="$RUNTIME/hadoop.tar.gz"
  echo 'Downloading official Hadoop 3.3.6 archive (large download; allow several minutes)...'
  curl --fail --location --retry 3 "$URL" -o "$ARCHIVE"
  curl --fail --location --retry 3 "$URL.sha512" -o "$ARCHIVE.sha512"
  HASH=$(grep -Eo '[[:xdigit:]]{128}' "$ARCHIVE.sha512" | head -1)
  test ${#HASH} -eq 128
  printf '%s  %s\n' "$HASH" "$ARCHIVE" | sha512sum --check -
  tar -xzf "$ARCHIVE" -C "$RUNTIME"
  export HADOOP_HOME="$RUNTIME/hadoop-3.3.6"
fi
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export HADOOP_CONF_DIR="$RUNTIME/conf"
export HADOOP_LOG_DIR="$RUNTIME/logs"
export HADOOP_PID_DIR="$RUNTIME/pids"
export YARN_LOG_DIR="$HADOOP_LOG_DIR"
export YARN_PID_DIR="$HADOOP_PID_DIR"
export HADOOP_IDENT_STRING="creditexp2-$(basename "$RUNTIME")"
mkdir -p "$HADOOP_CONF_DIR" "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" "$RUNTIME/tmp"
cp "$HADOOP_HOME/etc/hadoop/log4j.properties" "$HADOOP_CONF_DIR/"
cp "$PROJECT/scripts/capacity-scheduler.xml" "$HADOOP_CONF_DIR/"
cat > "$HADOOP_CONF_DIR/hadoop-env.sh" <<EOF
export JAVA_HOME="$JAVA_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_LOG_DIR="$HADOOP_LOG_DIR"
export HADOOP_PID_DIR="$HADOOP_PID_DIR"
export HADOOP_IDENT_STRING="$HADOOP_IDENT_STRING"
EOF
cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<configuration>
 <property><name>fs.defaultFS</name><value>hdfs://127.0.0.1:19000</value></property>
 <property><name>hadoop.tmp.dir</name><value>$RUNTIME/tmp</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
 <property><name>dfs.replication</name><value>1</value></property>
 <property><name>dfs.namenode.name.dir</name><value>file://$RUNTIME/namenode</value></property>
 <property><name>dfs.datanode.data.dir</name><value>file://$RUNTIME/datanode</value></property>
 <property><name>dfs.namenode.http-address</name><value>127.0.0.1:19870</value></property>
 <property><name>dfs.datanode.address</name><value>127.0.0.1:19866</value></property>
 <property><name>dfs.datanode.http.address</name><value>127.0.0.1:19864</value></property>
 <property><name>dfs.datanode.ipc.address</name><value>127.0.0.1:19867</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<EOF
<configuration>
 <property><name>yarn.resourcemanager.hostname</name><value>127.0.0.1</value></property>
 <property><name>yarn.resourcemanager.address</name><value>127.0.0.1:18032</value></property>
 <property><name>yarn.resourcemanager.scheduler.address</name><value>127.0.0.1:18030</value></property>
 <property><name>yarn.resourcemanager.resource-tracker.address</name><value>127.0.0.1:18031</value></property>
 <property><name>yarn.resourcemanager.admin.address</name><value>127.0.0.1:18033</value></property>
 <property><name>yarn.resourcemanager.webapp.address</name><value>127.0.0.1:18088</value></property>
 <property><name>yarn.nodemanager.hostname</name><value>127.0.0.1</value></property>
 <property><name>yarn.nodemanager.webapp.address</name><value>127.0.0.1:18042</value></property>
 <property><name>yarn.nodemanager.local-dirs</name><value>$RUNTIME/nm-local</value></property>
 <property><name>yarn.nodemanager.log-dirs</name><value>$RUNTIME/nm-logs</value></property>
 <property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
 <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>2</value></property>
 <property><name>yarn.scheduler.minimum-allocation-mb</name><value>256</value></property>
 <property><name>yarn.scheduler.maximum-allocation-mb</name><value>4096</value></property>
 <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
 <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
 <property><name>mapreduce.shuffle.port</name><value>17333</value></property>
 <property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME,PATH,LANG,TZ</value></property>
</configuration>
EOF
cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<EOF
<configuration>
 <property><name>mapreduce.framework.name</name><value>yarn</value></property>
 <property><name>mapreduce.application.classpath</name><value>$HADOOP_HOME/share/hadoop/mapreduce/*,$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value></property>
 <property><name>mapreduce.jobhistory.address</name><value>127.0.0.1:11020</value></property>
 <property><name>mapreduce.jobhistory.webapp.address</name><value>127.0.0.1:19888</value></property>
 <property><name>yarn.app.mapreduce.am.resource.mb</name><value>1024</value></property>
 <property><name>yarn.app.mapreduce.am.command-opts</name><value>-Xmx768m</value></property>
 <property><name>mapreduce.map.memory.mb</name><value>512</value></property>
 <property><name>mapreduce.map.java.opts</name><value>-Xmx384m</value></property>
 <property><name>mapreduce.reduce.memory.mb</name><value>1024</value></property>
 <property><name>mapreduce.reduce.java.opts</name><value>-Xmx768m</value></property>
</configuration>
EOF
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME"
# Emit a shutdown script for only this isolated cluster, leaving files intact.
{
 echo '#!/usr/bin/env bash'
 for variable in JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR HADOOP_LOG_DIR HADOOP_PID_DIR YARN_LOG_DIR YARN_PID_DIR HADOOP_IDENT_STRING; do
   printf 'export %s=%q\n' "$variable" "${!variable}"
 done
 printf '"$HADOOP_HOME/bin/mapred" --daemon stop historyserver\n'
 printf '"$HADOOP_HOME/bin/yarn" --daemon stop nodemanager\n'
 printf '"$HADOOP_HOME/bin/yarn" --daemon stop resourcemanager\n'
 printf '"$HADOOP_HOME/bin/hdfs" --daemon stop datanode\n'
 printf '"$HADOOP_HOME/bin/hdfs" --daemon stop namenode\n'
} > "$VERIFY/stop-wsl-cluster.sh"
echo 'Formatting only the newly created, empty temporary NameNode directory.'
test ! -e "$RUNTIME/namenode"
hdfs namenode -format -nonInteractive
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
mapred --daemon start historyserver
echo 'Waiting for HDFS and YARN...'
for i in $(seq 1 90); do
  if hdfs dfs -mkdir -p /credit-input 2>/dev/null && yarn node -list 2>/dev/null | grep -q RUNNING; then break; fi
  sleep 2
done
hdfs dfsadmin -safemode wait
yarn node -list
hdfs dfs -put "$TRAIN" /credit-input/train.csv
hdfs dfs -put "$TEST" /credit-input/test.csv
hadoop jar "$PROJECT/target/credit-mapreduce.jar" \
  -Dcredit.reducers=2 \
  -Dmapreduce.input.fileinputformat.split.maxsize=4194304 \
  /credit-input/train.csv /credit-input/test.csv /credit-result
DEST="$PROJECT/cluster-output-$(date +%Y%m%d-%H%M%S)"
hdfs dfs -get /credit-result "$DEST"
python3 "$PROJECT/scripts/verify.py" "$TRAIN" "$TEST" "$DEST" "$VERIFY/cluster-validation.json"
curl --fail http://127.0.0.1:18088/ws/v1/cluster/apps > "$VERIFY/yarn-applications.json"
cp -r "$HADOOP_CONF_DIR" "$VERIFY/wsl-hadoop-conf"
echo
echo 'CLUSTER RUN COMPLETE. Keep this WSL session open for screenshots.'
echo 'ResourceManager: http://localhost:18088/cluster/apps'
echo 'JobHistory: http://localhost:19888/jobhistory'
echo "Output: $DEST"
echo "After screenshots, stop this isolated cluster with: bash '$VERIFY/stop-wsl-cluster.sh'"
