#!/usr/bin/env bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/home/u5566/hadoop-3.3.6
export HADOOP_CONF_DIR=/tmp/credit-exp2.ZFeJ4n6K/conf
export HADOOP_LOG_DIR=/tmp/credit-exp2.ZFeJ4n6K/logs
export HADOOP_PID_DIR=/tmp/credit-exp2.ZFeJ4n6K/pids
export YARN_LOG_DIR=/tmp/credit-exp2.ZFeJ4n6K/logs
export YARN_PID_DIR=/tmp/credit-exp2.ZFeJ4n6K/pids
export HADOOP_IDENT_STRING=creditexp2-credit-exp2.ZFeJ4n6K
"$HADOOP_HOME/bin/mapred" --daemon stop historyserver
"$HADOOP_HOME/bin/yarn" --daemon stop nodemanager
"$HADOOP_HOME/bin/yarn" --daemon stop resourcemanager
"$HADOOP_HOME/bin/hdfs" --daemon stop datanode
"$HADOOP_HOME/bin/hdfs" --daemon stop namenode
