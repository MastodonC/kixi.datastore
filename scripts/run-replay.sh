#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

JAR_LOCATION=${1:-$JAR_LOCATION}
CONFIG_PROFILE=${2:-$ENVIRONMENT}
SANDBOX=${MESOS_SANDBOX:-"."}

exec java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$SANDBOX -XX:ErrorFile=$SANDBOX/hs_err_pid_%p.log -XX:+UseG1GC -Xloggc:$SANDBOX/gc_%p.log -XX:+PrintGCCause -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=2M -XX:+PrintGCDateStamps ${JAVA_OPTS:-} -cp $JAR_LOCATION kixi.datastore.bootstrap $CONFIG_PROFILE "replay-config.edn" >&1
