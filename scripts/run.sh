#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

JAR_LOCATION=${1:-$JAR_LOCATION}
CONFIG_PROFILE=${2:-$CONFIG_PROFILE}
SANDBOX=${MESOS_SANDBOX:-"."}

exec java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$SANDBOX -XX:+UseG1GC -Xloggc:$SANDBOX/gc.log -XX:+PrintGCCause -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 -XX:GCLogFileSize=2M ${JAVA_OPTS:-} -cp $JAR_LOCATION kixi.datastore.bootstrap $CONFIG_PROFILE >&1

