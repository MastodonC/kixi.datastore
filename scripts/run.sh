#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

JAR_LOCATION=${1:-$JAR_LOCATION}
CONFIG_PROFILE=${2:-$CONFIG_PROFILE}

exec java ${JAVA_OPTS:-} -cp $JAR_LOCATION kixi.datastore.bootstrap $CONFIG_PROFILE >&1

