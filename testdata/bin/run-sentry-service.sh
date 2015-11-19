#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
set -u
. ${IMPALA_HOME}/bin/set-classpath.sh

SENTRY_SERVICE_CONFIG=${SENTRY_CONF_DIR}/sentry-site.xml

# First kill any running instances of the service.
$IMPALA_HOME/testdata/bin/kill-sentry-service.sh

# Start the service.
# HIVE_HOME must be unset due to SENTRY-430.
unset HIVE_HOME
${SENTRY_HOME}/bin/sentry --command service -c ${SENTRY_SERVICE_CONFIG} &

# Wait for the service to come online
"$JAVA" -cp $CLASSPATH com.cloudera.impala.testutil.SentryServicePinger \
    --config_file "${SENTRY_SERVICE_CONFIG}" -n 30 -s 2
