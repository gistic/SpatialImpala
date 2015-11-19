#!/usr/bin/env bash
# Copyright 2015 GISTIC.

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export IMPALA_HOME=$ROOT

LOG_DIR=${IMPALA_TEST_CLUSTER_LOG_DIR}/query_tests

# Exit on reference to uninitialized variable
set -u

# Exit on non-zero return value
set -e

# Always run in debug mode
set -x

$IMPALA_HOME/testdata/bin/run-all.sh

${IMPALA_HOME}/bin/start-impala-cluster.py --log_dir=${LOG_DIR} --cluster_size=3 \
    --catalogd_args=--load_catalog_in_background=false


