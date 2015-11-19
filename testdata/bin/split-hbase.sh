#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

. ${IMPALA_HOME}/bin/impala-config.sh
if ${CLUSTER_DIR}/admin is_kerberized; then
  KERB_ARGS="--use_kerberos"
fi

# Split hbasealltypesagg and hbasealltypessmall and assign their splits
cd $IMPALA_HOME/testdata
mvn clean
# quietly resolve dependencies to avoid log spew in jenkins runs
if [ "${USER}" == "jenkins" ]; then
  echo "Quietly resolving testdata dependencies."
  mvn -q dependency:resolve
fi
mvn package
mvn -q dependency:copy-dependencies

. ${IMPALA_HOME}/bin/set-classpath.sh
export CLASSPATH=$IMPALA_HOME/testdata/target/impala-testdata-0.1-SNAPSHOT.jar:$CLASSPATH

RESULT=1
RETRY_COUNT=0
while [ $RESULT -ne 0 ] && [ $RETRY_COUNT -le 10 ]; do
  "$JAVA" ${JAVA_KERBEROS_MAGIC} \
     com.cloudera.impala.datagenerator.HBaseTestDataRegionAssigment \
     functional_hbase.alltypesagg functional_hbase.alltypessmall
  RESULT=$?

  if [ $RESULT -ne 0 ]; then
    ((RETRY_COUNT++))
    # If the split failed, force reload the hbase tables before trying the next split
    $IMPALA_HOME/bin/start-impala-cluster.py
    $IMPALA_HOME/bin/load-data.py -w functional-query \
        --table_names=alltypesagg,alltypessmall --table_formats=hbase/none --force \
        ${KERB_ARGS} --principal=${MINIKDC_PRINC_HIVE}
    $IMPALA_HOME/tests/util/compute_table_stats.py --db_names=functional_hbase \
        --table_names=alltypesagg,alltypessmall ${KERB_ARGS}
  fi
done

exit $RESULT
