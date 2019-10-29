#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -Eeuo pipefail

SCALE="0.01"

source "$(dirname "$0")"/common.sh

################################################################################
# Generate test data
################################################################################
echo "Generating test data..."

TARGET_DIR="$END_TO_END_DIR/flink-tpcds-test/target"
TPCDS_DATA_DIR="$END_TO_END_DIR/test-scripts/test-data/tpcds"
java -cp "$TARGET_DIR/TpcdsTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpcds.TpcdsDataGenerator -scale "$SCALE" -path "$TARGET_DIR"

################################################################################
# Prepare Flink
################################################################################

echo "Preparing Flink..."

set_config_key "taskmanager.heap.size" "4096m"
set_config_key "taskmanager.numberOfTaskSlots" "4"
set_config_key "parallelism.default" "1"
set_config_key "table.exec.resource.external-buffer-memor" "256m"
set_config_key "table.exec.resource.hash-agg.memory" "256m"
set_config_key "table.exec.resource.hash-join.memory" "256m"
set_config_key "table.exec.resource.sort.memory" "256m"

start_cluster

################################################################################
# Run TPC-DS SQL
################################################################################

echo "Runing TPC-DS queries..."

TABLE_DIR="$TARGET_DIR/table"
QUERY_DIR="$TPCDS_DATA_DIR/queries"
EXPECTED_DIR="$TPCDS_DATA_DIR/expected"
RESULT_DIR="$TEST_DATA_DIR/result"

mkdir "$RESULT_DIR"

$FLINK_DIR/bin/flink run -c org.apache.flink.table.tpcds.TpcdsTestProgram "$TARGET_DIR/TpcdsTestProgram.jar" -sourceTablePath "$TABLE_DIR" -queryPath "$QUERY_DIR" -sinkTablePath "$RESULT_DIR"

function sql_cleanup() {
  stop_cluster
  $FLINK_DIR/bin/taskmanager.sh stop-all
}
#on_exit sql_cleanup

################################################################################
# Check result
################################################################################

# Check custom queries
for i in {1..99}
do
    java -cp "$TARGET_DIR/TpcdsTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpcds.TpcdsResultComparator -expectedPath "$EXPECTED_DIR/q${i}.result" -actualPath "$RESULT_DIR/q${i}.result"
done

# Check variant queries
for i in {14 23 24 39}
do
    java -cp "$TARGET_DIR/TpcdsTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpcds.TpcdsResultComparator -expectedPath "$EXPECTED_DIR/q${i}a.result" -actualPath "$RESULT_DIR/q${i}a.result"
done
