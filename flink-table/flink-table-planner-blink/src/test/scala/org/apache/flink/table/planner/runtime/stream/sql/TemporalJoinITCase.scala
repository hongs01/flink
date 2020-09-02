/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.sql

import java.lang.{Long => JLong}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase

import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class TemporalJoinITCase(state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  @Before
  def prepare(): Unit = {
    // for Processing-Time temporal table join
    val orderData1 = List(
      TestValuesTableFactory.changelogRow("+I", toJLong(1), "Euro", "no1", toJLong(12)),
      TestValuesTableFactory.changelogRow("+I", toJLong(2), "US Dollar", "no1", toJLong(14)),
      TestValuesTableFactory.changelogRow("+I", toJLong(3), "US Dollar", "no2", toJLong(18)),
      TestValuesTableFactory.changelogRow("+I", toJLong(4), "RMB", "no1", toJLong(40)),
      TestValuesTableFactory.changelogRow("+U", toJLong(4), "RMB", "no1", toJLong(40)))
    val dataId1 = TestValuesTableFactory.registerData(orderData1)

    val currencyData1 = List(
      TestValuesTableFactory.changelogRow("+I", "Euro", "no1", toJLong(114)),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", "no1", toJLong(102)),
      TestValuesTableFactory.changelogRow("+I", "Yen", "no1", toJLong(1)),
      TestValuesTableFactory.changelogRow("+I", "RMB", "no1", toJLong(702)),
      TestValuesTableFactory.changelogRow("+I", "Euro", "no1", toJLong(118)),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", "no2", toJLong(106)))
    val dataId2 = TestValuesTableFactory.registerData(currencyData1)

    tEnv.executeSql(
      s"""
         |CREATE TABLE orders_proctime (
         |  order_id BIGINT,
         |  currency STRING,
         |  currency_no STRING,
         |  amount BIGINT,
         |  proctime as PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId1'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TABLE currency_proctime (
         |  currency STRING,
         |  currency_no STRING,
         |  rate  BIGINT,
         |  proctime as PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId2'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE VIEW versioned_currency_proc AS
         |SELECT
         |  currency,
         |  currency_no,
         |  rate,
         |  proctime FROM
         |      ( SELECT *, ROW_NUMBER() OVER (PARTITION BY currency, currency_no
         |        ORDER BY proctime DESC) AS rowNum
         |        FROM currency_proctime) T
         | WHERE rowNum = 1""".stripMargin)
  }

  /**
   * Because of nature of the processing time, we can not (or at least it is not that easy)
   * validate the result here. Instead of that, here we are just testing whether there are no
   * exceptions in a full blown ITCase. Actual correctness is tested in unit tests.
   */
  @Test
  def testProcessTimeTemoralJoin(): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    createSinkTable("proctime_sink1")
    val insertSql = "INSERT INTO proctime_sink1 " +
      "SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate,r.proctime " +
      " FROM orders_proctime AS o JOIN " +
      " currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"
    execInsertSqlAndWaitResult(insertSql)
  }

  @Test
  def testProcessTimeTemoralJoinWithView(): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    createSinkTable("proctime_sink2")
    val insertSql = "INSERT INTO proctime_sink2 " +
      "SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate,r.proctime " +
      " FROM orders_proctime AS o JOIN " +
      " versioned_currency_proc " +
      " FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"
    execInsertSqlAndWaitResult(insertSql)
  }

  @Test
  def testMultiTemporalJoin(): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    createSinkTable("proctime_sink3")
    val insertSql = "INSERT INTO proctime_sink3 " +
      "SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r1.proctime " +
      " FROM orders_proctime AS o JOIN " +
      " versioned_currency_proc " +
      " FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " JOIN currency_proctime " +
      " FOR SYSTEM_TIME AS OF o.proctime as r1" +
      " ON o.currency = r1.currency and o.currency_no = r1.currency_no"
    execInsertSqlAndWaitResult(insertSql)
  }

  private def createSinkTable(tableName: String): Unit = {
    // result table
    tEnv.executeSql(
      s"""
        |CREATE TABLE ${tableName} (
        |    order_id BIGINT,
        |    currency STRING,
        |    amount BIGINT,
        |    l_time TIMESTAMP(3),
        |    rate BIGINT,
        |    r_time TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)
  }

  private def toJLong(int: Int): JLong = {
    JLong.valueOf(int)
  }
}
