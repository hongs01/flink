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
import java.time.LocalDateTime

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase

import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class TemporalJoinITCase(state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  @Before
  def prepare(): Unit = {

    env.setParallelism(4)

    // for Processing-Time temporal table join
    val orderData1 = List(
      TestValuesTableFactory.changelogRow("+I", toLong(1), "Euro", "no1", toLong(12)),
      TestValuesTableFactory.changelogRow("+I", toLong(2), "US Dollar", "no1", toLong(14)),
      TestValuesTableFactory.changelogRow("+I", toLong(3), "US Dollar", "no2", toLong(18)),
      TestValuesTableFactory.changelogRow("+I", toLong(4), "RMB", "no1", toLong(40)),
      TestValuesTableFactory.changelogRow("+U", toLong(4), "RMB", "no1", toLong(40)))
    val dataId1 = TestValuesTableFactory.registerData(orderData1)

    val currencyData1 = List(
      TestValuesTableFactory.changelogRow("+I", "Euro", "no1", toLong(114)),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", "no1", toLong(102)),
      TestValuesTableFactory.changelogRow("+I", "Yen", "no1", toLong(1)),
      TestValuesTableFactory.changelogRow("+I", "RMB", "no1", toLong(702)),
      TestValuesTableFactory.changelogRow("+I", "Euro", "no1", toLong(118)),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", "no2", toLong(106)))
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
         |    ( SELECT *, ROW_NUMBER() OVER (PARTITION BY currency, currency_no
         |      ORDER BY proctime DESC) AS rowNum
         |       FROM currency_proctime) T
         | WHERE rowNum = 1""".stripMargin)

    // for Event-Time temporal table join
    val orderData2 = List(
      TestValuesTableFactory.changelogRow("+I", toLong(1), "Euro", "no1", toLong(12),
        toDateTime("2020-08-15T00:01:00")),
      TestValuesTableFactory.changelogRow("+I", toLong(2), "US Dollar", "no1", toLong(1),
        toDateTime("2020-08-15T00:02:00")),
      TestValuesTableFactory.changelogRow("+I", toLong(3), "RMB", "no1", toLong(40),
        toDateTime("2020-08-15T00:03:00")),
      TestValuesTableFactory.changelogRow("+I", toLong(4), "Euro", "no1", toLong(14),
        toDateTime("2020-08-16T00:02:00")),
      TestValuesTableFactory.changelogRow("+U", toLong(5), "US Dollar", "no1", toLong(18),
        toDateTime("2020-08-16T00:03:00")),
      TestValuesTableFactory.changelogRow("+I", toLong(6), "RMB", "no1", toLong(40),
        toDateTime("2020-08-16T00:03:00")),
      TestValuesTableFactory.changelogRow("-D", toLong(6), "RMB", "no1", toLong(40),
        toDateTime("2020-08-16T00:03:00")))
    val dataId3 = TestValuesTableFactory.registerData(orderData2)

    val currencyData2 = List(
      TestValuesTableFactory.changelogRow("+I", "Euro", "no1", toLong(114),
        toDateTime("2020-08-15T00:00:00")),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", "no1", toLong(102),
        toDateTime("2020-08-15T00:00:00")),
      TestValuesTableFactory.changelogRow("+I", "Yen", "no1", toLong(1),
        toDateTime("2020-08-15T00:00:00")),
      TestValuesTableFactory.changelogRow("+I", "RMB", "no1", toLong(702),
        toDateTime("2020-08-15T00:00:00")),
      TestValuesTableFactory.changelogRow("-U", "Euro", "no1", toLong(114),
        toDateTime("2020-08-16T00:01:00")),
      TestValuesTableFactory.changelogRow("+U", "Euro",  "no1", toLong(118),
        toDateTime("2020-08-16T00:01:00")),
      TestValuesTableFactory.changelogRow("-U", "US Dollar", "no1", toLong(102),
        toDateTime("2020-08-16T00:02:00")),
      TestValuesTableFactory.changelogRow("+U", "US Dollar",  "no1", toLong(106),
        toDateTime("2020-08-16T00:02:00")),
      TestValuesTableFactory.changelogRow("-D","RMB", "no1", toLong(708),
        toDateTime("2020-08-16T00:02:00")))
    val dataId4 = TestValuesTableFactory.registerData(currencyData2)

    tEnv.executeSql(
      s"""
         |CREATE TABLE orders_rowtime (
         |  order_id BIGINT,
         |  currency STRING,
         |  currency_no STRING,
         |  amount BIGINT,
         |  order_time TIMESTAMP(3),
         |  WATERMARK FOR order_time AS order_time
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId3'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TABLE versioned_currency_with_single_key (
         |  currency STRING,
         |  currency_no STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
         |  PRIMARY KEY(currency) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId4'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TABLE versioned_currency_with_multi_key (
         |  currency STRING,
         |  currency_no STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
         |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId4'
         |)
         |""".stripMargin)
  }

  /**
   * Because of nature of the processing time, we can not (or at least it is not that easy)
   * validate the result here. Instead of that, here we are just testing whether there are no
   * exceptions in a full blown ITCase. Actual correctness is tested in unit tests.
   */
  @Test
  def testProcessTimeTemoralJoin(): Unit = {
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
  def testProcessTimeMultiTemporalJoin(): Unit = {
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

  @Test
  def testEventTimeTemporalJoinWithSingleKey(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    createSinkTable("rowtime_sink1")
    val insertSql = "INSERT INTO rowtime_sink1 " +
      "SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN " +
      " versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    execInsertSqlAndWaitResult(insertSql)
    val rawResult = TestValuesTableFactory.getRawResults("rowtime_sink1")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00)",
      "+I(4,Euro,14,2020-08-16T00:02,118,2020-08-16T00:01)",
      "+U(5,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeTemporalJoinWithMultiKeys(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    createSinkTable("rowtime_sink2")
    val insertSql = "INSERT INTO rowtime_sink2 " +
      "SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN " +
      " versioned_currency_with_multi_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"
    execInsertSqlAndWaitResult(insertSql)
    val rawResult = TestValuesTableFactory.getRawResults("rowtime_sink2")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00)",
      "+I(4,Euro,14,2020-08-16T00:02,118,2020-08-16T00:01)",
      "+U(5,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeTemporalJoinWithNonEqualCondition(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    createSinkTable("rowtime_sink2")
    val insertSql = "INSERT INTO rowtime_sink2 " +
      "SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN " +
      " versioned_currency_with_multi_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " and o.order_id < 5 and r.rate > 114"
    execInsertSqlAndWaitResult(insertSql)
    val rawResult = TestValuesTableFactory.getRawResults("rowtime_sink2")
    val expected = List(
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00)",
      "+I(4,Euro,14,2020-08-16T00:02,118,2020-08-16T00:01)")
    assertEquals(expected.sorted, rawResult.sorted)
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

  private def toLong(int: Int): JLong = {
    JLong.valueOf(int)
  }

  private def toDateTime(dateStr: String): LocalDateTime = {
    LocalDateTime.parse(dateStr)
  }
}
