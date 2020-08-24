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

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.types.{Row, RowKind}
import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable
import scala.collection.JavaConversions._
import java.lang.{Long => JLong}
import java.time.LocalDateTime

@RunWith(classOf[Parameterized])
class TemporalJoinITCase(state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  @Before
  def prepare(): Unit = {
    // for Processing-Time temporal table join
    val orderData1 = List(
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(1L), "Euro", JLong.valueOf(12)),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(2L), "US Dollar", JLong.valueOf(14)),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(3L), "US Dollar", JLong.valueOf(14)),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(4L), "US Dollar", JLong.valueOf(18)),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(5L), "RMB", JLong.valueOf(40)),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(5L), "RMB", JLong.valueOf(40)))
    val dataId1 = TestValuesTableFactory.registerData(orderData1)

    val currencyData1 = List(
      TestValuesTableFactory.changelogRow("+I", "Euro", JLong.valueOf(114L)),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", JLong.valueOf(102L)),
      TestValuesTableFactory.changelogRow("+I", "Yen", JLong.valueOf(1L)),
      TestValuesTableFactory.changelogRow("+I", "RMB", JLong.valueOf(702L)),
      TestValuesTableFactory.changelogRow("+I", "Euro",  JLong.valueOf(118L)),
      TestValuesTableFactory.changelogRow("+I", "US Dollar",  JLong.valueOf(106L)))
    val dataId2 = TestValuesTableFactory.registerData(currencyData1)

    tEnv.executeSql(
      s"""
         |CREATE TABLE orders_proctime (
         |  order_id BIGINT,
         |  currency STRING,
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
         |  rate,
         |  proctime FROM
         |      (SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY proctime DESC) AS rowNum
         |        FROM currency_proctime)
         | WHERE rowNum = 1""".stripMargin)


    // for Event-Time temporal table join
    val orderData2 = List(
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(1L), "Euro", JLong.valueOf(12L),
        LocalDateTime.parse("2020-08-16T00:01:01")),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(2L), "US Dollar", JLong.valueOf(1L),
        LocalDateTime.parse("2020-08-16T00:01:08")),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(3L), "US Dollar", JLong.valueOf(14L),
        LocalDateTime.parse("2020-08-16T00:01:12")),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(4L), "US Dollar", JLong.valueOf(18L),
        LocalDateTime.parse("2020-08-16T00:01:14")),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(5L), "RMB", JLong.valueOf(40L),
        LocalDateTime.parse("2020-08-16T00:02:01")),
      TestValuesTableFactory.changelogRow("+I", JLong.valueOf(5L), "RMB", JLong.valueOf(40L),
        LocalDateTime.parse("2020-08-16T00:02:30")))
    val dataId3 = TestValuesTableFactory.registerData(orderData2)

    val currencyData2 = List(
      TestValuesTableFactory.changelogRow("+I", "Euro", JLong.valueOf(114L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", JLong.valueOf(102L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("+I", "Yen", JLong.valueOf(1L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("+I", "RMB", JLong.valueOf(702L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("-U", "Euro", JLong.valueOf(114L),
        LocalDateTime.parse("2020-08-16T00:01:12")),
      TestValuesTableFactory.changelogRow("+U", "Euro",  JLong.valueOf(118L),
        LocalDateTime.parse("2020-08-16T00:01:12")),
      TestValuesTableFactory.changelogRow("-U", "US Dollar", JLong.valueOf(102L),
        LocalDateTime.parse("2020-08-16T00:01:12")),
      TestValuesTableFactory.changelogRow("+U", "US Dollar",  JLong.valueOf(106L),
        LocalDateTime.parse("2020-08-16T00:01:12")),
      TestValuesTableFactory.changelogRow("-D","RMB", JLong.valueOf(708L),
        LocalDateTime.parse("2020-08-16T00:02:05")))
    val dataId4 = TestValuesTableFactory.registerData(currencyData2)

    tEnv.executeSql(
      s"""
         |CREATE TABLE orders_rowtime (
         |  order_id BIGINT,
         |  currency STRING,
         |  amount BIGINT,
         |  order_time TIMESTAMP(3),
         |  WATERMARK FOR order_time AS order_time - interval '10' SECOND
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId3'
         |)
         |""".stripMargin)


    tEnv.executeSql(
      s"""
         |CREATE TABLE versioned_currency (
         |  currency STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
         |  PRIMARY KEY(currency) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId4'
         |)
         |""".stripMargin)


    val currencyData3 = List(
      TestValuesTableFactory.changelogRow("+I", "Euro", JLong.valueOf(114L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("+I", "US Dollar", JLong.valueOf(102L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("+I", "Yen", JLong.valueOf(1L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("+I", "RMB", JLong.valueOf(702L),
        LocalDateTime.parse("2020-08-15T08:00:00")),
      TestValuesTableFactory.changelogRow("+I", "Euro",  JLong.valueOf(118L),
        LocalDateTime.parse("2020-08-16T00:01:12")),
      TestValuesTableFactory.changelogRow("+I", "US Dollar",  JLong.valueOf(106L),
        LocalDateTime.parse("2020-08-16T00:01:12")))
    val dataId5 = TestValuesTableFactory.registerData(currencyData3)
    // append-only table
    tEnv.executeSql(
      s"""
         |CREATE TABLE currency_history (
         |  currency STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId5',
         |  'changelog-mode' = 'I')
         |  """.stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE VIEW versioned_currency_view AS
         |SELECT
         |  currency,
         |  rate,
         |  currency_time FROM
         |      (SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY currency_time DESC) AS rowNum
         |        FROM currency_history)
         | WHERE rowNum = 1""".stripMargin)

    // result table
    tEnv.executeSql(
      """
        |CREATE TABLE proctime_sink (
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

    tEnv.executeSql(
      """
        |CREATE TABLE rowtime_sink (
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

  /**
   * Because of nature of the processing time, we can not (or at least it is not that easy)
   * validate the result here. Instead of that, here we are just testing whether there are no
   * exceptions in a full blown ITCase. Actual correctness is tested in unit tests.
   */
  @Test
  def testProcessTimeTemoralJoin(): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val insertSql = "INSERT INTO proctime_sink " +
      "SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate,r.proctime " +
      " FROM orders_proctime AS o JOIN " +
      " versioned_currency_proc " +
      " FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency"
    execInsertSqlAndWaitResult(insertSql)
  }

  @Test
  def testEventTimeInnerJoin(): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val insertSql = "INSERT INTO rowtime_sink " +
      "SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate," +
      " cast(r.currency_time as TIMESTAMP(3)) " +
      " FROM orders_rowtime AS o JOIN " +
      " versioned_currency " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    execInsertSqlAndWaitResult(insertSql)
    val rawResult = TestValuesTableFactory.getRawResults("rowtime_sink")
    val expected = List(
     "+I(1,Euro,12,2020-08-16T00:01:01,114,2020-08-15T08:00)",
      "+I(2,US Dollar,1,2020-08-16T00:01:08,102,2020-08-15T08:00)",
      "+I(3,US Dollar,14,2020-08-16T00:01:12,106,2020-08-16T00:01:12)",
      "+I(4,US Dollar,18,2020-08-16T00:01:14,106,2020-08-16T00:01:12)",
      "+I(5,RMB,40,2020-08-16T00:02:01,702,2020-08-15T08:00)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeViewInnerJoin(): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val insertSql = "INSERT INTO rowtime_sink " +
      "SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate," +
      " cast(r.currency_time as TIMESTAMP(3)) " +
      " FROM orders_rowtime AS o JOIN " +
      " versioned_currency_view " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    execInsertSqlAndWaitResult(insertSql)
    val rawResult = TestValuesTableFactory.getRawResults("rowtime_sink")
    val expected = List(
      "+I(3,US Dollar,14,2020-08-16T00:01:12,106,2020-08-16T00:01:12)",
      "+I(4,US Dollar,18,2020-08-16T00:01:14,106,2020-08-16T00:01:12)",
      "+I(5,RMB,40,2020-08-16T00:02:01,702,2020-08-15T08:00)",
      "+I(5,RMB,40,2020-08-16T00:02:30,702,2020-08-15T08:00)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testNestedTemporalJoin(): Unit = {}
}

