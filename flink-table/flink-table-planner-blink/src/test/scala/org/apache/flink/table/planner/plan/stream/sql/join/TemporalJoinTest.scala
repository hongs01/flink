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
package org.apache.flink.table.planner.plan.stream.sql.join

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class TemporalJoinTest extends TableTestBase {

  val util: StreamTableTestUtil = streamTestUtil()

  util.addTable(
    """
      |CREATE TABLE Orders (
      | o_amount INT,
      | o_currency STRING,
      | o_rowtime TIMESTAMP(3),
      | o_proctime as PROCTIME(),
      | WATERMARK FOR o_rowtime AS o_rowtime
      |) WITH (
      | 'connector' = 'COLLECTION',
      | 'is-bounded' = 'false'
      |)
      """.stripMargin)
  util.addTable(
    """
      |CREATE TABLE RatesHistory (
      | currency STRING,
      | rate INT,
      | rowtime TIMESTAMP(3),
      | WATERMARK FOR rowtime AS rowtime
      |) WITH (
      | 'connector' = 'COLLECTION',
      | 'is-bounded' = 'false'
      |)
      """.stripMargin)

  util.addTable(
    """
      |CREATE TABLE RatesHistoryWithPK (
      | currency STRING,
      | rate INT,
      | rowtime TIMESTAMP(3),
      | WATERMARK FOR rowtime AS rowtime,
      | PRIMARY KEY(currency) NOT ENFORCED
      |) WITH (
      | 'connector' = 'COLLECTION',
      | 'is-bounded' = 'false'
      |)
      """.stripMargin)

  util.addTable(
    """
      |CREATE TABLE RatesOnly (
      | currency STRING,
      | rate INT,
      | proctime AS PROCTIME()
      |) WITH (
      | 'connector' = 'COLLECTION',
      | 'is-bounded' = 'false'
      |)
      """.stripMargin)

  util.addTable(
    " CREATE VIEW DeduplicatedView as SELECT currency, rate, rowtime FROM " +
      "  (SELECT *, " +
      "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rowtime DESC) AS rowNum " +
      "   FROM RatesHistory" +
      "  )" +
      "  WHERE rowNum = 1")

  util.addTable(
    " CREATE VIEW latestView as SELECT currency, rate, proctime FROM " +
      "  (SELECT *, " +
      "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY proctime DESC) AS rowNum " +
      "   FROM RatesOnly" +
      "  )" +
      "  WHERE rowNum = 1")

  util.addTable("CREATE VIEW latest_rates AS SELECT currency, LAST_VALUE(rate) AS rate " +
    "FROM RatesHistory " +
    "GROUP BY currency ")

  @Test
  def testSimpleJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.o_rowtime as r " +
      "on o.o_currency = r.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimpleRowtimeVersionedViewJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.o_rowtime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimpleProctimeVersionedViewJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "latestView " +
      "FOR SYSTEM_TIME AS OF o.o_proctime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimpleViewRowTimeJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "latest_rates " +
      "FOR SYSTEM_TIME AS OF o.o_rowtime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimpleViewProcTimeJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "latest_rates " +
      "FOR SYSTEM_TIME AS OF o.o_proctime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyPlan(sqlQuery)
  }
//
//  @Test
//  def testSimpleProctimeJoin(): Unit = {
//    val sqlQuery = "SELECT " +
//      "o_amount * rate as rate " +
//      "FROM ProctimeOrders AS o, " +
//      "LATERAL TABLE (ProctimeRates(o.o_proctime)) AS r " +
//      "WHERE currency = o_currency"
//
//    util.verifyPlan(sqlQuery)
//  }
//
//  @Test
//  def testJoinOnQueryLeft(): Unit = {
//    val orders = util.tableEnv.sqlQuery("SELECT * FROM Orders WHERE o_amount > 1000")
//    util.tableEnv.createTemporaryView("Orders2", orders)
//
//    val sqlQuery = "SELECT " +
//      "o_amount * rate as rate " +
//      "FROM Orders2 AS o, " +
//      "LATERAL TABLE (Rates(o.o_rowtime)) AS r " +
//      "WHERE currency = o_currency"
//
//    util.verifyPlan(sqlQuery)
//  }
//
//  /**
//    * Test versioned joins with more complicated query.
//    * Important thing here is that we have complex OR join condition
//    * and there are some columns that are not being used (are being pruned).
//    */
//  @Test
//  def testComplexJoin(): Unit = {
//    val util = streamTestUtil()
//    util.addDataStream[(String, Int)]("Table3", 't3_comment, 't3_secondary_key)
//    util.addDataStream[(Timestamp, String, Long, String, Int)](
//      "Orders", 'o_rowtime.rowtime, 'o_comment, 'o_amount, 'o_currency, 'o_secondary_key)
//
//    util.addDataStream[(Timestamp, String, String, Int, Int)](
//      "RatesHistory", 'rowtime.rowtime, 'comment, 'currency, 'rate, 'secondary_key)
//    val rates = util.tableEnv
//      .sqlQuery("SELECT * FROM RatesHistory WHERE rate > 110")
//      .createTemporalTableFunction($"rowtime", $"currency")
//    util.addTemporarySystemFunction("Rates", rates)
//
//    val sqlQuery =
//      "SELECT * FROM " +
//        "(SELECT " +
//        "o_amount * rate as rate, " +
//        "secondary_key as secondary_key " +
//        "FROM Orders AS o, " +
//        "LATERAL TABLE (Rates(o_rowtime)) AS r " +
//        "WHERE currency = o_currency OR secondary_key = o_secondary_key), " +
//        "Table3 " +
//        "WHERE t3_secondary_key = secondary_key"
//
//    util.verifyPlan(sqlQuery)
//  }
}
