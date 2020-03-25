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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchAbstractTestBase, BatchTestBase}

import org.junit._

class FileSystemITCase extends BatchTestBase {

  private val path = BatchAbstractTestBase.TEMPORARY_FOLDER.newFolder("testcsv").getPath

  @Before
  override def before(): Unit = {
    super.before()
    conf.setSqlDialect(SqlDialect.HIVE)
  }

  @Test
  def testCsv(): Unit = {
    tEnv.sqlUpdate(
      s"""
         |create table my_csv(
         |  i int,
         |  j bigint,
         |  a string,
         |  b bigint)
         |partitioned by (a, b)
         |with(
         |  'connector.type'='filesystem',
         |  'connector.path'='$path',
         |  'format.type'='testcsv'
         |)
         |""".stripMargin
    )
    tEnv.sqlUpdate("insert into my_csv select 1, 1, '1', 1")
    tEnv.execute("1")

    checkResult(
      "select * from my_csv",
      Seq(row(1, 1, 1, 1))
    )
  }
}
