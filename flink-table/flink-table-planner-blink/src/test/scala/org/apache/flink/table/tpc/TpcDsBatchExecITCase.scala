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

package org.apache.flink.table.tpc

import org.apache.flink.streaming.api.transformations.ShuffleMode
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.sources.{CsvTableSource, CsvTableSource2}

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.io.File
import java.util

import scala.io.Source
import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class TpcDsBatchExecITCase(
    caseName: String,
    factor: Int,
    isUncertain: Boolean)
    extends BatchTestBase {

  // original csv data is generated by command: dsdgen -VERBOSE Y  -SCALE 1  -RNGSEED 0 -FORCE
  val csvDataDir = "/tpcds/data"

  def getDataFile(tableName: String): String =
    "/Users/pingyong.xpy/workspace/ideaProject/csv_data/" + tableName + ".dat"

  def getResultFile: String = s"/tpcds/result/$factor/$caseName"

  @Before
  def prepareOp(): Unit = {
    //dynamic allocate memory.
    for ((tableName, schema) <- TpcDsSchemaProvider.schemaMap) {
      val builder = CsvTableSource2.builder()
          .path(getDataFile(tableName))
         // .fields(schema.getFieldNames, schema.getFieldTypes, schema.getFieldNullables)
          .fieldDelimiter("|")
          .lineDelimiter("\n")
       //   .enableEmptyColumnAsNull()
       //   .uniqueKeys(schema.getUniqueKeys)
      schema.getFieldNames.zip(schema.getFieldTypes).foreach {
        case (fieldName, fieldType) =>
          builder.field(fieldName, fieldType)
      }
      val tableSource = builder.build()
      tEnv.registerTableSource(tableName, tableSource)

    }
    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    tEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, false)
    conf.getConfiguration.setString(ExecutionConfigOptions.SQL_EXEC_SHUFFLE_MODE,
      ShuffleMode.BATCH.toString)
   // TpcUtils.disableBroadcastHashJoin(tEnv)
  }

  def execute(caseName: String): Unit = {
    val tmpFile = File.createTempFile("tpcds", ".dat")
    tmpFile.deleteOnExit()
    val query = parseQuery(TpcUtils.getTpcDsQuery(caseName, factor))
    executeQuery(query)
  }

  def validate(actualFile: File, caseName: String): Unit = {
    val expectedResource = getClass.getResource(getResultFile).getFile
    val expected = Source.fromFile(expectedResource).getLines().toArray
    val actual = Source.fromFile(actualFile).getLines().toArray
    BatchTestBase.compareResult[String](expected, actual, isUncertain)
  }

  @Test
  def test(): Unit = {
    execute(caseName)
  }
}

object TpcDsBatchExecITCase {

  @Parameterized.Parameters(name = "caseName={0}, factor={1}, isUncertain={1}")
  def parameters(): util.Collection[Array[Any]] = {
    val factor = 1
    Seq[Array[Any]](

      // FIXME: Array("q78", true) will block
      Array("q1", false), Array("q2", false), Array("q3", false), Array("q4", false),
      Array("q5", false), Array("q6", true), Array("q7", false), Array("q8", false),
      Array("q9", false), Array("q10", false), Array("q11", false), Array("q12", false),
      Array("q13", false), Array("q14a", false), Array("q14b", false), Array("q15", false),
      Array("q16", false), Array("q17", false), Array("q18", false), Array("q19", false),
      Array("q20", false), Array("q21", false), Array("q22", false), Array("q23a", false),
      Array("q23b", false), Array("q24a", true), Array("q24b", false), Array("q25", false),
      Array("q26", false), Array("q27", false), Array("q28", false), Array("q29", false),
      Array("q30", false), Array("q31", false), Array("q32", false), Array("q33", false),
      Array("q34", false), Array("q35", false), Array("q36", false), Array("q37", false),
      Array("q38", false), Array("q39a", false), Array("q39b", false), Array("q40", false),
      Array("q41", false), Array("q42", false), Array("q43", false), Array("q44", false),
      Array("q45", false), Array("q46", false), Array("q47", false), Array("q48", false),
      Array("q49", false), Array("q50", false), Array("q51", false), Array("q52", false),
      Array("q53", false), Array("q54", false), Array("q55", false), Array("q56", true),
      Array("q57", false), Array("q58", false), Array("q59", false), Array("q60", false),
      Array("q61", false), Array("q62", false), Array("q63", false), Array("q64", true),
      Array("q65", false), Array("q66", false), Array("q67", false), Array("q68", false),
      Array("q69", false), Array("q70", false), Array("q71", true), Array("q72", false),
      Array("q73", false), Array("q74", false), Array("q75", true), Array("q76", false),
      Array("q77", true), Array("q79", true), Array("q80", false),
      Array("q81", false), Array("q82", false), Array("q83", false), Array("q84", false),
      Array("q85", false), Array("q86", false), Array("q87", false), Array("q88", false),
      Array("q89", false), Array("q91", false), Array("q92", false), Array("q93", false),
      Array("q94", false), Array("q95", false), Array("q96", false), Array("q97", false),
      Array("q98", false), Array("q99", false)


    ).map(a => Array(a.head, factor, a.last))
  }
}

