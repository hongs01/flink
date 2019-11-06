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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext,
  FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.{FilterJoinRule, FilterMultiJoinMergeRule,
  FlinkFilterFirstJoinReorderRule, JoinToMultiJoinRule, ProjectMultiJoinMergeRule}
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}


class FlinkFilterFirstJoinReorderRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "filter_on_join",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FilterJoinRule.FILTER_ON_JOIN))
        .build())
    programs.addLast(
      "prepare_join_reorder",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          JoinToMultiJoinRule.INSTANCE,
          ProjectMultiJoinMergeRule.INSTANCE,
          FilterMultiJoinMergeRule.INSTANCE))
        .build())
    programs.addLast(
      "join_reorder",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FlinkFilterFirstJoinReorderRule.INSTANCE))
        .build())
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Int, String)]("T1", 'a1, 'b1, 'c1)
    util.addTableSource[(Int, Int, String)]("T2", 'a2, 'b2, 'c2)
    util.addTableSource[(Int, Int, String)]("T3", 'a3, 'b3, 'c3)
    util.addTableSource[(Int, Int, String)]("T4", 'a4, 'b4, 'c4)
    util.addTableSource[(Int, Int, String)]("T5", 'a5, 'b5, 'c5)
  }

  @Test
  def testSimpleInnerJoin(): Unit = {
    val query =
      """
        |select * from T1, T2 where a1 = a2
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithoutFilter(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a1 = a3 and a3 = a4
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithOnlyOneFilter(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a1 = a3 and a3 = a4 and b3 > 10
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithSameSelectivity1(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a1 = a3 and a3 = a4 and b3 > 10 and b2 < 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithSameSelectivity2(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a2 = a3 and a3 = a4 and b4 > 10 and b1 < 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithSameSelectivity3(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a1 = a3 and a3 = a4 and b3 > 10 and b1 < 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithSameSelectivity4(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a1 = a3 and a1 = a4 and b4 > 10 and b3 < 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithSameSelectivity5(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4, T5 where
        |   b1 = b2 and a2 = a3 and c3 = c5 and b4 > 10 and b3 < 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithDiffSelectivity1(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a1 = a3 and a3 = a4 and b1 > 10 and b4 = 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithDiffSelectivity2(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4 where a1 = a2 and a1 = a3 and a1 = a4 and b3 > 10 and b4 = 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithTwoFilterWithDiffSelectivity3(): Unit = {
    val query =
      """
        |select * from T1, T2, T3, T4, T5 where
        |    a1 = a3 and a1 = a4 and a2 = a5 and c2 = c1 and b3 > 10 and b4 = 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testOnlyLeftJoin(): Unit = {
    val query =
      """
        |select * from T1 left join T2 on a1 = a2
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinWithoutFilter(): Unit = {
    val query =
      """
        |select * from T1
        |    join T2 on a1 = a2
        |    join T3 on a1 = a3
        |    left join T4 on a3 = a4 and b4 < 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testOnlyRightJoin(): Unit = {
    val query =
      """
        |select * from T1 right join T2 on a1 = a2
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinWithoutFilter(): Unit = {
    val query =
      """
        |select * from T1
        |    join T2 on a1 = a2
        |    join T3 on a1 = a3
        |    right join T4 on a3 = a4 and b4 < 5
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testOnlyFullJoin(): Unit = {
    val query =
      """
        |select * from T1 full join T2 on a1 = a2
      """.stripMargin
    util.verifyPlan(query)
  }

}
