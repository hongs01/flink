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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.{CorrelationId, JoinRelType}
import org.apache.calcite.rel.logical.{LogicalJoin, LogicalProject}
import org.apache.calcite.rel.rules.MultiJoin
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexUtil}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{ImmutableBitSet, ImmutableIntList}
import com.google.common.collect.ImmutableMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class EliminateCrossJoinRule extends RelOptRule(
  operand(classOf[MultiJoin], any),
  "EliminateCrossJoinRule") {
  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: MultiJoin = call.rel(0)
    val edges = getJoinConditionEdges(join)
    if (join.isFullOuterJoin) {
      // full outer join, do not reorder joins
      call.transformTo(LogicalJoin.create(
        join.getInput(0), join.getInput(1),
        join.getJoinFilter, new java.util.HashSet[CorrelationId](), JoinRelType.FULL))
    } else if (getJoinConditionConnectivity(join, edges)) {
      val (newInputToOld, newInputRefToOld, oldInputRefToNew) = getMapping(join, edges)
      val joinAndProject =
        swapJoinsAndProject(join, newInputToOld, newInputRefToOld, oldInputRefToNew)
      call.transformTo(joinAndProject)
    } else {
      // can't eliminate cross join, do not reorder joins
      call.transformTo(multiJoinToJoin(join))
    }
  }

  private def getSumFields(join: MultiJoin): Seq[Int] = {
    val sumFields = ArrayBuffer.empty[Int]
    for (i <- 0 until join.getInputs.size()) {
      sumFields += join.getInput(i).getRowType.getFieldCount
    }
    for (i <- 1 until sumFields.size) {
      sumFields(i) += sumFields(i - 1)
    }
    sumFields
  }

  def getInputIdx(inputRefIdx: Int, sumFields: Seq[Int]): Int = {
    for (i <- sumFields.indices) {
      if (sumFields(i) > inputRefIdx) {
        return i
      }
    }
    throw new RuntimeException("Input index not found. This is a bug.")
  }

  private def getJoinConditionEdges(join: MultiJoin): Seq[(Int, Int)] = {
    val sumFields = getSumFields(join)
    val edges = ArrayBuffer.empty[(Int, Int)]

    def dfs(rex: RexNode): Unit = {
      if (rex.isA(SqlKind.AND)) {
        val iter = rex.asInstanceOf[RexCall].operands.iterator
        while (iter.hasNext) {
          dfs(iter.next)
        }
      } else {
        if (rex.isA(SqlKind.EQUALS)) {
          val call = rex.asInstanceOf[RexCall]
          if (!call.operands.get(0).isInstanceOf[RexInputRef] ||
            !call.operands.get(1).isInstanceOf[RexInputRef]) {
            return
          }
          val left = call.operands.get(0).asInstanceOf[RexInputRef]
          val right = call.operands.get(1).asInstanceOf[RexInputRef]
          val leftInputIdx = getInputIdx(left.getIndex, sumFields)
          val rightInputIdx = getInputIdx(right.getIndex, sumFields)
          edges.append((leftInputIdx, rightInputIdx))
        }
      }
    }

    dfs(join.getJoinFilter)
    edges
  }

  private def getJoinConditionConnectivity(join: MultiJoin, edges: Seq[(Int, Int)]): Boolean = {
    val root = ArrayBuffer.range(0, join.getInputs.size())

    def findRoot(node: Int): Int = {
      if (root(node) != node) {
        root(node) = findRoot(root(node))
      }
      root(node)
    }

    edges.foreach(e => {
      val a = findRoot(e._1)
      val b = findRoot(e._2)
      root(a) = b
    })
    var rootCount = 0
    for (i <- root.indices) {
      if (join.getOuterJoinConditions.get(i) == null) {
        rootCount += (if (root(i) == i) 1 else 0)
      }
    }
    rootCount == 1
  }

  private def getMapping(join: MultiJoin, edges: Seq[(Int, Int)]):
  (Seq[Int], Seq[Int], Seq[Int]) = {
    val vis = ArrayBuffer.fill(join.getInputs.size) {
      false
    }
    val e = Seq.fill(join.getInputs.size()) {
      ArrayBuffer.empty[Int]
    }
    for (edge <- edges) {
      e(edge._1).append(edge._2)
      e(edge._2).append(edge._1)
    }

    def getStart: Int = {
      for (i <- 0 until join.getInputs.size) {
        if (join.getOuterJoinConditions.get(i) == null) {
          return i
        }
      }
      throw new RuntimeException("No inner join table found. This is a bug.")
    }

    val start = getStart
    vis(start) = true
    val next = collection.mutable.PriorityQueue(start).reverse
    val order = ArrayBuffer.empty[Int]
    while (next.nonEmpty) {
      val sn = next.head
      next.dequeue
      order += sn
      for (fn <- e(sn)) {
        if (!vis(fn)) {
          vis(fn) = true
          next.enqueue(fn)
        }
      }
    }
    val newInputToOld = ArrayBuffer.empty[Int]

    {
      var j = 0
      for (i <- 0 until join.getInputs.size) {
        if (join.getOuterJoinConditions.get(i) == null) {
          newInputToOld += order(j)
          j += 1
        } else {
          newInputToOld += i
        }
      }
    }

    val sumFields = getSumFields(join)
    val newInputRefToOld = ArrayBuffer.empty[Int]
    for (i <- newInputToOld) {
      val fieldCount = join.getInput(i).getRowType.getFieldCount
      for (j <- 0 until fieldCount) {
        newInputRefToOld += sumFields(i) - (fieldCount - j)
      }
    }
    val oldInputRefToNew = ArrayBuffer.fill(newInputRefToOld.size) {
      0
    }
    for ((v, i) <- newInputRefToOld.zipWithIndex) {
      oldInputRefToNew(v) = i
    }
    (newInputToOld, newInputRefToOld, oldInputRefToNew)
  }

  private def swapJoinsAndProject(
                                   join: MultiJoin,
                                   newInputToOld: Seq[Int],
                                   newInputRefToOld: Seq[Int],
                                   oldInputRefToNew: Seq[Int]): RelNode = {
    val newInputs = new java.util.ArrayList[RelNode]()
    val newOuterJoinConditions = new java.util.ArrayList[RexNode]()
    val newJoinTypes = new java.util.ArrayList[JoinRelType]()
    val newProjFields = new java.util.ArrayList[ImmutableBitSet]()
    val newJoinFieldRefCountsMapBuilder = new ImmutableMap.Builder[Integer, ImmutableIntList]()
    // reorder inputs
    for ((oldIdx, newIdx) <- newInputToOld.zipWithIndex) {
      newInputs.add(join.getInput(oldIdx))
      newOuterJoinConditions.add(join.getOuterJoinConditions.get(oldIdx))
      newJoinTypes.add(join.getJoinTypes.get(oldIdx))
      newProjFields.add(join.getProjFields.get(oldIdx))
      newJoinFieldRefCountsMapBuilder.put(newIdx, join.getJoinFieldRefCountsMap.get(oldIdx))
    }
    // change the ids of input refs
    val adjustment = ArrayBuffer.empty[Int]
    for ((v, i) <- oldInputRefToNew.zipWithIndex) {
      adjustment += v - i
    }

    def rewriteFilter(filter: RexNode): RexNode =
      if (filter == null) {
        null
      } else {
        filter.accept(new RelOptUtil.RexInputConverter(
          join.getCluster.getRexBuilder,
          join.getRowType.getFieldList,
          adjustment.toArray))
      }

    val newJoinFilter = rewriteFilter(join.getJoinFilter)
    val newPostJoinFilter = rewriteFilter(join.getPostJoinFilter)
    // update row type
    val newRowTypeBuilder = new RelDataTypeFactory.Builder(join.getCluster.getTypeFactory)
    val fieldNames = join.getRowType.getFieldNames
    val fields = join.getRowType.getFieldList
    for (idx <- newInputRefToOld) {
      newRowTypeBuilder.add(fieldNames.get(idx), fields.get(idx).getType)
    }
    val newMultiJoin = new MultiJoin(
      join.getCluster,
      newInputs,
      newJoinFilter,
      newRowTypeBuilder.build,
      join.isFullOuterJoin,
      newOuterJoinConditions,
      newJoinTypes,
      newProjFields,
      newJoinFieldRefCountsMapBuilder.build,
      newPostJoinFilter)
    val newJoinTree = multiJoinToJoin(newMultiJoin)
    // use project to keep the final result be the same
    val projects = new java.util.ArrayList[RexNode]()
    for ((newIdx, oldIdx) <- oldInputRefToNew.zipWithIndex) {
      projects.add(new RexInputRef(newIdx, fields.get(oldIdx).getType))
    }
    new LogicalProject(
      join.getCluster,
      join.getTraitSet,
      newJoinTree,
      projects,
      join.getRowType
    )
  }

  private def multiJoinToJoin(join: MultiJoin): RelNode = {
    val sumFields = getSumFields(join)
    val joinConditions = Seq.fill(join.getInputs.size) {
      ArrayBuffer.empty[RexNode]
    }
    val otherConditions = ArrayBuffer.empty[RexNode]

    def dfs(rex: RexNode): Unit = {
      if (rex.isA(SqlKind.AND)) {
        val iter = rex.asInstanceOf[RexCall].operands.iterator
        while (iter.hasNext) {
          dfs(iter.next)
        }
      } else {
        if (rex.isA(SqlKind.EQUALS)) {
          val call = rex.asInstanceOf[RexCall]
          call.operands.get(0) match {
            case left: RexInputRef =>
              call.operands.get(1) match {
                case right: RexInputRef =>
                  val leftInputIdx = getInputIdx(left.getIndex, sumFields)
                  val rightInputIdx = getInputIdx(right.getIndex, sumFields)
                  joinConditions(Math.max(leftInputIdx, rightInputIdx)).append(rex)
                  return
                case _ =>
              }
            case _ =>
          }
          otherConditions.append(rex)
        }
      }
    }

    dfs(join.getJoinFilter)
    val rexBuilder = join.getCluster.getRexBuilder
    var joinTree: RelNode = null
    for (i <- 0 until join.getInputs.size) {
      if (join.getOuterJoinConditions.get(i) == null) {
        if (joinTree == null) {
          joinTree = join.getInput(i)
        } else joinTree = LogicalJoin.create(
          joinTree, join.getInput(i),
          RexUtil.composeConjunction(rexBuilder, joinConditions(i).toList.asJava, false),
          new java.util.HashSet[CorrelationId](), JoinRelType.INNER)
      }
    }
    for (i <- 0 until join.getInputs.size) {
      if (join.getOuterJoinConditions.get(i) != null) {
        joinTree = join.getJoinTypes.get(i) match {
          case JoinRelType.LEFT =>
            LogicalJoin.create(
              joinTree, join.getInput(i),
              join.getOuterJoinConditions.get(i),
              new java.util.HashSet[CorrelationId](), JoinRelType.LEFT)
          case JoinRelType.RIGHT =>
            LogicalJoin.create(
              join.getInput(i), joinTree,
              join.getOuterJoinConditions.get(i),
              new java.util.HashSet[CorrelationId](), JoinRelType.RIGHT)
        }
      }
    }
    val postJoinConditions = join.getPostJoinFilter
    if (postJoinConditions != null) {
      otherConditions.append(postJoinConditions)
    }
    val otherConditionsConj =
      RexUtil.composeConjunction(rexBuilder, otherConditions.toList.asJava, true)
    if (otherConditionsConj != null) {
      val lastJoin = joinTree.asInstanceOf[LogicalJoin]
      joinTree = LogicalJoin.create(
        lastJoin.getLeft, lastJoin.getRight,
        RexUtil.composeConjunction(rexBuilder, Seq(
          lastJoin.getCondition, otherConditionsConj).asJava, false),
        new java.util.HashSet[CorrelationId](), lastJoin.getJoinType)
    }
    joinTree
  }
}

object EliminateCrossJoinRule {
  val INSTANCE = new EliminateCrossJoinRule
}
