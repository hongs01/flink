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

import com.google.common.collect.{ImmutableMap, ImmutableSet}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.{CorrelationId, JoinRelType}
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.rel.rules.MultiJoin
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexUtil}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{ImmutableBitSet, ImmutableIntList}
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Eliminate cross join by reordering joins.
  */
class EliminateCrossJoinRule extends RelOptRule(
  operand(classOf[MultiJoin], any),
  "EliminateCrossJoinRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: MultiJoin = call.rel(0)

    if (join.isFullOuterJoin) {
      // full outer join, do not reorder joins
      Preconditions.checkArgument(
        join.getInputs.size == 2,
        "Full outer join must have exactly 2 inputs. This is a bug.", null)
      val fullJoin = LogicalJoin.create(
        join.getInput(0), join.getInput(1),
        join.getJoinFilter, ImmutableSet.of[CorrelationId](), JoinRelType.FULL)
      call.transformTo(fullJoin)
    } else {
      // try to eliminate cross join
      val edges = getJoinEquiConditionEdges(join)
      val (newInputToOld, newInputRefToOld, oldInputRefToNew) = getMapping(join, edges)
      val joinAndProject =
        swapJoinsAndProject(join, newInputToOld, newInputRefToOld, oldInputRefToNew)
      call.transformTo(joinAndProject)
    }
  }

  private def getJoinEnd(join: MultiJoin): Seq[Int] = {
    val joinEnd = ArrayBuffer.empty[Int]
    var total = 0
    for (i <- 0 until join.getInputs.size()) {
      total += join.getInput(i).getRowType.getFieldCount
      joinEnd += total
    }
    joinEnd
  }

  def findRef(inputRefIdx: Int, joinEnd: Seq[Int]): Int = {
    // find `inputRefIdx` belongs to which input, using binary search
    if (joinEnd.last <= inputRefIdx) {
      throw new RuntimeException("Input index not found. This is a bug.")
    }

    var head = 0
    var tail = joinEnd.size - 1
    while (head < tail) {
      val mid = (head + tail) / 2
      if (joinEnd(mid) <= inputRefIdx) {
        head = mid + 1
      } else {
        tail = mid
      }
    }
    head
  }

  private def getJoinEquiConditionEdges(join: MultiJoin): Seq[(Int, Int)] = {
    val joinEnd = getJoinEnd(join)

    // an equi condition connects two inputs
    val edges = ArrayBuffer.empty[(Int, Int)]
    for (rex <- RelOptUtil.conjunctions(join.getJoinFilter).asScala) {
      if (isEquiCondition(rex)) {
        val call = rex.asInstanceOf[RexCall]
        (call.operands.get(0), call.operands.get(1)) match {
          case (left: RexInputRef, right: RexInputRef) =>
            val leftInputIdx = findRef(left.getIndex, joinEnd)
            val rightInputIdx = findRef(right.getIndex, joinEnd)
            edges.append((leftInputIdx, rightInputIdx))
          case _ =>
        }
      }
    }

    edges
  }

  /**
    * Returns the mapping between old join orderings and new join orderings.
    * 1st sequence: index = new input index, value = old input index
    * 2nd sequence: index = new input ref index, value = old input ref index
    * 3rd sequence: index = old input ref index, value = new input ref index
    */
  private def getMapping(join: MultiJoin, edges: Seq[(Int, Int)]):
  (Seq[Int], Seq[Int], Seq[Int]) = {
    // traverse the graph to generate a new join order
    val visited = ArrayBuffer.fill(join.getInputs.size){false}
    val graph = Seq.fill(join.getInputs.size()){ArrayBuffer.empty[Int]}
    for (edge <- edges) {
      graph(edge._1).append(edge._2)
      graph(edge._2).append(edge._1)
    }
    val order = ArrayBuffer.empty[Int]

    for (start <- 0 until join.getInputs.size) {
      if (join.getJoinTypes.get(start) == JoinRelType.INNER && !visited(start)) {
        // we found an unvisited inner join input with the smallest index
        visited(start) = true
        // we use priority queue here
        // because we want to keep the original ordering of joins as much as possible
        val priorityQueue = collection.mutable.PriorityQueue(start).reverse
        while (priorityQueue.nonEmpty) {
          val currentJoin = priorityQueue.head
          priorityQueue.dequeue
          order += currentJoin

          for (nextJoin <- graph(currentJoin)) {
            if (!visited(nextJoin)) {
              visited(nextJoin) = true
              priorityQueue.enqueue(nextJoin)
            }
          }
        }
      }
    }

    // generate mapping. index = new input index, value = old input index
    val newInputToOld = ArrayBuffer.empty[Int]

    {
      var j = 0
      for (i <- 0 until join.getInputs.size) {
        if (join.getJoinTypes.get(i) == JoinRelType.INNER) {
          newInputToOld += order(j)
          j += 1
        } else {
          newInputToOld += i
        }
      }
    }

    // generate mapping. index = new input ref index, value = old input ref index
    val joinEnd = getJoinEnd(join)
    val newInputRefToOld = ArrayBuffer.empty[Int]
    for (i <- newInputToOld) {
      val fieldCount = join.getInput(i).getRowType.getFieldCount
      for (j <- 0 until fieldCount) {
        newInputRefToOld += joinEnd(i) - (fieldCount - j)
      }
    }

    // index = old input ref index, value = new input ref index
    val oldInputRefToNew = ArrayBuffer.fill(newInputRefToOld.size){0}
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
    LogicalProject.create(newJoinTree, projects, join.getRowType)
  }

  /**
    * Change multi-join back to join.
    * The inputs will be joined from left to right, skipping outer join inputs.
    * The outer join inputs will be joined at the end.
    */
  private def multiJoinToJoin(join: MultiJoin): RelNode = {
    val joinEnd = getJoinEnd(join)

    val joinConditions = Seq.fill(join.getInputs.size) {ArrayBuffer.empty[RexNode]}

    // extract conditions and relate each condition with the right-most input in that condition
    class InputFinder extends RelOptUtil.InputFinder {
      var maxInputRef: Int = 0

      override def visitInputRef(inputRef: RexInputRef): Void = {
        maxInputRef = Math.max(maxInputRef, inputRef.getIndex)
        super.visitInputRef(inputRef)
      }
    }

    for (condition <- RelOptUtil.conjunctions(join.getJoinFilter).asScala) {
      val inputFinder = new InputFinder()
      condition.accept(inputFinder)
      joinConditions(findRef(inputFinder.maxInputRef, joinEnd)).append(condition)
    }

    val rexBuilder = join.getCluster.getRexBuilder
    var joinTree: RelNode = null

    // first build inner joins in new order
    for (i <- 0 until join.getInputs.size) {
      if (join.getJoinTypes.get(i) == JoinRelType.INNER) {
        if (joinTree == null) {
          joinTree = join.getInput(i)
        } else {
          joinTree = LogicalJoin.create(
            joinTree, join.getInput(i),
            // as the inputs on the left have been joined together,
            // we can safely apply the join conditions
            RexUtil.composeConjunction(rexBuilder, joinConditions(i).toList.asJava, false),
            ImmutableSet.of[CorrelationId](), JoinRelType.INNER)
        }
      }
    }
    Preconditions.checkNotNull(joinTree, "No inner join found. This is a bug.", null)

    // next build outer joins
    for (i <- 0 until join.getInputs.size) {
      joinTree = join.getJoinTypes.get(i) match {
        case JoinRelType.LEFT =>
          LogicalJoin.create(
            joinTree, join.getInput(i),
            join.getOuterJoinConditions.get(i),
            ImmutableSet.of[CorrelationId](), JoinRelType.LEFT)
        case JoinRelType.RIGHT =>
          LogicalJoin.create(
            join.getInput(i), joinTree,
            join.getOuterJoinConditions.get(i),
            ImmutableSet.of[CorrelationId](), JoinRelType.RIGHT)
        case _ => joinTree
      }
    }

    // finally add post join conditions
    val postJoinConditions = join.getPostJoinFilter
    if (postJoinConditions != null) {
      joinTree = LogicalFilter.create(joinTree, postJoinConditions)
    }

    joinTree
  }

  private def isEquiCondition(rex: RexNode): Boolean =
  // TODO: IS_NOT_DISTINCT_FROM might be changed to CASE
    rex.isA(SqlKind.EQUALS) || rex.isA(SqlKind.IS_NOT_DISTINCT_FROM)
}

object EliminateCrossJoinRule {
  val INSTANCE = new EliminateCrossJoinRule
}
