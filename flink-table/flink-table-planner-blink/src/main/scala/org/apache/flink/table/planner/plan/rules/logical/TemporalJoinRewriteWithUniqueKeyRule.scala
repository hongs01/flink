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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexNode, RexShuttle}

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}
import org.apache.flink.table.planner.plan.rules.physical.common.CommonTemporalTableJoinRule
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil

import scala.collection.JavaConversions._

/**
  * Planner rule that rewrites temporal join with extracted primary key, Event-time temporal
  * table join requires primary key and row time attribute of versioned table. The versioned table
  * could be a table source or a view only if it contains the unique key and time attribute.
  *
  * <p> Flink support extract the primary key and row time attribute from the view if the view comes
  * from [[LogicalRank]] node which can convert to a [[Deduplicate]] node.
  */
class TemporalJoinRewriteWithUniqueKeyRule extends RelOptRule(
  operand(classOf[FlinkLogicalJoin],
    operand(classOf[FlinkLogicalRel], any()),
    operand(classOf[FlinkLogicalSnapshot],
      operand(classOf[FlinkLogicalRel], any()))),
  "TemporalJoinRewriteWithUniqueKeyRule")
  with CommonTemporalTableJoinRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)
    val snapshotInput = call.rel[FlinkLogicalRel](3)

    val isTemporalJoin = matches(snapshot)
    val canConvertToLookup = canConvertToLookupJoin(snapshot, snapshotInput)
    val supportedJoinTypes = Seq(JoinRelType.INNER)

    isTemporalJoin && !canConvertToLookup && supportedJoinTypes.contains(join.getJoinType)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val leftInput = call.rel[FlinkLogicalRel](1)
    val snapshot = call.rel[FlinkLogicalSnapshot](2)

    val joinCondition = join.getCondition
    val joinInfo = JoinInfo.of(leftInput, snapshot, joinCondition)
    val leftFieldCnt = leftInput.getRowType.getFieldCount
    val joinRowType = join.getRowType

    val newJoinCondition = joinCondition.accept(new RexShuttle {
      override def visitCall(call: RexCall): RexNode = {
        if (call.getOperator == TemporalJoinUtil.TEMPORAL_JOIN_CONDITION &&
        isRowTimeTemporalTableJoin(snapshot)) {
          val snapshotTimeInputRef = call.operands(0)
          val rightTimeInputRef = call.operands(1)
          val leftJoinKey = call.operands(2).asInstanceOf[RexCall].operands
          val rightJoinKey = call.operands(3).asInstanceOf[RexCall].operands

          val rexBuilder = join.getCluster.getRexBuilder
          val primaryKeyInputRefs = extractPrimaryKeyInputRefs(leftFieldCnt, snapshot, rexBuilder)
          if (primaryKeyInputRefs.isEmpty) {
            throw new ValidationException("Event-Time Temporal Table Join requires both" +
              s" primary key and row time attribute in versioned table," +
              s" but no primary key found.")
          }
          validatePrimaryKeyInVersionedTable(joinRowType, leftFieldCnt,
            joinInfo, primaryKeyInputRefs.get)
          TemporalJoinUtil.makeRowTimeTemporalJoinConditionCall(rexBuilder, snapshotTimeInputRef,
            rightTimeInputRef, leftJoinKey, rightJoinKey, primaryKeyInputRefs.get)
        }
        else {
          super.visitCall(call)
        }
      }
    })
    val rewriteJoin = FlinkLogicalJoin.create(
      leftInput, snapshot, newJoinCondition, join.getJoinType)
    call.transformTo(rewriteJoin)
  }

  private def validatePrimaryKeyInVersionedTable(
      joinRowType: RelDataType,
      leftFieldCnt: Int,
      joinInfo: JoinInfo,
      rightPrimaryKey: Seq[RexNode]): Unit  = {

    val rightJoinKeyIndices = joinInfo.rightKeys
      .map(index => index + leftFieldCnt)
      .toArray

    val rightPrimaryKeyIndices = rightPrimaryKey
      .map(r => r.asInstanceOf[RexInputRef].getIndex)
      .toArray

    val joinKeyContainsPrimaryKey = rightPrimaryKeyIndices
      .forall(key => rightJoinKeyIndices.contains(key))

    if (!joinKeyContainsPrimaryKey) {
      val joinKeyFieldNames = rightJoinKeyIndices.map(i => joinRowType.getFieldNames.get(i))
        .toList
        .mkString(",")

      val primaryKeyFieldNames = rightPrimaryKeyIndices.map(i => joinRowType.getFieldNames.get(i))
        .toList
        .mkString(",")

      throw new ValidationException(
        s"Join key [$joinKeyFieldNames] must contains versioned table's " +
          s"primary key [$primaryKeyFieldNames] in Event-time temporal table join")
    }
  }

  private def extractPrimaryKeyInputRefs(
      leftFieldCnt: Int,
      snapshot: FlinkLogicalSnapshot,
      rexBuilder: RexBuilder): Option[Seq[RexNode]] = {
    val rightFields = snapshot.getRowType.getFieldList
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(snapshot.getCluster.getMetadataQuery)

    val uniqueKeys = fmq.getUniqueKeys(snapshot.getInput())
    val fields = snapshot.getRowType.getFieldList

    if (uniqueKeys != null && uniqueKeys.size() > 0) {
      uniqueKeys
        .filter(_.nonEmpty)
        .map(_.toArray
          .map(fields)
          .map(f => rexBuilder.makeInputRef(
            f.getType,
            leftFieldCnt + rightFields.indexOf(f)))
          .toSeq)
        .toArray
        .sortBy(_.length)
        .headOption
    } else {
      None
    }
  }

  private def isRowTimeTemporalTableJoin(snapshot: FlinkLogicalSnapshot): Boolean =
    snapshot.getPeriod.getType match {
      case t: TimeIndicatorRelDataType if t.isEventTime => true
      case _ => false
    }
}

object TemporalJoinRewriteWithUniqueKeyRule {
  val INSTANCE: TemporalJoinRewriteWithUniqueKeyRule = new TemporalJoinRewriteWithUniqueKeyRule
}


