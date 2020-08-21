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
package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes}
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalSnapshot}
import org.apache.flink.util.Preconditions.checkArgument

import scala.collection.JavaConversions._

/**
  * Utilities for temporal table join.
  */
object TemporalJoinUtil {

  // ----------------------------------------------------------------------------------------
  //                          Temporal TableFunction Join Utilities
  // ----------------------------------------------------------------------------------------

  /**
    * [[TEMPORAL_JOIN_CONDITION]] is a specific condition which correctly defines
    * references to rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute.
    * The condition is used to mark this is a temporal table function join and ensure columns these
    * expressions depends on will not be pruned. Later rightTimeAttribute, rightPrimaryKeyExpression
    * and leftTimeAttribute will be extracted from the condition.
    */
  val TEMPORAL_JOIN_CONDITION = new SqlFunction(
    "__TEMPORAL_JOIN_CONDITION",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.or(
      // Left time attribute, right time attribute and primary key are required
      // for event-time temporal table join,
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.DATETIME,
        OperandTypes.ANY),
      // Only left time attribute is required for processing-time temporal table join,
      // primary key is optional
      OperandTypes.sequence(
        "'(LEFT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
        OperandTypes.DATETIME,
        OperandTypes.ANY),
      // Only left time attribute for processing-time temporal table join
      OperandTypes.DATETIME),
    SqlFunctionCategory.SYSTEM)

  val TEMPORAL_JOIN_CONDITION_PRIMARY_KEY = new SqlFunction(
    "__TEMPORAL_JOIN_CONDITION_PRIMARY_KEY",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN_NOT_NULL,
    null,
    OperandTypes.ARRAY,
    SqlFunctionCategory.SYSTEM)


  def isRowtimeCall(call: RexCall): Boolean = {
    checkArgument(call.getOperator == TEMPORAL_JOIN_CONDITION)
    call.getOperands.size() == 3
  }

  def isProctimeCall(call: RexCall): Boolean = {
    checkArgument(call.getOperator == TEMPORAL_JOIN_CONDITION)
    call.getOperands.size() == 2 || call.getOperands.size() == 1
  }

  def makeRowTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode,
    rightTimeAttribute: RexNode,
    rightPrimaryKeyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      rightTimeAttribute,
      makePrimaryKeyCall(rexBuilder, rightPrimaryKeyExpression))
  }

  def makeProcTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode,
    rightPrimaryKeyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute,
      makePrimaryKeyCall(rexBuilder, rightPrimaryKeyExpression))
  }

  private def makePrimaryKeyCall(
      rexBuilder: RexBuilder,
      rightPrimaryKeyExpression: Seq[RexNode]): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION_PRIMARY_KEY,
      rightPrimaryKeyExpression)
  }

  def makeProcTimeTemporalJoinConditionCall(
    rexBuilder: RexBuilder,
    leftTimeAttribute: RexNode): RexNode = {
    rexBuilder.makeCall(
      TEMPORAL_JOIN_CONDITION,
      leftTimeAttribute)
  }

  def isTemporalJoin(join: FlinkLogicalJoin): Boolean = {
    isTemporalJoin(join.getRight, join.getCondition)
  }

  def isTemporalJoin(joinRightInput: RelNode, condition: RexNode): Boolean = {
    var hasTemporalJoinCondition: Boolean = false
    condition.accept(new RexVisitorImpl[Void](true) {
      override def visitCall(call: RexCall): Void = {
        if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
          super.visitCall(call)
        } else {
          hasTemporalJoinCondition = true
          null
        }
      }
    })
    hasTemporalJoinCondition || joinRightInput.isInstanceOf[FlinkLogicalSnapshot]
  }

}
