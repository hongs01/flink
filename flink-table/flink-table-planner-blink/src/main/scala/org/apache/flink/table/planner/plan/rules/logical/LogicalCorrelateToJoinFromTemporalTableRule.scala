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
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.{RelNode}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalProject, LogicalSnapshot, LogicalTableScan}
import org.apache.calcite.rex._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.connector.source.LookupTableSource
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalLegacyTableSourceScan, FlinkLogicalTableSourceScan}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecLookupJoin, StreamExecTemporalJoin}
import org.apache.flink.table.planner.plan.schema.{LegacyTableSourceTable, TableSourceTable, TimeIndicatorRelDataType}
import org.apache.flink.table.planner.plan.utils.{RexDefaultVisitor, TemporalJoinUtil}
import org.apache.flink.table.sources.LookupableTableSource

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * The initial temporal table join (FOR SYSTEM_TIME AS OF) is a Correlate, rewrite it into a Join
  * to make join condition can be pushed-down. The join will be translated into
  * [[StreamExecLookupJoin]] in physical or translated into [[StreamExecTemporalJoin]].
  */
abstract class LogicalCorrelateToJoinFromTemporalTableRule(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description) {

  def getFilterCondition(call: RelOptRuleCall): RexNode

  def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot

  /** Trim out the HepRelVertex wrapper and get current relational expression. */
  protected def trimHep(node: RelNode): RelNode = {
    node match {
      case hepRelVertex: HepRelVertex =>
        hepRelVertex.getCurrentRel
      case _ => node
    }
  }

  protected def validateSnapshotInCorrelate(
      snapshot: LogicalSnapshot,
      correlate: LogicalCorrelate): Unit = {
    // period specification check
    snapshot.getPeriod.getType match {
      // validate type is  event-time or processing time
      case t: TimeIndicatorRelDataType =>
      case _ =>
        throw new TableException("Temporal table join currently only supports " +
          "'FOR SYSTEM_TIME AS OF' left table's time attribute field")
    }

    snapshot.getPeriod match {
      // validate period comes from left table's field
      case r: RexFieldAccess if r.getReferenceExpr.isInstanceOf[RexCorrelVariable] &&
        correlate.getCorrelationId.equals(r.getReferenceExpr.asInstanceOf[RexCorrelVariable].id) =>
      case _ =>
        throw new TableException("Temporal table join currently only supports " +
          "'FOR SYSTEM_TIME AS OF' left table's time attribute field'")
    }
  }

  protected def isLookupJoin(snapshot: LogicalSnapshot, snapshotInput: RelNode): Boolean = {
    val isProcessingTime = snapshot.getPeriod.getType match {
      case t: TimeIndicatorRelDataType if !t.isEventTime => true
      case _ => false
    }

    val tableScan = getTableScan(snapshotInput)
    val snapshotOnLookupSource = tableScan match {
      case Some(scan) => isTableSourceScan(scan) && isLookupTableSource(scan)
      case _ => false
    }

    isProcessingTime && snapshotOnLookupSource
  }

  private def getTableScan(snapshotInput: RelNode): Option[TableScan] = {
    snapshotInput match {
      case tableScan: TableScan
        => Some(tableScan)
      // computed column on lookup table
      case project: LogicalProject if trimHep(project.getInput).isInstanceOf[TableScan]
        => Some(trimHep(project.getInput).asInstanceOf[TableScan])
      case _ => None
    }
  }

  private def isTableSourceScan(relNode: RelNode): Boolean = {
    relNode match {
      case _: LogicalTableScan | _: FlinkLogicalLegacyTableSourceScan |
           _: FlinkLogicalTableSourceScan => true
      case _ => false
    }
  }

  private def isLookupTableSource(relNode: RelNode): Boolean = relNode match {
    case scan: FlinkLogicalLegacyTableSourceScan =>
      scan.tableSource.isInstanceOf[LookupableTableSource[_]]
    case scan: FlinkLogicalTableSourceScan =>
      scan.tableSource.isInstanceOf[LookupTableSource]
    case scan: LogicalTableScan =>
      scan.getTable match {
        case table: LegacyTableSourceTable[_] =>
          table.tableSource.isInstanceOf[LookupableTableSource[_]]
        case table: TableSourceTable =>
          table.tableSource.isInstanceOf[LookupTableSource]
      }
    case _ => false
  }
}

/**
 * Lookup join is a kind of temporal table join implementation which only supports
 * Processing-time temporal table join and the right input required a [[LookupTableSource]].
 */
abstract class LogicalCorrelateToJoinFromLookupTemporalTableRule(
    operand: RelOptRuleOperand,
    description: String)
  extends LogicalCorrelateToJoinFromTemporalTableRule(operand, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: LogicalCorrelate = call.rel(0)
    val leftInput: RelNode = call.rel(1)
    val filterCondition = getFilterCondition(call)
    val snapshot = getLogicalSnapshot(call)

    validateSnapshotInCorrelate(snapshot, correlate)

    val leftRowType = leftInput.getRowType
    val joinCondition = filterCondition.accept(new RexShuttle() {
      // change correlate variable expression to normal RexInputRef (which is from left side)
      override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
        fieldAccess.getReferenceExpr match {
          case corVar: RexCorrelVariable =>
            require(correlate.getCorrelationId.equals(corVar.id))
            val index = leftRowType.getFieldList.indexOf(fieldAccess.getField)
            RexInputRef.of(index, leftRowType)
          case _ => super.visitFieldAccess(fieldAccess)
        }
      }

      // update the field index from right side
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        val rightIndex = leftRowType.getFieldCount + inputRef.getIndex
        new RexInputRef(rightIndex, inputRef.getType)
      }
    })

    val builder = call.builder()
    builder.push(leftInput)
    builder.push(snapshot)
    builder.join(correlate.getJoinType, joinCondition)

    val rel = builder.build()
    call.transformTo(rel)
  }
}


/**
 * General temporal table join rule to rewrite the original Correlate into a Join.
 */
abstract class LogicalCorrelateToJoinFromGeneralTemporalTableRule(
    operand: RelOptRuleOperand,
    description: String)
  extends LogicalCorrelateToJoinFromTemporalTableRule(operand, description) {

  protected def extractRightTimeInputRef(
      leftInput: RelNode,
      snapshot: LogicalSnapshot): Option[RexNode] = {
    val rightFields = snapshot.getRowType.getFieldList.asScala
    val timeAttributeFields = rightFields.filter(
      f => f.getType.isInstanceOf[TimeIndicatorRelDataType])
    val rexBuilder = snapshot.getCluster.getRexBuilder

    if (timeAttributeFields != null && timeAttributeFields.length == 1) {
      val leftFieldCnt = leftInput.getRowType.getFieldCount
      val timeColIndex = leftFieldCnt + rightFields.indexOf(timeAttributeFields.get(0))
      val timeColDataType = timeAttributeFields.get(0).getType;
      Some(rexBuilder.makeInputRef(timeColDataType, timeColIndex))
    } else {
      None
    }
  }

  protected def extractSnapshotTimeInputRef(
      leftInput: RelNode,
      snapshot: LogicalSnapshot): RexInputRef = {
    val leftRowType = leftInput.getRowType
    val periodField = snapshot.getPeriod.asInstanceOf[RexFieldAccess].getField
    val index = leftRowType.getFieldList.indexOf(periodField)
    RexInputRef.of(index, leftRowType)
  }

  protected def extractPrimaryKeyInputRefs(
     leftInput: RelNode,
     snapshot: LogicalSnapshot,
     rexBuilder: RexBuilder): Option[Seq[RexNode]] = {

    val rightFields = snapshot.getRowType.getFieldList
    val fmq = snapshot.getCluster.getMetadataQuery
    val uniqueKeys = fmq.getUniqueKeys(snapshot.getInput())
    val fields = snapshot.getRowType.getFieldList

    if (uniqueKeys != null && uniqueKeys.size() > 0) {
     val leftFieldCnt = leftInput.getRowType.getFieldCount
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

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: LogicalCorrelate = call.rel(0)
    val leftInput: RelNode = call.rel(1)
    val filterCondition = getFilterCondition(call)
    val snapshot = getLogicalSnapshot(call)

    val leftRowType = leftInput.getRowType
    val joinCondition = filterCondition.accept(new RexShuttle() {
      // change correlate variable expression to normal RexInputRef (which is from left side)
      override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
        fieldAccess.getReferenceExpr match {
          case corVar: RexCorrelVariable =>
            require(correlate.getCorrelationId.equals(corVar.id))
            val index = leftRowType.getFieldList.indexOf(fieldAccess.getField)
            RexInputRef.of(index, leftRowType)
          case _ => super.visitFieldAccess(fieldAccess)
        }
      }

      // update the field index from right side
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        val rightIndex = leftRowType.getFieldCount + inputRef.getIndex
        new RexInputRef(rightIndex, inputRef.getType)
      }
    })

    validateSnapshotInCorrelate(snapshot, correlate)

    val rexBuilder = correlate.getCluster.getRexBuilder

    val snapshotTimeInputRef= extractSnapshotTimeInputRef(leftInput, snapshot)
    val rightTimeInputRef = extractRightTimeInputRef(leftInput, snapshot)

    val primaryKeyInputRefs = extractPrimaryKeyInputRefs(leftInput, snapshot, rexBuilder)

    val temporalCondition = if(isProcTimeTemporalTableJoin(snapshot)) {
        TemporalJoinUtil.makeProcTimeTemporalJoinConditionCall(rexBuilder, snapshotTimeInputRef)
    } else {
      if (primaryKeyInputRefs.isDefined && rightTimeInputRef.isDefined) {
        if (joinKeyContainsPrimaryKey(joinCondition, primaryKeyInputRefs.get)) {
          TemporalJoinUtil.makeRowTimeTemporalJoinConditionCall(
            rexBuilder, snapshotTimeInputRef, rightTimeInputRef.get, primaryKeyInputRefs.get)
        } else {
          throw new TableException("Event-Time Temporal Table Join requires primary key " +
            s"contained in join key but the time primary key ${primaryKeyInputRefs} is " +
            s"not in join key ${primaryKeyInputRefs.get}.")
        }
      } else {
        throw new TableException("Event-Time Temporal Table Join requires both primary key and " +
          s"time attribute in temporal table, but the actual primaryKey is " +
          s"${primaryKeyInputRefs.getOrElse("NULL")}, time attribute is" +
          s"${rightTimeInputRef.getOrElse("NULL")}.")
      }
    }

    val builder = call.builder()
    val condition = builder.and(joinCondition, temporalCondition)

    builder.push(leftInput)
    builder.push(snapshot)
    val rewriteJoin = builder.join(correlate.getJoinType, condition).build()
    call.transformTo(rewriteJoin)
  }

  private def isProcTimeTemporalTableJoin(snapshot: LogicalSnapshot): Boolean =
    snapshot.getPeriod.getType match {
      case t: TimeIndicatorRelDataType if !t.isEventTime => true
      case _ => false
    }

  private def joinKeyContainsPrimaryKey(
      joinCondition: RexNode,
      primaryKey: Seq[RexNode]) : Boolean = {
    true
  }
}

/**
  * Planner rule that matches temporal table join which implemented by lookup join, the join
  * condition is not true, that means the right input of the Correlate is a Filter.
  * e.g. SELECT * FROM MyTable AS T JOIN lookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
  * ON T.a = D.id
  */
class LogicalCorrelateToJoinFromLookupTableRuleWithFilter
  extends LogicalCorrelateToJoinFromLookupTemporalTableRule(
    operand(classOf[LogicalCorrelate],
      operand(classOf[RelNode], any()),
      operand(classOf[LogicalFilter],
        operand(classOf[LogicalSnapshot],
          operand(classOf[RelNode], any())))),
    "LogicalCorrelateToJoinFromLookupTableRuleWithFilter"
  ) {
  override def matches(call: RelOptRuleCall): Boolean = {
    val snapshot: LogicalSnapshot = call.rel(3)
    val snapshotInput: RelNode = trimHep(call.rel(4))
    isLookupJoin(snapshot, snapshotInput)
  }

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    val filter: LogicalFilter = call.rel(2)
    filter.getCondition
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(3).asInstanceOf[LogicalSnapshot]
  }
}

/**
  * Planner rule that matches temporal table join which implemented by lookup join, the join
  * condition is true, that means the right input of the Correlate is a Snapshot.
  * e.g. SELECT * FROM MyTable AS T JOIN lookupTable FOR SYSTEM_TIME AS OF T.proctime AS D ON true
  */
class LogicalCorrelateToJoinFromLookupTableRuleWithoutFilter
  extends LogicalCorrelateToJoinFromLookupTemporalTableRule(
    operand(classOf[LogicalCorrelate],
      operand(classOf[RelNode], any()),
      operand(classOf[LogicalSnapshot],
        operand(classOf[RelNode], any()))),
    "LogicalCorrelateToJoinFromLookupTableRuleWithoutFilter"
  ) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val snapshot: LogicalSnapshot = call.rel(2)
    val snapshotInput: RelNode = trimHep(call.rel(3))
    isLookupJoin(snapshot, snapshotInput)
  }

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    call.builder().literal(true)
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(2).asInstanceOf[LogicalSnapshot]
  }
}

/**
 * Planner rule that matches general temporal table join except lookup join, the join
 * condition is not true, that means the right input of the Correlate is a Filter.
 * e.g. SELECT * FROM MyTable AS T JOIN lookupTable FOR SYSTEM_TIME AS OF T.rowtime AS D
 * ON T.a = D.id
 */
class LogicalCorrelateToJoinFromTemporalTableRuleWithFilter
  extends LogicalCorrelateToJoinFromGeneralTemporalTableRule(
    operand(classOf[LogicalCorrelate],
      operand(classOf[RelNode], any()),
      operand(classOf[LogicalFilter],
        operand(classOf[LogicalSnapshot],
        operand(classOf[RelNode], any())))),
      "LogicalCorrelateToJoinFromTemporalTableRuleWithFilter"
  ) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val snapshot: LogicalSnapshot = call.rel(3)
    val snapshotInput: RelNode = trimHep(call.rel(4))
    !isLookupJoin(snapshot, snapshotInput)
  }

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    val filter: LogicalFilter = call.rel(2)
    filter.getCondition
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(3).asInstanceOf[LogicalSnapshot]
  }
}

/**
 * Planner rule that matches general temporal table join except lookup join, the join
 * condition is true, that means the right input of the Correlate is a Snapshot.
 * e.g. SELECT * FROM MyTable AS T JOIN temporalTable FOR SYSTEM_TIME AS OF T.rowtime AS D ON true
 */
class LogicalCorrelateToJoinFromTemporalTableRuleWithoutFilter
  extends LogicalCorrelateToJoinFromGeneralTemporalTableRule(
    operand(classOf[LogicalCorrelate],
      operand(classOf[RelNode], any()),
      operand(classOf[LogicalSnapshot],
        operand(classOf[RelNode], any()))),
    "LogicalCorrelateToJoinFromTemporalTableRuleWithoutFilter"
  ) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val snapshot: LogicalSnapshot = call.rel(2)
    val snapshotInput: RelNode = trimHep(call.rel(3))
    !isLookupJoin(snapshot, snapshotInput)
  }

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    call.builder().literal(true)
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(2).asInstanceOf[LogicalSnapshot]
  }
}

object LogicalCorrelateToJoinFromTemporalTableRule {
  val LOOKUP_JOIN_WITH_FILTER = new LogicalCorrelateToJoinFromLookupTableRuleWithFilter
  val LOOKUP_JOIN_WITHOUT_FILTER = new LogicalCorrelateToJoinFromLookupTableRuleWithoutFilter
  val WITH_FILTER = new LogicalCorrelateToJoinFromTemporalTableRuleWithFilter
  val WITHOUT_FILTER = new LogicalCorrelateToJoinFromTemporalTableRuleWithoutFilter
}
