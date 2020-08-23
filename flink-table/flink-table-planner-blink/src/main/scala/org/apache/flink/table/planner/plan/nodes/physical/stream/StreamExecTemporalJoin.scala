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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinInfo, JoinRelType}
import org.apache.calcite.rex._
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.transformations.TwoInputTransformation
import org.apache.flink.table.api.{TableConfig, TableException, ValidationException}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.isRowtimeIndicatorType
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.TEMPORAL_JOIN_CONDITION
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.TEMPORAL_JOIN_CONDITION_PRIMARY_KEY
import org.apache.flink.table.planner.plan.utils.{InputRefVisitor, KeySelectorUtil, RelExplainUtil, TemporalJoinUtil}
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector
import org.apache.flink.table.runtime.operators.join.temporal.{TemporalProcessTimeJoinOperator, TemporalRowTimeJoinOperator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Preconditions.checkState

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
 * Stream physical node for temporal table join (FOR SYSTEM_TIME AS OF).
 */
class StreamExecTemporalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  def rightInputUniqueKeyContainsJoinKey(): Boolean = {
    val right = getInput(1)
    val rightUniqueKeys = getCluster.getMetadataQuery.getUniqueKeys(right)
    if (rightUniqueKeys != null) {
      val joinKeys = keyPairs.map(_.target).toArray
      rightUniqueKeys.exists {
        uniqueKey => joinKeys.forall(uniqueKey.toArray.contains(_))
      }
    } else {
      false
    }
  }

  override def requireWatermark: Boolean = {
    TemporalJoinUtil.isRowTimeJoin(cluster.getRexBuilder, getJoinInfo)
  }

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamExecTemporalJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
    ordinalInParent: Int,
    newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
    planner: StreamPlanner): Transformation[RowData] = {

    validateKeyTypes()

    val returnType = FlinkTypeFactory.toLogicalRowType(getRowType)

    val joinTranslator = StreamExecTemporalJoinToCoProcessTranslator.create(
      this.toString,
      planner.getTableConfig,
      returnType,
      leftRel,
      rightRel,
      getJoinInfo,
      cluster.getRexBuilder)

    val joinOperator = joinTranslator.getJoinOperator(joinType, returnType.getFieldNames)
    val leftKeySelector = joinTranslator.getLeftKeySelector
    val rightKeySelector = joinTranslator.getRightKeySelector

    val leftTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val rightTransform = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val ret = new TwoInputTransformation[RowData, RowData, RowData](
      leftTransform,
      rightTransform,
      getRelDetailedDescription,
      joinOperator,
      InternalTypeInfo.of(returnType),
      leftTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftKeySelector, rightKeySelector)
    ret.setStateKeyType(leftKeySelector.asInstanceOf[ResultTypeQueryable[_]].getProducedType)
    ret
  }

  private def validateKeyTypes(): Unit = {
    // at least one equality expression
    val leftFields = left.getRowType.getFieldList
    val rightFields = right.getRowType.getFieldList

    getJoinInfo.pairs().toList.foreach(pair => {
      val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
      val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName
      // check if keys are compatible
      if (leftKeyType != rightKeyType) {
        throw new TableException(
          "Equality join predicate on incompatible types.\n" +
            s"\tLeft: $left,\n" +
            s"\tRight: $right,\n" +
            s"\tCondition: (${RelExplainUtil.expressionToString(
              getCondition, inputRowType, getExpressionString)})"
        )
      }
    })
  }
}


/**
  * @param rightTimeAttributeInputReference is defined only for event time joins.
  */
class StreamExecTemporalJoinToCoProcessTranslator private(
  textualRepresentation: String,
  config: TableConfig,
  returnType: RowType,
  leftInputType: RowType,
  rightInputType: RowType,
  joinInfo: JoinInfo,
  rexBuilder: RexBuilder,
  leftTimeAttributeInputReference: Int,
  rightTimeAttributeInputReference: Option[Int],
  remainingNonEquiJoinPredicates: Option[RexNode]) {

  val nonEquiJoinPredicates: Option[RexNode] = remainingNonEquiJoinPredicates

  def getLeftKeySelector: RowDataKeySelector = {
    KeySelectorUtil.getRowDataSelector(
      joinInfo.leftKeys.toIntArray,
      InternalTypeInfo.of(leftInputType)
    )
  }

  def getRightKeySelector: RowDataKeySelector = {
    KeySelectorUtil.getRowDataSelector(
      joinInfo.rightKeys.toIntArray,
      InternalTypeInfo.of(rightInputType)
    )
  }

  def getJoinOperator(
    joinType: JoinRelType,
    returnFieldNames: Seq[String]): TwoInputStreamOperator[RowData, RowData, RowData] = {

    // input must not be nullable, because the runtime join function will make sure
    // the code-generated function won't process null inputs
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
      .bindInput(leftInputType)
      .bindSecondInput(rightInputType)

    val body = if (nonEquiJoinPredicates.isEmpty) {
      // only equality condition
      "return true;"
    } else {
      val condition = exprGenerator.generateExpression(nonEquiJoinPredicates.get)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    val generatedJoinCondition = FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "ConditionFunction",
      body)

    createJoinOperator(config, joinType, generatedJoinCondition)
  }

  protected def createJoinOperator(
    tableConfig: TableConfig,
    joinType: JoinRelType,
    generatedJoinCondition: GeneratedJoinCondition)
  : TwoInputStreamOperator[RowData, RowData, RowData] = {

    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
    joinType match {
      case JoinRelType.INNER =>
        if (rightTimeAttributeInputReference.isDefined) {
          new TemporalRowTimeJoinOperator(
            InternalTypeInfo.of(leftInputType),
            InternalTypeInfo.of(rightInputType),
            generatedJoinCondition,
            leftTimeAttributeInputReference,
            rightTimeAttributeInputReference.get,
            minRetentionTime,
            maxRetentionTime)
        } else {
          new TemporalProcessTimeJoinOperator(
            InternalTypeInfo.of(rightInputType),
            generatedJoinCondition,
            minRetentionTime,
            maxRetentionTime)
        }
      case _ =>
        throw new ValidationException(
          s"Only ${JoinRelType.INNER} temporal join is supported in [$textualRepresentation]")
    }
  }
}

object StreamExecTemporalJoinToCoProcessTranslator {
  def create(
    textualRepresentation: String,
    config: TableConfig,
    returnType: RowType,
    leftInput: RelNode,
    rightInput: RelNode,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder): StreamExecTemporalJoinToCoProcessTranslator = {


    val leftType = FlinkTypeFactory.toLogicalRowType(leftInput.getRowType)
    val rightType = FlinkTypeFactory.toLogicalRowType(rightInput.getRowType)

    val temporalJoinConditionExtractor = new TemporalJoinConditionExtractor(
      textualRepresentation,
      leftType.getFieldCount,
      joinInfo,
      rexBuilder)

    val (leftTimeAttributeInputRef, rightTimeAttributeInputRef, remainingNonEquiJoinPredicates) =
      if (TemporalJoinUtil.isRowTimeJoin(rexBuilder, joinInfo)) {
      checkState(
        !joinInfo.isEqui,
        "Missing %s in Event-Time temporal join condition",
        TEMPORAL_JOIN_CONDITION)
      val nonEquiJoinRex: RexNode = joinInfo.getRemaining(rexBuilder)
      val remainingNonEquiJoinPredicates = temporalJoinConditionExtractor.apply(nonEquiJoinRex)

      checkState(
        temporalJoinConditionExtractor.leftTimeAttribute.isDefined &&
          temporalJoinConditionExtractor.rightPrimaryKeyExpression.isDefined,
        "Missing %s in Event-Time temporal join condition",
        TEMPORAL_JOIN_CONDITION)
        (extractInputRef(
          temporalJoinConditionExtractor.leftTimeAttribute.get,
          textualRepresentation),
          temporalJoinConditionExtractor.rightTimeAttribute.map(
            rightTimeAttribute =>
              extractInputRef(
                rightTimeAttribute,
                textualRepresentation
              ) - leftType.getFieldCount),
          Some(remainingNonEquiJoinPredicates))
      } else {
        val leftTimeAttributes = leftInput.getRowType.getFieldList
          .filter(f => f.getType.isInstanceOf[TimeIndicatorRelDataType])
        if (leftTimeAttributes.isEmpty) {
          throw new ValidationException(
            s"Missing timeAttribute in the left input of Processing-Time temporal join")
        }
        val leftTimeAttributeInputRef = leftTimeAttributes
          .map(f => f.getIndex)
          .head
        (leftTimeAttributeInputRef, None, None)
      }

    new StreamExecTemporalJoinToCoProcessTranslator(
      textualRepresentation,
      config,
      returnType,
      leftType,
      rightType,
      joinInfo,
      rexBuilder,
      leftTimeAttributeInputRef,
      rightTimeAttributeInputRef,
      remainingNonEquiJoinPredicates)
  }

  private def extractInputRef(rexNode: RexNode, textualRepresentation: String): Int = {
    val inputReferenceVisitor = new InputRefVisitor
    rexNode.accept(inputReferenceVisitor)
    checkState(
      inputReferenceVisitor.getFields.length == 1,
      "Failed to find input reference in [%s]",
      textualRepresentation)
    inputReferenceVisitor.getFields.head
  }

  private def extractInputRefs(rexNode: RexNode, textualRepresentation: String): Array[Int] = {
    val inputReferenceVisitor = new InputRefVisitor
    rexNode.accept(inputReferenceVisitor)
    checkState(
      inputReferenceVisitor.getFields.length == 1,
      "Failed to find input reference in [%s]",
      textualRepresentation)
    inputReferenceVisitor.getFields
  }

  private class TemporalJoinConditionExtractor(
    textualRepresentation: String,
    rightKeysStartingOffset: Int,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder)

    extends RexShuttle {

    var leftTimeAttribute: Option[RexNode] = None

    var rightTimeAttribute: Option[RexNode] = None

    var rightPrimaryKeyExpression: Option[Array[RexNode]] = None

    override def visitCall(call: RexCall): RexNode = {
      if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
        return super.visitCall(call)
      }
      leftTimeAttribute = Some(call.getOperands.get(0))
      rightTimeAttribute = Some(call.getOperands.get(1))

      rightPrimaryKeyExpression = Some(validateRightPrimaryKey(call.getOperands.get(2)))

      if (!isRowtimeIndicatorType(rightTimeAttribute.get.getType)) {
        throw new ValidationException(
          s"Non rowtime timeAttribute [${rightTimeAttribute.get.getType}] " +
            s"used in right input of Event-time temporal table join")
      }
      if (!isRowtimeIndicatorType(leftTimeAttribute.get.getType)) {
        throw new ValidationException(
          s"Non rowtime timeAttribute [${leftTimeAttribute.get.getType}] " +
            s"used in in left input of Event-time temporal table join")
      }
      rexBuilder.makeLiteral(true)
    }

    private def validateRightPrimaryKey(rightPrimaryKey: RexNode): Array[RexNode]  = {
      if (!rightPrimaryKey.isInstanceOf[RexCall] ||
        rightPrimaryKey.asInstanceOf[RexCall].getOperator != TEMPORAL_JOIN_CONDITION_PRIMARY_KEY) {
        throw new ValidationException(
          s"Non primary key [${rightPrimaryKey.asInstanceOf[RexCall]}] " +
            s"defined in right input of Event-time temporal table join")
       }

      val rightJoinKeyInputRefs = joinInfo.rightKeys
        .map(index => index + rightKeysStartingOffset)
        .toArray

      val rightPrimaryKeyInputRefs = extractInputRefs(
        rightPrimaryKey,
        textualRepresentation)

      val primaryKeyContainedInJoinKey = rightPrimaryKeyInputRefs.zipWithIndex
        .map(r => (r._1, rightJoinKeyInputRefs(r._2)))
        .forall(r => r._1 == r._2)
      if (!primaryKeyContainedInJoinKey) {
        throw new ValidationException(
          s"Join key [$rightJoinKeyInputRefs] must be the same as " +
            s"temporal table's primary key [$rightPrimaryKeyInputRefs] " +
            s"in [$textualRepresentation]")
      }
      rightPrimaryKey.asInstanceOf[RexCall].getOperands
        .asScala.toArray
     }
  }
}
