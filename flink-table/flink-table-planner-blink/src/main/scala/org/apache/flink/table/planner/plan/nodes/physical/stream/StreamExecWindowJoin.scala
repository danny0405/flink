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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.TwoInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.JBigDecimal
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.plan.utils.{JoinUtil, KeySelectorUtil}
import org.apache.flink.table.planner.utils.WindowJoinUtils
import org.apache.flink.table.planner.utils.WindowJoinUtils.patchUpWindowAttrs
import org.apache.flink.table.runtime.operators.window.join.{WindowAttribute, WindowJoinOperator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Join, JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexChecker, RexInputRef, RexLiteral, RexNode, RexShuttle}
import org.apache.calcite.sql.`type`.{SqlOperandMetadata, SqlTypeFamily, SqlTypeName}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.sql.{SqlFunction, SqlKind}
import org.apache.calcite.util.Litmus

import java.time.Duration
import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for window [[Join]].
  *
  * A window join joins the elements of two streams that share a common key
  * and lie in the same window. These windows can be defined by using a window assigner
  * and are evaluated on elements from both of the streams.
  *
  * A window join expects to be used in the near real-time cases.
  */
class StreamExecWindowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    leftAttr: WindowAttribute,
    rightAttr: WindowAttribute,
    leftCall: RexCall, // call of the left window table function
    rightCall: RexCall, // call of the right window table function
    condition: RexNode,
    joinType: JoinRelType,
    isCopied: Boolean = false)
  extends CommonPhysicalJoin(cluster, traitSet, left, right, condition, joinType)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = true

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): StreamExecWindowJoin = {
    new StreamExecWindowJoin(cluster, traitSet, left, right, leftAttr, rightAttr,
      leftCall, rightCall, conditionExpr, joinType, isCopied = true)
  }

  def windowAttributeCnt(windowAttr: WindowAttribute): Int = {
    windowAttr match {
      case WindowAttribute.NONE => 0
      case WindowAttribute.START => 1
      case WindowAttribute.END => 1
      case WindowAttribute.START_END => 2
      case _ => 0
    }
  }

  private lazy val shiftCondition: RexNode = if (isCopied) {
    condition
  } else {
    {
      // 2 is the cnt of left window attributes: window_start, window_end
      val shift = windowAttributeCnt(leftAttr)
      val leftCnt = left.getRowType.getFieldCount + shift
      condition.accept(new RexShuttle {
        override def visitInputRef(inputRef: RexInputRef): RexNode = {
          if (inputRef.getIndex < leftCnt) {
            inputRef
          } else {
            new RexInputRef(inputRef.getIndex - shift, inputRef.getType)
          }
        }
      })
    }
  }

  override lazy val inputRowType: RelDataType = {
    // Combines inputs' RowType, the result is different from SEMI/ANTI Join's RowType.
    SqlValidatorUtil.createJoinType(
      getCluster.getTypeFactory,
      getLeft.getRowType,
      getRight.getRowType,
      null,
      Collections.emptyList[RelDataTypeField]
    )
  }

  private lazy val shiftJoinInfo: JoinInfo = JoinInfo.of(left, right, shiftCondition)

  override def getCondition: RexNode = shiftCondition

  override def getJoinInfo: JoinInfo = shiftJoinInfo

  override def deriveRowType(): RelDataType = {
    // patch up the LHS and RHS with window attributes type
    val typeFactory = getCluster.getTypeFactory
    val newLeftType = WindowJoinUtils.patchUpWindowAttrs(typeFactory, left.getRowType, leftAttr)
    val newRightType = WindowJoinUtils.patchUpWindowAttrs(typeFactory, right.getRowType, rightAttr)
    SqlValidatorUtil.deriveJoinRowType(
      newLeftType,
      newRightType,
      joinType,
      typeFactory,
      null,
      Collections.emptyList())
  }

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = {
    var fieldCnt = left.getRowType.getFieldCount + windowAttributeCnt(leftAttr)
    if (joinType.projectsRight()) {
      fieldCnt += right.getRowType.getFieldCount
      fieldCnt += windowAttributeCnt(rightAttr)
    }
    if (getRowType.getFieldCount != fieldCnt) {
      return litmus.fail("field count mismatch");
    }
    if (condition != null) {
      if (condition.getType.getSqlTypeName != SqlTypeName.BOOLEAN) {
        return litmus.fail("condition must be boolean: {}",
          condition.getType);
      }
      val typeFactory = getCluster.getTypeFactory
      // The input to the condition is a row type consisting of system
      // fields, left fields, and right fields. Very similar to the
      // output row type, except that fields have not yet been made due
      // due to outer joins.
      val inputRowType = typeFactory.builder()
          .addAll(patchUpWindowAttrs(typeFactory, left.getRowType, leftAttr).getFieldList)
          .addAll(patchUpWindowAttrs(typeFactory, right.getRowType, rightAttr).getFieldList)
          .build()
      val checker =
        new RexChecker(
          inputRowType,
          context, litmus);
      condition.accept(checker);
      if (checker.getFailureCount > 0) {
        return litmus.fail(checker.getFailureCount
            + " failures in condition " + condition);
      }
    }
    litmus.succeed();
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("left", getLeft).input("right", getRight)
        .item("joinType", joinType.lowerName)
        .item("where", getExpressionString(
          getCondition, inputRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
        .item("window", explainWindowFunction(leftCall, rightCall))
        .item("inputSpec",
          s"[${JoinUtil.analyzeJoinInput(getCluster, getLeft, keyPairs, isLeft = true)}, " +
              s"${JoinUtil.analyzeJoinInput(getCluster, getRight, keyPairs, isLeft = false)}]")
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d * 2 // two input stream, now it's same with regular stream join
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
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

    val tableConfig = planner.getTableConfig
    val returnType = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val leftTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val rightTransform = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val leftType = leftTransform.getOutputType.asInstanceOf[InternalTypeInfo[RowData]]
    val rightType = rightTransform.getOutputType.asInstanceOf[InternalTypeInfo[RowData]]

    val (leftJoinKey, rightJoinKey) =
      JoinUtil.checkAndGetJoinKeys(keyPairs, getLeft, getRight, allowEmptyKey = true)

    val leftSelect = KeySelectorUtil.getRowDataSelector(leftJoinKey, leftType)
    val rightSelect = KeySelectorUtil.getRowDataSelector(rightJoinKey, rightType)

    val leftInputSpec = JoinUtil.analyzeJoinInput(getCluster, left, keyPairs, isLeft = true)
    val rightInputSpec = JoinUtil.analyzeJoinInput(getCluster, right, keyPairs, isLeft = false)

    val generatedCondition = JoinUtil.generateConditionFunction(
      tableConfig,
      cluster.getRexBuilder,
      analyzeCondition,
      leftType.toRowType,
      rightType.toRowType)

    val builder = WindowJoinOperator.builder()
      .windowAttribute(leftAttr, rightAttr)
      .inputType(leftType, rightType)
      .joinCondition(generatedCondition)
      .joinInputSpec(leftInputSpec, rightInputSpec)
      .filterNullKeys(filterNulls:_*)
      .joinType(flinkJoinType)

    setUpWindow(builder, leftCall, rightCall)

    val operator = builder.build()

    val trans = new TwoInputTransformation[RowData, RowData, RowData](
      leftTransform,
      rightTransform,
      getRelDetailedDescription,
      operator,
      returnType,
      leftTransform.getParallelism)

    if (inputsContainSingleton()) {
      trans.setParallelism(1)
      trans.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    trans.setStateKeySelectors(leftSelect, rightSelect)
    trans.setStateKeyType(leftSelect.getProducedType)
    trans
  }

  def setUpWindow(
      builder: WindowJoinOperator.Builder,
      winCall1: RexCall,
      winCall2: RexCall): Unit = {
    winCall1.getOperator.getName match {
      case SqlKind.TUMBLE.sql =>
        val size = getOperandAsLong(winCall1, 1)
        builder.tumble(Duration.ofMillis(size))
      case SqlKind.HOP.sql =>
        val slide = getOperandAsLong(winCall1, 1)
        val size = getOperandAsLong(winCall1, 2)
        builder.sliding(Duration.ofMillis(size), Duration.ofMillis(slide))
      case SqlKind.SESSION.sql =>
        val size = getOperandAsLong(winCall1, 1)
        builder.session(Duration.ofMillis(size))
      case _ =>
        throw new TableException("Unexpected window table function")
    }
    // Returns whether the window table function TIMECOL is event-time attribute.
    val isEventTimeAttribute: Boolean = FlinkTypeFactory
        .isRowtimeIndicatorType(
          winCall1.getOperands.get(0).asInstanceOf[RexCall].getOperands.get(0).getType)
    if (isEventTimeAttribute) {
      val timeCol1 = winCall1.getOperands.get(0)
          .asInstanceOf[RexCall].getOperands.get(0).asInstanceOf[RexInputRef].getIndex
      val timeCol2 = winCall2.getOperands.get(0)
          .asInstanceOf[RexCall].getOperands.get(0).asInstanceOf[RexInputRef].getIndex
      builder.eventTime(timeCol1, timeCol2)
    } else {
      builder.processingTime()
    }
  }

  // TODO: refactor it out
  def getOperandAsLong(call: RexCall, idx: Int): Long =
    call.getOperands.get(idx) match {
      case v: RexLiteral if v.getTypeName.getFamily == SqlTypeFamily.INTERVAL_DAY_TIME =>
        v.getValue.asInstanceOf[JBigDecimal].longValue()
      case _: RexLiteral => throw new TableException(
        "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. " +
          "MONTH and YEAR time unit are not supported yet.")
      case _ => throw new TableException("Only constant window descriptors are supported.")
    }

  /** Print the window function operands with param names, the left and right
   * window function expect to have the same window attributes, e.g. the kind of the window,
   * the window size.
   */
  protected def explainWindowFunction(call1: RexCall, call2: RexCall): String = {
    val builder: StringBuilder = new StringBuilder
    val leftTimeCol = call1.getOperands.get(0)
        .asInstanceOf[RexCall].getOperands.get(0).asInstanceOf[RexInputRef]
    val rightTimeCol = call2.getOperands.get(0)
        .asInstanceOf[RexCall].getOperands.get(0).asInstanceOf[RexInputRef]
    val paramNames = call1.getOperator.asInstanceOf[SqlFunction]
        .getOperandTypeChecker.asInstanceOf[SqlOperandMetadata]
        .paramNames()
    builder.append(call1.getOperator.getName)
        .append(s"(${paramNames(1)} => " +
            s"[${left.getRowType.getFieldNames.get(leftTimeCol.getIndex)}, " +
            s"${right.getRowType.getFieldNames.get(rightTimeCol.getIndex)}], ")
    call1.getOperands.zipWithIndex foreach {
      case (node, i) =>
        if (i != 0) {
          builder.append(s"${paramNames.get(i + 1)} => $node")
          if (i != call1.getOperands.length - 1) {
            builder.append(", ")
          }
        }
    }
    builder.append(")")
    builder.toString()
  }
}
