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
import org.apache.flink.table.planner.plan.utils.{JoinUtil, KeySelectorUtil}
import org.apache.flink.table.runtime.operators.window.join.WindowJoinOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.{SqlTypeFamily, SqlTypeName}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.Litmus

import java.time.Duration
import java.util

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
    input1: RelNode,
    input2: RelNode,
    winCall1: RexCall, // call of the left window table function
    winCall2: RexCall, // call of the right window table function
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, input1, input2, condition, joinType)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = true

  override def deriveRowType(): RelDataType = {
    // patch up the LHS and RHS with window attributes type
    val typeFactory = getCluster.getTypeFactory
    val timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP)
    val newLeftType = typeFactory
        .builder()
        .addAll(left.getRowType.getFieldList)
        .add("window_start", timestampType)
        .add("window_end", timestampType)
        .build()
    val newRightType = typeFactory
        .builder()
        .addAll(right.getRowType.getFieldList)
        .add("window_start", timestampType)
        .add("window_end", timestampType)
        .build()
    SqlValidatorUtil.deriveJoinRowType(
      newLeftType,
      newRightType,
      joinType,
      typeFactory,
      null,
      getSystemFieldList)
  }

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = {
    var fieldCnt = 4 + getSystemFieldList.size() + left.getRowType.getFieldCount
    if (joinType.projectsRight()) {
      fieldCnt += right.getRowType.getFieldCount
    }
    if (getRowType.getFieldCount != fieldCnt) {
      return litmus.fail("field count mismatch");
    }
    if (condition != null) {
      if (condition.getType.getSqlTypeName != SqlTypeName.BOOLEAN) {
        return litmus.fail("condition must be boolean: {}",
          condition.getType);
      }
      // The input to the condition is a row type consisting of system
      // fields, left fields, and right fields. Very similar to the
      // output row type, except that fields have not yet been made due
      // due to outer joins.
//      val checker =
//          new RexChecker(
//            getCluster.getTypeFactory.builder()
//                .addAll(getSystemFieldList)
//                .addAll(getLeft.getRowType.getFieldList)
//                .addAll(getRight.getRowType.getFieldList)
//                .build(),
//            context, litmus);
//      condition.accept(checker);
//      if (checker.getFailureCount > 0) {
//        return litmus.fail(checker.getFailureCount
//            + " failures in condition " + condition);
//      }
    }
    litmus.succeed();
  }

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamExecWindowJoin(cluster, traitSet, left, right,
      winCall1, winCall2, conditionExpr, joinType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .item("winCall", winCall2)
      .item("leftInputSpec", analyzeJoinInput(left))
      .item("rightInputSpec", analyzeJoinInput(right))
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

    val leftInputSpec = analyzeJoinInput(left)
    val rightInputSpec = analyzeJoinInput(right)

    val generatedCondition = JoinUtil.generateConditionFunction(
      tableConfig,
      cluster.getRexBuilder,
      getJoinInfo,
      leftType.toRowType,
      rightType.toRowType)

    val builder = WindowJoinOperator.builder()
      .inputType(leftType, rightType)
      .joinCondition(generatedCondition)
      .joinInputSpec(leftInputSpec, rightInputSpec)
      .filterNullKeys(filterNulls:_*)
      .joinType(flinkJoinType)

    setUpWindow(builder, winCall1, winCall2)

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
    val timeCol1 = winCall1.getOperands.get(0)
      .asInstanceOf[RexCall].getOperands.get(0).asInstanceOf[RexInputRef].getIndex
    val timeCol2 = winCall2.getOperands.get(0)
      .asInstanceOf[RexCall].getOperands.get(0).asInstanceOf[RexInputRef].getIndex
    winCall1.getKind match {
      case SqlKind.TUMBLE =>
        val size = getOperandAsLong(winCall1, 1)
        builder.tumble(Duration.ofMillis(size))
      case SqlKind.HOP =>
        val slide = getOperandAsLong(winCall1, 1)
        val size = getOperandAsLong(winCall1, 2)
        builder.sliding(Duration.ofMillis(size), Duration.ofMillis(slide))
      case SqlKind.SESSION =>
        val size = getOperandAsLong(winCall1, 1)
        builder.session(Duration.ofMillis(size))
     // TODO: process time time-attributes.
     builder.eventTime(timeCol1, timeCol2)
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
}
