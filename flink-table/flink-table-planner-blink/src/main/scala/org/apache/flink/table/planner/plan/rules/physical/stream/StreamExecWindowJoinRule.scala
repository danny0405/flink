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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.JList
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecWindowJoin
import org.apache.flink.table.planner.plan.utils.JoinUtil.toHashTraitByColumns

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexLocalRef, RexNode, RexProgram, RexProgramBuilder, RexShuttle, RexUtil}
import org.apache.calcite.sql.SqlWindowTableFunction
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalJoin]] with window bounds in join condition
  * to [[StreamExecWindowJoin]].
  */
class StreamExecWindowJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalCalc],
        operand(classOf[FlinkLogicalTableFunctionScan], any())),
      operand(classOf[FlinkLogicalCalc],
        operand(classOf[FlinkLogicalTableFunctionScan], any()))),
    "StreamExecWindowJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0)
    if (!join.getJoinType.projectsRight()) {
      throw new ValidationException("Window function does not support SEMI or ANTI-SEMI join yet")
    }
    val left: FlinkLogicalTableFunctionScan = call.rel(2)
      .asInstanceOf[FlinkLogicalTableFunctionScan]
    val scanCall1: RexCall = left.getCall.asInstanceOf[RexCall]
    val right: FlinkLogicalTableFunctionScan = call.rel(4)
      .asInstanceOf[FlinkLogicalTableFunctionScan]
    val scanCall2: RexCall = right.getCall.asInstanceOf[RexCall]

    if (!isWindowFunctionCall(scanCall1) || !isWindowFunctionCall(scanCall2)) {
      return false
    }

    if (!FlinkTypeFactory.isTimeIndicatorType(descriptorArgumentType(scanCall1)) ||
      !FlinkTypeFactory.isTimeIndicatorType(descriptorArgumentType(scanCall2))) {
      throw new ValidationException("The timeCol of the window function must be time attribute," +
        "e.g. you should define a watermark strategy based on the column")
    }

    if (!isSameWindowAttributes(scanCall1, scanCall2)) {
      throw new ValidationException("Only stream window of same attributes can be joined together")
    }

    true
  }

  def descriptorArgumentType(node: RexCall): RelDataType = {
    node.getOperands.get(0).asInstanceOf[RexCall].getOperands.get(0).getType
  }

  def isWindowFunctionCall(node: RexCall): Boolean = {
    node.getOperator.isInstanceOf[SqlWindowTableFunction]
  }

  def isSameWindowAttributes(call1: RexCall, call2: RexCall): Boolean = {
    if (call1.getKind != call2.getKind) {
      return false
    }
    val operands1: JList[RexNode] = call1.getOperands
    val operands2: JList[RexNode] = call2.getOperands
    if (operands1.size() != operands2.size()) {
      return false
    }
    // All the operands are the same except for the timeCol.
    // The timeCol is always the first operand.
    Util.skip(operands1) zip Util.skip(operands2) foreach {
      case (opd1: RexNode, opd2: RexNode) =>
        if (!opd1.equals(opd2)) {
          return false
        }
    }
    true
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val relBuilder: RelBuilder = call.builder()
    val rexBuilder: RexBuilder = relBuilder.getRexBuilder
    val join: FlinkLogicalJoin = call.rel(0)
    val leftCalc: FlinkLogicalCalc = call.rel(1)
    val leftFuncScan: FlinkLogicalTableFunctionScan = call.rel(2)
    val leftInput = leftFuncScan.getInput(0)
    val rightCalc: FlinkLogicalCalc = call.rel(3)
    val rightFuncScan: FlinkLogicalTableFunctionScan = call.rel(4)
    val rightInput = rightFuncScan.getInput(0)

    // -------------------------------------------------------------------------
    //  1. Push the calc filters into the join input
    // -------------------------------------------------------------------------

    val joinInfo = join.analyzeCondition()
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, leftInput.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, rightInput.getTraitSet))

    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val leftProgram = leftCalc.getProgram
    val rightProgram = rightCalc.getProgram
    val newLeftInput0: RelNode = if (leftProgram.getCondition != null) {
      relBuilder.push(leftInput)
          .filter(leftProgram.expandLocalRef(leftProgram.getCondition))
          .build()
    } else {
      leftInput
    }
    val newLeftInput: RelNode = RelOptRule.convert(newLeftInput0, leftRequiredTrait)
    val newRightInput0: RelNode = if (rightProgram.getCondition != null) {
      relBuilder.push(rightInput)
          .filter(rightProgram.expandLocalRef(rightProgram.getCondition))
          .build()
    } else {
      rightInput
    }
    val newRightInput: RelNode = RelOptRule.convert(newRightInput0, rightRequiredTrait)

    val newJoin = new StreamExecWindowJoin(
      join.getCluster,
      providedTraitSet,
      newLeftInput,
      newRightInput,
      leftFuncScan.getCall.asInstanceOf[RexCall],
      rightFuncScan.getCall.asInstanceOf[RexCall],
      shiftJoinCondition(join.getCondition, leftProgram, rightProgram,
        leftFuncScan.getRowType.getFieldCount),
      join.getJoinType)

    val newExprs = new java.util.ArrayList[RexNode](leftProgram.getExprList)
    val leftExprCnt = newExprs.size()
    val newProjectRefs = new java.util.ArrayList[RexLocalRef](leftProgram.getProjectList)
    val rightExprList = rightProgram.getExprList
    newExprs.addAll(RexUtil.shift(rightExprList, leftFuncScan.getRowType.getFieldCount))
    newProjectRefs.addAll(
      rightProgram.getProjectList
          .map(ref => new RexLocalRef(ref.getIndex + leftExprCnt, ref.getType)))

    // -------------------------------------------------------------------------
    //  2. Construct the final program
    // -------------------------------------------------------------------------
    val newProgram = RexProgramBuilder.create(
      rexBuilder,
      // Table function san row type has window attributes
      SqlValidatorUtil.createJoinType(
        relBuilder.getTypeFactory,
        leftFuncScan.getRowType,
        rightFuncScan.getRowType,
        null,
        Collections.emptyList[RelDataTypeField]),
      newExprs,
      newProjectRefs,
      null,
      // join type
      SqlValidatorUtil.createJoinType(
        relBuilder.getTypeFactory,
        leftCalc.getRowType,
        rightCalc.getRowType,
        null,
        Collections.emptyList[RelDataTypeField]),
      false,
      null
    ).getProgram

    val newLogicalNode = FlinkLogicalCalc.create(newJoin, newProgram)
    val newNode = RelOptRule.convert(newLogicalNode,
      newLogicalNode.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL))
    call.transformTo(newNode)
  }

  private def shiftJoinCondition(
      condition: RexNode,
      leftProgram: RexProgram,
      rightProgram: RexProgram,
      leftFields: Int): RexNode = {
    val leftProjects = leftProgram.getProjectList
    val rightProjects = rightProgram.getProjectList
    val leftCnt = leftProjects.length

    condition.accept(new RexShuttle() {
      override def visitInputRef(input: RexInputRef): RexNode = {
        val index = input.getIndex
        val program = if (index < leftCnt) { leftProgram } else { rightProgram }
        val projects = if (index < leftCnt) { leftProjects } else { rightProjects }
        // Shift the LHS Calc fields cnt.
        val shift0 = if (index < leftCnt) { 0 } else { -leftCnt }
        val item = program.expandLocalRef(projects.get(index + shift0))
        if (!item.isInstanceOf[RexInputRef]) {
          throw new ValidationException("Window join condition referencing " +
              "underlying calls is not supported yet")
        }
        // Shift the join LHS field cnt.
        val shift = if (index < leftCnt) { 0 } else { leftFields }
        new RexInputRef(item.asInstanceOf[RexInputRef].getIndex + shift, input.getType)
      }
    })
  }
}

object StreamExecWindowJoinRule {
  val INSTANCE: RelOptRule = new StreamExecWindowJoinRule
}
