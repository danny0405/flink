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
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecWindowJoin
import org.apache.flink.table.planner.plan.utils.JoinUtil.toHashTraitByColumns
import org.apache.flink.table.planner.utils.WindowJoinUtils
import org.apache.flink.table.runtime.operators.window.join.WindowAttribute

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableFunctionScan
import org.apache.calcite.rex.{RexCall, RexNode, RexProgramBuilder}
import org.apache.calcite.tools.RelBuilder

/**
  * Rule that converts [[FlinkLogicalWindowJoin]] with window bounds in join condition
  * to [[StreamExecWindowJoin]].
  */
class StreamExecWindowJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamExecWindowJoinRule2") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0)
    val left: FlinkLogicalRel = call.rel(1)
    val right: FlinkLogicalRel = call.rel(2)
    val leftMatch = isMatchedInput(left)
    if (!leftMatch) {
      return false
    }
    val rightMatch = isMatchedInput(right)
    if (!rightMatch) {
      return false
    }
    if (!join.getJoinType.projectsRight()) {
      throw new ValidationException("Window function does not support SEMI or ANTI-SEMI join yet")
    }
    true
  }

  /** Decides whether the input rel is what desire to match. */
  private def isMatchedInput(rel: FlinkLogicalRel): Boolean = {
    rel match {
      case scan: FlinkLogicalTableFunctionScan =>
        WindowJoinUtils.isWindowFunctionCall(scan.getCall)
      case calc: FlinkLogicalCalc =>
        val scan = WindowJoinUtils.trimPlannerNodes(calc.getInput(0))
        scan match {
          case funcScan: TableFunctionScan =>
            WindowJoinUtils.isWindowFunctionCall(funcScan.getCall)
          case _ => false
        }
      case _ => false
    }
  }

  def resolveNewInput(
      relBuilder: RelBuilder,
      oriInput: FlinkLogicalRel)
  : (RelNode, RexNode, WindowAttribute) = oriInput match {
    case scan: FlinkLogicalTableFunctionScan =>
      (scan.getInput(0), scan.getCall, WindowAttribute.START_END)
    case calc: FlinkLogicalCalc =>
      val program = calc.getProgram
      val tableFunctionScan = WindowJoinUtils.trimPlannerNodes(calc.getInput)
          .asInstanceOf[TableFunctionScan]
      val scanFieldsCnt = tableFunctionScan.getRowType.getFieldCount
      val scanInput = tableFunctionScan.getInput(0)
      val windowStartRef = scanFieldsCnt - 2
      val windowEndRef = scanFieldsCnt - 1
      val windowAttr = WindowJoinUtils.validateAndGetWindowAttr(
        program, windowStartRef, windowEndRef)
      val condition = if (program.getCondition == null) {
        null
      } else {
        program.expandLocalRef(program.getCondition)
      }
      WindowJoinUtils.validateCondition(condition, windowStartRef, windowEndRef)
      val newProgram = RexProgramBuilder.create(
        relBuilder.getRexBuilder,
        scanInput.getRowType,
        WindowJoinUtils.getPushedExprs(program, windowStartRef, windowEndRef),
        WindowJoinUtils.getPushedProjects(program, windowAttr),
        condition,
        WindowJoinUtils.getPushedRowType(
          relBuilder.getTypeFactory,
          calc.getRowType,
          windowAttr),
        false,
        null)
          .getProgram
      val newNode = FlinkLogicalCalc.create(scanInput, newProgram)
      (newNode, tableFunctionScan.getCall, windowAttr)
    case _ => (null, null, null)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: FlinkLogicalJoin = call.rel(0)
    val left: FlinkLogicalRel = call.rel(1)
    val right: FlinkLogicalRel = call.rel(2)

    val joinInfo = join.analyzeCondition
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, right.getTraitSet))

    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    // resolve left window function
    val (newLeft0, leftCall, leftAttr) = resolveNewInput(call.builder(), left)
    if (leftAttr == null) {
      return
    }
    // resolve right window function
    val (newRight0, rightCall, rightAttr) = resolveNewInput(call.builder(), right)
    if (rightAttr == null) {
      return
    }

    val newLeft: RelNode = RelOptRule.convert(newLeft0, leftRequiredTrait)
    val newRight: RelNode = RelOptRule.convert(newRight0, rightRequiredTrait)

    val newJoin = new StreamExecWindowJoin(
      join.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      leftAttr,
      rightAttr,
      leftCall.asInstanceOf[RexCall],
      rightCall.asInstanceOf[RexCall],
      join.getCondition,
      join.getJoinType)

    call.transformTo(newJoin)
  }
}

object StreamExecWindowJoinRule {
  val INSTANCE: RelOptRule = new StreamExecWindowJoinRule
}