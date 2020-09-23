/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.WindowJoinUtils;

import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProjectWindowFunctionScanTransposeRule
		extends RelOptRule
		implements TransformationRule {
	public static RelOptRule INSTANCE = new ProjectWindowFunctionScanTransposeRule();

	public ProjectWindowFunctionScanTransposeRule() {
		super(operand(LogicalProject.class,
				operand(LogicalTableFunctionScan.class, any())),
				"ProjectWindowFunctionScanTransposeRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalTableFunctionScan scan = call.rel(1);
		return WindowJoinUtils.isWindowFunctionCall(scan.getCall());
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final RelBuilder builder = call.builder();
		final RexBuilder rexBuilder = builder.getRexBuilder();
		final LogicalProject oriProject = call.rel(0);
		final LogicalTableFunctionScan scan = call.rel(1);
		if (isTrivial(oriProject)) {
			call.transformTo(scan);
			return;
		}
		int fieldCnt = scan.getRowType().getFieldCount();
		final Set<Integer> windowRefs = Sets.newHashSet(fieldCnt - 2, fieldCnt - 1);
		List<RexNode> oriProjects = oriProject.getProjects();
		int oriTimeColIdx = WindowJoinUtils.getTimeColRef(scan.getCall());
		int newTimeColIdx = -1;
		Map<Integer, RexNode> reservedNodes = new HashMap<>();
		List<RexNode> pushedNodes = new ArrayList<>();
		for (int i = 0; i < oriProjects.size(); i++) {
			RexNode node = oriProjects.get(i);
			if (FlinkRexUtil.containsInputRef(node, windowRefs)) {
				// The reserved nodes expect to only have window attributes.
				reservedNodes.put(i, node);
			} else {
				if (node instanceof RexInputRef
						&& ((RexInputRef) node).getIndex() == oriTimeColIdx) {
					newTimeColIdx = pushedNodes.size();
				}
				pushedNodes.add(node);
			}
		}
		// keep the time col.
		if (newTimeColIdx == -1) {
			return;
		}
		int windowAttrShift = pushedNodes.size() - scan.getInput(0).getRowType().getFieldCount();
		RelNode newScanInput = builder.push(scan.getInput(0))
				.project(pushedNodes)
				.build();
		RelNode newScan = scan.copy(
				scan.getTraitSet(),
				Collections.singletonList(newScanInput),
				WindowJoinUtils.replaceTimeColRef(
						rexBuilder,
						scan.getCall(),
						rexBuilder.makeInputRef(newScanInput, newTimeColIdx)),
				scan.getElementType(),
				WindowJoinUtils.patchUpWindowAttrs(
						newScanInput.getRowType(),
						builder.getTypeFactory()),
				scan.getColumnMappings());
		List<RexNode> newProjects = new ArrayList<>();
		int j = 0;
		for (int i = 0; i < oriProjects.size(); i++) {
			if (reservedNodes.containsKey(i)) {
				newProjects.add(RexUtil.shift(reservedNodes.get(i), windowAttrShift));
			} else {
				newProjects.add(rexBuilder.makeInputRef(newScanInput, j));
				j++;
			}
		}
		RelNode newNode = builder.push(newScan)
				.project(newProjects)
				.build();
		call.transformTo(newNode);
	}

	private static boolean isTrivial(Project project) {
		return RexUtil.isIdentity(project.getProjects(),
				project.getInput().getRowType());
	}
}
