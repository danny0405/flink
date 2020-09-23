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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;

public class FilterWindowFunctionScanTransposeRule extends RelOptRule {
	public static RelOptRule INSTANCE = new FilterWindowFunctionScanTransposeRule();

	public FilterWindowFunctionScanTransposeRule() {
		super(operand(LogicalFilter.class,
				operand(LogicalTableFunctionScan.class, any())),
				"FilterWindowFunctionScanTransposeRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalFilter filter = call.rel(0);
		LogicalTableFunctionScan scan = call.rel(1);
		int fieldCnt = scan.getRowType().getFieldCount();
		return WindowJoinUtils.isWindowFunctionCall(scan.getCall())
				&& !FlinkRexUtil.containsInputRef(
						filter.getCondition(),
				        Sets.newHashSet(fieldCnt - 2, fieldCnt - 1));
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final RelBuilder builder = call.builder();
		final LogicalFilter oriFilter = call.rel(0);
		final LogicalTableFunctionScan scan = call.rel(1);
		RelNode newInput = builder
				.push(scan.getInput(0))
				.filter(oriFilter.getCondition())
				.build();
		RelNode newNode = scan.copy(
				scan.getTraitSet(),
				Collections.singletonList(newInput));
		call.transformTo(newNode);
	}
}
