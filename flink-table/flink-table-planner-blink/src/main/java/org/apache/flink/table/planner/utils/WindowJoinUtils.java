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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.runtime.operators.window.join.WindowAttribute;

import com.google.common.collect.Sets;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils for window-join. */
public class WindowJoinUtils {
	private static final String WINDOW_ATTR_ERROR_MSG = "Window attributes can only be "
			+ "in the end of the projection list, "
			+ "supported attributes permutations: "
			+ "[window_start, window_end], [window_start], [window_end]";

	private WindowJoinUtils() {}

	public static WindowAttribute validateAndGetWindowAttr(
			RexProgram program,
			int windowStartRef,
			int windowEndRef) {
		List<RexLocalRef> localRefs = program.getProjectList();
		Set<Integer> windowAttrSet = Sets.newHashSet(windowStartRef, windowEndRef);
		assert localRefs.size() > 0;
		for (int i = 0; i < localRefs.size() - 2; i++) {
			RexNode expanded = program.expandLocalRef(localRefs.get(i));
			if (FlinkRexUtil.containsInputRef(expanded, windowAttrSet)) {
				throw new TableException(WINDOW_ATTR_ERROR_MSG);
			}
		}
		if (localRefs.size() == 1) {
			RexNode expanded = program.expandLocalRef(localRefs.get(0));
			if (expanded instanceof RexInputRef) {
				RexInputRef inputRef = (RexInputRef) expanded;
				return getWindowAttributeByRef(inputRef, windowStartRef, windowEndRef);
			} else if (FlinkRexUtil.containsInputRef(expanded, windowAttrSet)) {
				throw new TableException(WINDOW_ATTR_ERROR_MSG);
			} else {
				return WindowAttribute.NONE;
			}
		} else {
			RexNode node1 = program.expandLocalRef(localRefs.get(localRefs.size() - 2));
			RexNode node2 = program.expandLocalRef(localRefs.get(localRefs.size() - 1));
			WindowAttribute attr1 = null;
			WindowAttribute attr2 = null;
			if (node1 instanceof RexInputRef) {
				RexInputRef inputRef = (RexInputRef) node1;
				attr1 = getWindowAttributeByRef(inputRef, windowStartRef, windowEndRef);
			}
			if (node2 instanceof RexInputRef) {
				RexInputRef inputRef = (RexInputRef) node2;
				attr2 = getWindowAttributeByRef(inputRef, windowStartRef, windowEndRef);
			}
			if (attr1 == null && FlinkRexUtil.containsInputRef(node1, windowAttrSet)
					|| attr2 == null && FlinkRexUtil.containsInputRef(node2, windowAttrSet)) {
				throw new TableException(WINDOW_ATTR_ERROR_MSG);
			}
			if ((attr1 == null || attr1 == WindowAttribute.NONE)) {
				return attr2 == null ? WindowAttribute.NONE : attr2;
			} else if (attr1 == WindowAttribute.START && attr2 == WindowAttribute.END) {
				return WindowAttribute.START_END;
			} else {
				throw new TableException(WINDOW_ATTR_ERROR_MSG);
			}
		}
	}

	public static RelDataType getPushedRowType(
			RelDataTypeFactory typeFactory,
			RelDataType oriRowType,
			WindowAttribute windowAttr) {
		List<RelDataTypeField> fields = new ArrayList<>(oriRowType.getFieldList());
		switch (windowAttr) {
		case NONE:
			return oriRowType;
		case START:
		case END:
			return typeFactory.builder()
					.addAll(fields.subList(0, fields.size() - 1))
					.build();
		case START_END:
			return typeFactory.builder()
					.addAll(fields.subList(0, fields.size() - 2))
					.build();
		default:
			throw new IllegalArgumentException("Unexpected window attribute kind: " + windowAttr);
		}
	}

	public static void validateCondition(
			RexNode condition,
			int windowStartRef,
			int windowEndRef) {
		if (condition != null
				&& FlinkRexUtil.containsInputRef(condition,
				Sets.newHashSet(windowStartRef, windowEndRef))) {
			throw new TableException("Filter condition of window table " +
					"function can not contains window attributes");
		}
	}

	public static List<RexNode> getPushedExprs(
			RexProgram program,
			int windowStartRef,
			int windowEndRef) {
		return program.getExprList().stream().filter(expr -> {
			if (expr instanceof RexInputRef) {
				RexInputRef inputRef = (RexInputRef) expr;
				return inputRef.getIndex() != windowStartRef
						&& inputRef.getIndex() != windowEndRef;
			}
			return true;
		}).collect(Collectors.toList());
	}

	public static List<RexNode> getPushedProjects(RexProgram program, WindowAttribute windowAttr) {
		List<RexNode> oriProjects = program.getProjectList().stream()
				.map(program::expandLocalRef)
				.collect(Collectors.toList());
		switch (windowAttr) {
		case NONE:
			return oriProjects;
		case START:
		case END:
			return oriProjects.subList(0, oriProjects.size() - 1);
		case START_END:
			return oriProjects.subList(0, oriProjects.size() - 2);
		default:
			throw new IllegalArgumentException("Unexpected window attribute kind: " + windowAttr);
		}
	}

	private static WindowAttribute getWindowAttributeByRef(
			RexInputRef inputRef,
			int windowStartRef,
			int windowEndRef) {
		if (inputRef.getIndex() == windowStartRef) {
			return WindowAttribute.START;
		} else if (inputRef.getIndex() == windowEndRef) {
			return WindowAttribute.END;
		} else {
			return WindowAttribute.NONE;
		}
	}

	public static RelNode trimPlannerNodes(RelNode node) {
		if (node instanceof RelSubset) {
			RelSubset subset = (RelSubset) node;
			return subset.getOriginal();
		} else if (node instanceof HepRelVertex) {
			return ((HepRelVertex) node).getCurrentRel();
		} else {
			return node;
		}
	}

	public static boolean hasWindowFunctionScan(RelNode node) {
		RelNode trimmed = trimPlannerNodes(node);
		if (trimmed instanceof TableFunctionScan) {
			TableFunctionScan scan = (TableFunctionScan) trimmed;
			return isWindowFunctionCall(scan.getCall());
		}
		try {
			trimmed.accept(new RelShuttleImpl() {
				@Override
				public RelNode visit(TableFunctionScan scan) {
					if (isWindowFunctionCall(scan.getCall())) {
						throw Util.FoundOne.NULL;
					}
					return super.visit(scan);
				}

				@Override
				public RelNode visit(RelNode other) {
					RelNode trimmed = trimPlannerNodes(other);
					return super.visit(trimmed);
				}
			});
		} catch (Util.FoundOne e) {
			return true;
		}
		return false;
	}

	public static boolean isWindowFunctionCall(RexNode node) {
		if (node instanceof RexCall) {
			return ((RexCall) node).getOperator() instanceof SqlWindowTableFunction;
		}
		return false;
	}

	public static int getTimeColRef(RexNode node) {
		final RexCall call = (RexCall) node;
		final RexCall descCall = (RexCall) call.getOperands().get(0);
		assert descCall.getKind() == SqlKind.DESCRIPTOR;
		RexNode operand0 = descCall.getOperands().get(0);
		return ((RexInputRef) operand0).getIndex();
	}

	public static RexNode replaceTimeColRef(
			RexBuilder rexBuilder,
			RexNode node,
			RexInputRef newOperand) {
		final RexCall call = (RexCall) node;
		final RexCall descCall = (RexCall) call.getOperands().get(0);
		assert descCall.getKind() == SqlKind.DESCRIPTOR;
		assert descCall.getOperands().size() == 1;
		RexNode newDescCall = rexBuilder.makeCall(descCall.getOperator(), newOperand);
		List<RexNode> newOperands = new ArrayList<>(call.getOperands());
		newOperands.set(0, newDescCall);
		return rexBuilder.makeCall(call.getOperator(), newOperands);
	}

	public static RelDataType patchUpWindowAttrs(
			RelDataType rowType,
			RelDataTypeFactory factory) {
		return factory.builder()
				.addAll(rowType.getFieldList())
				.add("window_start", SqlTypeName.TIMESTAMP)
				.add("window_end", SqlTypeName.TIMESTAMP)
				.build();
	}

	public static RelDataType patchUpWindowAttrs(
			RelDataTypeFactory factory,
			RelDataType rowType,
			WindowAttribute windowAttr) {
		switch (windowAttr) {
		case NONE:
			return rowType;
		case START:
			return factory.builder()
					.addAll(rowType.getFieldList())
					.add("window_start", SqlTypeName.TIMESTAMP)
					.build();
		case END:
			return factory.builder()
					.addAll(rowType.getFieldList())
					.add("window_end", SqlTypeName.TIMESTAMP)
					.build();
		case START_END:
			return factory.builder()
					.addAll(rowType.getFieldList())
					.add("window_start", SqlTypeName.TIMESTAMP)
					.add("window_end", SqlTypeName.TIMESTAMP)
					.build();
		default:
			throw new IllegalArgumentException("Unexpected window attribute kind: " + windowAttr);
		}
	}
}
