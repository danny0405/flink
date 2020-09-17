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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.utils.{JoinTypeUtil, JoinUtil, KeySelectorUtil}
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.mapping.IntPair

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Base physical class for flink [[Join]].
  */
abstract class CommonPhysicalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends Join(cluster, traitSet, Collections.emptyList(), leftRel, rightRel, condition,
    Set.empty[CorrelationId], joinType)
  with FlinkPhysicalRel {

  if (containsPythonCall(condition)) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  def getJoinInfo: JoinInfo = joinInfo

  lazy val filterNulls: Array[Boolean] = {
    val filterNulls = new util.ArrayList[java.lang.Boolean]
    JoinUtil.createJoinInfo(getLeft, getRight, getCondition, filterNulls)
    filterNulls.map(_.booleanValue()).toArray
  }

  lazy val keyPairs: List[IntPair] = getJoinInfo.pairs.toList

  // TODO remove FlinkJoinType
  lazy val flinkJoinType: FlinkJoinType = JoinTypeUtil.getFlinkJoinType(this.getJoinType)

  lazy val inputRowType: RelDataType = joinType match {
    case JoinRelType.SEMI | JoinRelType.ANTI =>
      // Combines inputs' RowType, the result is different from SEMI/ANTI Join's RowType.
      SqlValidatorUtil.createJoinType(
        getCluster.getTypeFactory,
        getLeft.getRowType,
        getRight.getRowType,
        null,
        Collections.emptyList[RelDataTypeField]
      )
    case _ =>
      getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("left", getLeft).input("right", getRight)
      .item("joinType", flinkJoinType.toString)
      .item("where", getExpressionString(
        getCondition, inputRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  def inputUniqueKeyContainsJoinKey(inputOrdinal: Int): Boolean = {
    val input = getInput(inputOrdinal)
    val inputUniqueKeys = getCluster.getMetadataQuery.getUniqueKeys(input)
    if (inputUniqueKeys != null) {
      val joinKeys = if (inputOrdinal == 0) {
        // left input
        keyPairs.map(_.source).toArray
      } else {
        // right input
        keyPairs.map(_.target).toArray
      }
      inputUniqueKeys.exists {
        uniqueKey => joinKeys.forall(uniqueKey.toArray.contains(_))
      }
    } else {
      false
    }
  }

  protected def analyzeJoinInput(input: RelNode): JoinInputSideSpec = {
    val uniqueKeys = cluster.getMetadataQuery.getUniqueKeys(input)
    if (uniqueKeys == null || uniqueKeys.isEmpty) {
      JoinInputSideSpec.withoutUniqueKey()
    } else {
      val inRowType = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(input.getRowType))
      val joinKeys = if (input == left) {
        keyPairs.map(_.source).toArray
      } else {
        keyPairs.map(_.target).toArray
      }
      val uniqueKeysContainedByJoinKey = uniqueKeys
        .filter(uk => uk.toArray.forall(joinKeys.contains(_)))
        .map(_.toArray)
        .toArray
      if (uniqueKeysContainedByJoinKey.nonEmpty) {
        // join key contains unique key
        val smallestUniqueKey = getSmallestKey(uniqueKeysContainedByJoinKey)
        val uniqueKeySelector = KeySelectorUtil.getRowDataSelector(smallestUniqueKey, inRowType)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(uniqueKeyTypeInfo, uniqueKeySelector)
      } else {
        val smallestUniqueKey = getSmallestKey(uniqueKeys.map(_.toArray).toArray)
        val uniqueKeySelector = KeySelectorUtil.getRowDataSelector(smallestUniqueKey, inRowType)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKey(uniqueKeyTypeInfo, uniqueKeySelector)
      }
    }
  }

  protected def getSmallestKey(keys: Array[Array[Int]]): Array[Int] = {
    var smallest = keys.head
    for (key <- keys) {
      if (key.length < smallest.length) {
        smallest = key
      }
    }
    smallest
  }
}
