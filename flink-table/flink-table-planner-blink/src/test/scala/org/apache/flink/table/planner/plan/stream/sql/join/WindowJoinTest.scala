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

package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

class WindowJoinTest extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()

  util.addTable(
    s"""create table L(
        |  l0 int,
        |  lts timestamp(3),
        |  l2 varchar(20),
        |  watermark for lts as lts - INTERVAL '5' SECOND
        |) with (
        |  'connector' = 'values'
        |)
       |""".stripMargin)

  util.addTable(
    s"""create table R(
       |  r0 int,
       |  rts timestamp(3),
       |  r2 varchar(20),
       |  watermark for rts as rts - INTERVAL '5' SECOND
       |) with (
       |  'connector' = 'values'
       |)
       |""".stripMargin)

  util.addTable(
    s"""create table S(
       |  s0 int primary key not enforced,
       |  sts timestamp(3),
       |  s2 varchar(20),
       |  watermark for sts as sts - INTERVAL '5' SECOND
       |) with (
       |  'connector' = 'values'
       |)
       |""".stripMargin)

  util.addTable(
    s"""create table M(
       |  m0 int primary key not enforced,
       |  mts timestamp(3),
       |  m2 varchar(20),
       |  watermark for mts as mts - INTERVAL '5' SECOND
       |) with (
       |  'connector' = 'values'
       |)
       |""".stripMargin)

  @Test
  def testInnerJoin(): Unit = {
    val query =
      s"""
         |select l0, r2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l0 = b.r0
         |""".stripMargin
    val query1 =
      s"""
         |select l0, r2 from
         |(select l0, lts from table(tumble(table L, descriptor(lts), interval '10' second))) a
         |join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l0 = b.r0
         |""".stripMargin
    util.verifyPlan(query1)
  }

  @Test
  def testInnerJoinWithProjectInput(): Unit = {
    val query =
      s"""
         |select l0, r2 from
         |(select l0, lts from table(tumble(table L, descriptor(lts), interval '10' second))) a
         |join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l0 = b.r0
         |""".stripMargin
    util.verifyPlan(query)
  }

  // TODO:
  //  1. test different window functions
  //  2. test join inputs with window attributes
  //  3. test negative cases

  @Test
  def testInnerJoinWithPk(): Unit = {
    val query =
      s"""
         |select l0, s2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |join
         |table(tumble(table S, descriptor(sts), interval '10' second)) b
         |on a.l0 = b.s0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinNonEqui(): Unit = {
    val query =
      s"""
         |select l0, s2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |left join
         |table(tumble(table S, descriptor(sts), interval '10' second)) b
         |on a.l0 = b.s0 and a.l2 <> b.s2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinWithEqualPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s0 = b.m0 and a.s2 <> b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinWithRightNotPkNonEqui(): Unit = {
    val query =
      s"""
         |select l0, m2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.l0 = b.m0 and a.l2 <> b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinWithPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2 and a.s0 <> b.m0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoin(): Unit = {
    val query =
      s"""
         |select l0, r2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |left join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l2 = b.r2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinWithEqualPk(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s0 = b.m0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinWithRightNotPk(): Unit = {
    val query =
      s"""
         |select s0, l2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table L, descriptor(lts), interval '10' second)) b
         |on a.s0 = b.l0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftJoinWithPk(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinNonEqui(): Unit = {
    val query =
      s"""
         |select l0, r2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |right join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l0 = b.r0 and a.l2 <> b.r2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinWithEqualPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s0 = b.m0 and a.s2 <> b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinWithRightNotPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, l2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table L, descriptor(lts), interval '10' second)) b
         |on a.s0 = b.l0 and a.s2 <> b.l2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinWithPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2 and a.s0 <> b.m0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoin(): Unit = {
    val query =
      s"""
         |select l0, r2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |right join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l2 = b.r2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinWithEqualPk(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s0 = b.m0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinWithRightNotPk(): Unit = {
    val query =
      s"""
         |select s0, l2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table L, descriptor(lts), interval '10' second)) b
         |on a.s0 = b.l0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightJoinWithPk(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    val query =
      s"""
         |select l0, r2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |full join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l0 = b.r0 and a.l2 <> b.r2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoinWithEqualPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s0 = b.m0 and a.s2 <> b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoinWithFullNotPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |full join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s0 = b.m0 and a.s2 <> b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoinWithPkNonEqui(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |full join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2 and a.s0 <> b.m0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoin(): Unit = {
    val query =
      s"""
         |select l0, r2 from
         |table(tumble(table L, descriptor(lts), interval '10' second)) a
         |full join
         |table(tumble(table R, descriptor(rts), interval '10' second)) b
         |on a.l2 = b.r2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoinWithEqualPk(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |full join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s0 = b.m0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoinWithFullNotPk(): Unit = {
    val query =
      s"""
         |select s0, l2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |full join
         |table(tumble(table L, descriptor(lts), interval '10' second)) b
         |on a.s0 = b.l0
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testFullJoinWithPk(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |full join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testSelfJoinPlan(): Unit = {
    val query =
      s"""
         |select a.s0, b.s2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table S, descriptor(sts), interval '10' second)) b
         |on a.s0 = b.s0 and a.s2 > 3
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testJoinWithSort(): Unit = {
    val query =
      s"""
         |select s0, m2 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |full join
         |table(tumble((select * from M order by m0 ASC, m2 DESC), descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2
         |""".stripMargin

    util.verifyPlan(query)
  }

  @Test
  def testLeftOuterJoinEquiPred(): Unit = {
    val query =
      s"""
         |select s0, m0 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftOuterJoinEquiAndLocalPred(): Unit = {
    val query =
      s"""
         |select s0, m0 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2 and s0 > 5
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testLeftOuterJoinEquiAndNonEquiPred(): Unit = {
    val query =
      s"""
         |select s0, m0 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |left join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2 and s0 <> m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightOuterJoinEquiPred(): Unit = {
    val query =
      s"""
         |select s0, m0 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightOuterJoinEquiAndLocalPred(): Unit = {
    val query =
      s"""
         |select s0, m0 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2 and s0 > 5
         |""".stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testRightOuterJoinEquiAndNonEquiPred(): Unit = {
    val query =
      s"""
         |select s0, m0 from
         |table(tumble(table S, descriptor(sts), interval '10' second)) a
         |right join
         |table(tumble(table M, descriptor(mts), interval '10' second)) b
         |on a.s2 = b.m2 and s0 <> m2
         |""".stripMargin
    util.verifyPlan(query)
  }
}
