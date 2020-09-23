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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.planner.JBigDecimal
import org.apache.flink.table.planner.expressions.utils.FuncWithOpen
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.util.CollectionUtil

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.time.LocalDateTime

import scala.collection.JavaConversions._
import scala.collection.{Seq, mutable}

@RunWith(classOf[Parameterized])
class WindowJoinITCase(state: StateBackendMode) extends StreamingWithStateTestBase(state) {

  private def prepareTableLR(): Unit = {
    // register table without primary keys
    // data set
    val dataL = List(
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 1, 1d, 1f, new JBigDecimal("1"), "Hi", "la"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:02"), 2, 2d, 2f, new JBigDecimal("2"), "Hallo", "la"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:03"), 2, 2d, 2f, new JBigDecimal("2"), "Hello", "la"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:04"), 5, 5d, 5f, new JBigDecimal("5"), "Hello", "la"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:07"), 3, 3d, 3f, new JBigDecimal("3"), "Hello", "lb"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:06"), 5, 5d, 5f, new JBigDecimal("5"), "Hello", "la"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:08"), 3, 3d, 3f, new JBigDecimal("3"), "Hello world", "la"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:16"), 4, 4d, 4f, new JBigDecimal("4"), "Hello world", "lb"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:16"), 8, 5d, 4f, new JBigDecimal("4"), null.asInstanceOf[String], "lb"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:32"), 9, 4d, 4f, new JBigDecimal("4"),
        null.asInstanceOf[String], null.asInstanceOf[String]))

    val dataR = List(
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 1, 1d, 1f, new JBigDecimal("1"), "Hi", "ra"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:02"), 2, 2d, 2f, new JBigDecimal("2"), "Hallo", "ra"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:03"), 3, 2d, 2f, new JBigDecimal("2"), "Hello", "ra"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:04"), 4, 5d, 5f, new JBigDecimal("5"), "Hello", "ra"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:07"), 5, 3d, 3f, new JBigDecimal("3"), "Hello", "rb"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:06"), 6, 5d, 5f, new JBigDecimal("5"), "Hello", "ra"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:08"), 7, 3d, 3f, new JBigDecimal("3"), "Hello world", "ra"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:16"), 8, 4d, 4f, new JBigDecimal("4"), "Hello world", "rb"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:16"), 4, 4d, 4f, new JBigDecimal("4"), null.asInstanceOf[String], "lb"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:32"), 9, 4d, 4f, new JBigDecimal("4"),
        null.asInstanceOf[String], null.asInstanceOf[String]))

    val lDataID = TestValuesTableFactory.registerData(dataL)
    val rDataID = TestValuesTableFactory.registerData(dataR)

    val createL =
      s"""
         |create table L(
         |  rowtime timestamp(3),
         |  f_int int,
         |  f_double double,
         |  f_float float,
         |  f_bigdec decimal(10, 0),
         |  f_string varchar(20),
         |  f_name varchar(20),
         |  watermark for rowtime as rowtime - interval '0.01' second
         |) with (
         |  'connector' = 'values',
         |  'data-id' = '$lDataID',
         |  'changelog-mode' = 'I'
         |)
         |""".stripMargin
    val createR =
      s"""
         |create table R(
         |  rowtime timestamp(3),
         |  f_int int,
         |  f_double double,
         |  f_float float,
         |  f_bigdec decimal(10, 0),
         |  f_string varchar(20),
         |  f_name varchar(20),
         |  watermark for rowtime as rowtime - interval '0.01' second
         |) with (
         |  'connector' = 'values',
         |  'data-id' = '$rDataID',
         |  'changelog-mode' = 'I'
         |)
         |""".stripMargin

    tEnv.executeSql(createL)
    tEnv.executeSql(createR)
  }

  private def prepareTableABC(): Unit = {
    // register table with primary keys
    // data set
    val dataA = List(
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 1, 2L, "a1"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:04"), 2, 3L, "a2"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:08"), 3, 4L, "a3"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:16"), 4, 5L, "a4"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:32"), 5, 6L, "a5"))

    val dataB = List(
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 1, 3L, "b1"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:04"), 2, 4L, "b2"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:08"), 3, 5L, "b3"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:16"), 4, 6L, "b4"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:32"), 5, 7L, "b5"))

    val aDataID = TestValuesTableFactory.registerData(dataA)
    val bDataID = TestValuesTableFactory.registerData(dataB)

    val createA =
      s"""create table A(
         |  ats timestamp(3),
         |  a_int int primary key not enforced,
         |  a_bigint bigint,
         |  a_varchar varchar(20),
         |  watermark for ats as ats
         |) with (
         |  'connector' = 'values',
         |  'data-id' = '$aDataID',
         |  'changelog-mode' = 'I'
         |)
         |""".stripMargin

    val createB =
      s"""
         |create table B(
         |  bts timestamp(3),
         |  b_int int primary key not enforced,
         |  b_bigint bigint,
         |  b_varchar varchar(20),
         |  watermark for bts as bts
         |) with (
         |  'connector' = 'values',
         |  'data-id' = '$bDataID',
         |  'changelog-mode' = 'I'
         |)
         |""".stripMargin

    val createC =
      s"""
         |create table C(
         |  cts timestamp(3),
         |  c_int int,
         |  c_bigint bigint,
         |  c_varchar varchar(20),
         |  watermark for cts as cts
         |) with (
         |  'connector' = 'values',
         |  'data-id' = '$aDataID',
         |  'changelog-mode' = 'I'
         |)
         |""".stripMargin
    tEnv.executeSql(createA)
    tEnv.executeSql(createB)
    tEnv.executeSql(createC) // same data with A but without PK
  }

  private def prepareTableDE(): Unit = {
    // register table with same timeCols and nulls as keys.
    // data set
    val dataD = List(
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 1, 1L, "a1"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 3, 8L, "a2"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 4, 2L, "a3"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), null, 5L, "a4"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), null, 6L, "a5"))

    val dataE = List(
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 1, 1L, "b1"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 2, 2L, "b2"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), 3, 2L, "b3"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), null, 6L, "b4"),
      rowOf(LocalDateTime.parse("1970-01-01T00:00:01"), null, 7L, "b5"))

    val dDataID = TestValuesTableFactory.registerData(dataD)
    val eDataID = TestValuesTableFactory.registerData(dataE)

    val createD =
      s"""create table D(
         |  dts timestamp(3),
         |  d_int int primary key not enforced,
         |  d_bigint bigint,
         |  d_varchar varchar(20),
         |  watermark for dts as dts
         |) with (
         |  'connector' = 'values',
         |  'data-id' = '$dDataID',
         |  'changelog-mode' = 'I'
         |)
         |""".stripMargin

    val createE =
      s"""
         |create table E(
         |  ets timestamp(3),
         |  e_int int primary key not enforced,
         |  e_bigint bigint,
         |  e_varchar varchar(20),
         |  watermark for ets as ets
         |) with (
         |  'connector' = 'values',
         |  'data-id' = '$eDataID',
         |  'changelog-mode' = 'I'
         |)
         |""".stripMargin
    tEnv.executeSql(createD)
    tEnv.executeSql(createE)
  }

  override def before(): Unit = {
    super.before()
    prepareTableLR()
    prepareTableABC()
    prepareTableDE()
  }

  // Tests for inner join.
  override def after(): Unit = {}

  /** test window inner join **/
  @Test
  def testWindowInnerJoin(): Unit = {
    val query =
      """
        |SELECT a.f_int, b.f_name, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,ra,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "4,lb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,rb,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,rb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,null,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testInnerJoinWithIsNullCond(): Unit = {
    val query =
      """
        |SELECT a.f_int, b.f_name, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on (a.f_string is null and b.f_string is null or a.f_string = b.f_string)
        |and (a.f_int > b.f_int)
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "5,ra,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "8,lb,1970-01-01T00:00:16,1970-01-01T00:00:18")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val query =
      """
        |SELECT a.f_int, a.f_double, b.f_name, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int where a.f_double > 3
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "4,4.0,lb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,5.0,rb,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,5.0,rb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,4.0,null,1970-01-01T00:00:32,1970-01-01T00:00:34")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val query =
      """
        |SELECT a.f_int, a.f_double, b.f_bigdec, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int and (a.f_double > 3 or b.f_bigdec < 2)
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1.0,1.000000000000000000,1970-01-01T00:00,1970-01-01T00:00:02",
      "4,4.0,4.000000000000000000,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,5.0,3.000000000000000000,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,5.0,4.000000000000000000,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,4.0,4.000000000000000000,1970-01-01T00:00:32,1970-01-01T00:00:34")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val query =
      """
        |SELECT a.f_int, b.f_name, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int and a.f_name = b.f_name
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "4,lb,1970-01-01T00:00:16,1970-01-01T00:00:18")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testJoinWithAggregationInput(): Unit = {
    // TODO: test join with agg input
  }

  @Test
  def testFullOuterJoin(): Unit = {
    val query =
      """
        |SELECT a.f_int, b.f_name, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |full outer join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,ra,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "3,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "3,null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,lb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "5,rb,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,rb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,null,1970-01-01T00:00:32,1970-01-01T00:00:34",
      "null,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "null,ra,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "null,ra,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "null,ra,1970-01-01T00:00:08,1970-01-01T00:00:10")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightOuterJoin(): Unit = {
    val query =
      """
        |SELECT a.f_int, b.f_name, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |right outer join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,ra,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "4,lb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,rb,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,rb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,null,1970-01-01T00:00:32,1970-01-01T00:00:34",
      "null,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "null,ra,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "null,ra,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "null,ra,1970-01-01T00:00:08,1970-01-01T00:00:10")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testInnerJoinWithEqualPk(): Unit = {
    val query =
      s"""
         |select ats, a_int, b_varchar
         |from
         |table(tumble(table A, descriptor(ats), interval '2' second)) aa
         |join
         |table(tumble(table B, descriptor(bts), interval '2' second)) bb
         |on aa.a_int = bb.b_int
         |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = List(
      "1970-01-01T00:00:01,1,b1",
      "1970-01-01T00:00:04,2,b2",
      "1970-01-01T00:00:08,3,b3",
      "1970-01-01T00:00:16,4,b4",
      "1970-01-01T00:00:32,5,b5")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testInnerJoinWithPk(): Unit = {
    val query =
      s"""
         |select ats, a_int, b_varchar
         |from
         |table(tumble(table A, descriptor(ats), interval '2' second)) aa
         |join
         |table(tumble(table B, descriptor(bts), interval '2' second)) bb
         |on aa.a_varchar= bb.b_varchar
         |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = Seq[String]()
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val query =
      """
        |SELECT a.f_int, b.f_name, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |left join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,ra,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "2,ra,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "3,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "3,null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,lb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "5,rb,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,rb,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,null,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftJoinNonEqui(): Unit = {
    val query =
      """
        |SELECT a.f_int, a.f_double, b.f_double, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |left join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int and a.f_double < b.f_double
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // a.f_double < b.f_double is always false
    val expected = mutable.MutableList(
      "1,1.0,null,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,2.0,null,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "2,2.0,null,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "3,3.0,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "3,3.0,null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,4.0,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,5.0,null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "5,5.0,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,5.0,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,4.0,null,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftJoinWithEqualPkNonEqui(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |left join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int = bb.b_int and aa.a_bigint < bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftJoinWithRightNotPkNonEqui(): Unit = {
    val query =
      """
        |SELECT bb.b_int, bb.b_bigint, cc.c_bigint, bb.window_start, bb.window_end
        |from
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |left join
        |table(tumble(table C, descriptor(cts), interval '2' second)) cc
        |on cc.c_int = bb.b_int and cc.c_bigint < bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,3,2,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,4,3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,5,4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,6,5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,7,6,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftJoinWithPkNonEqui(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |left join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int <> bb.b_int and aa.a_bigint = bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,null,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,null,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftJoinWithEqualPk(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |left join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int = bb.b_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftJoinWithRightNotPk(): Unit = {
    val query =
      """
        |SELECT bb.b_int, bb.b_bigint, cc.c_bigint, bb.window_start, bb.window_end
        |from
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |left join
        |table(tumble(table C, descriptor(cts), interval '2' second)) cc
        |on cc.c_int = bb.b_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,3,2,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,4,3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,5,4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,6,5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,7,6,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testLeftJoinWithPk(): Unit = {
    val query =
      """
        |SELECT aa.a_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |left join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_bigint = bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "2,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "6,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightJoinNonEqui(): Unit = {
    val query =
      """
        |SELECT b.f_int, b.f_double, a.f_double, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |right join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int and a.f_double < b.f_double
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // a.f_double < b.f_double is always false
    val expected = mutable.MutableList(
      "1,1.0,null,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,2.0,null,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "3,2.0,null,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "4,4.0,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "4,5.0,null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "5,3.0,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "6,5.0,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "7,3.0,null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "8,4.0,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,4.0,null,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightJoinWithEqualPkNonEqui(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |right join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int = bb.b_int and aa.a_bigint < bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightJoinWithRightNotPkNonEqui(): Unit = {
    val query =
      """
        |SELECT bb.b_int, bb.b_bigint, cc.c_bigint, bb.window_start, bb.window_end
        |from
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |right join
        |table(tumble(table C, descriptor(cts), interval '2' second)) cc
        |on cc.c_int = bb.b_int and cc.c_bigint < bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,3,2,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,4,3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,5,4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,6,5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,7,6,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightJoinWithPkNonEqui(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |right join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int <> bb.b_int and aa.a_bigint = bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "null,null,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,null,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "null,null,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "null,null,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "null,null,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightJoinWithEqualPk(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |right join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int = bb.b_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightJoinWithRightNotPk(): Unit = {
    val query =
      """
        |SELECT bb.b_int, bb.b_bigint, cc.c_bigint, bb.window_start, bb.window_end
        |from
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |left join
        |table(tumble(table C, descriptor(cts), interval '2' second)) cc
        |on cc.c_int = bb.b_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,3,2,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,4,3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,5,4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,6,5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,7,6,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRightJoinWithPk(): Unit = {
    val query =
      """
        |SELECT aa.a_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |right join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_bigint = bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "null,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "null,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    val query =
      """
        |SELECT a.f_int, a.f_double, b.f_double, a.window_start, a.window_end from
        |table(tumble(table L, descriptor(rowtime), interval '2' second)) a
        |full outer join
        |table(tumble(table R, descriptor(rowtime), interval '2' second)) b
        |on a.f_int = b.f_int and a.f_double < b.f_double
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // a.f_double < b.f_double is always false
    val expected = mutable.MutableList(
      "1,1.0,null,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,2.0,null,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "2,2.0,null,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "3,3.0,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "3,3.0,null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,4.0,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,5.0,null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "5,5.0,null,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "8,5.0,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "9,4.0,null,1970-01-01T00:00:32,1970-01-01T00:00:34",
      "null,null,1.0,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,null,2.0,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "null,null,2.0,1970-01-01T00:00:02,1970-01-01T00:00:04",
      "null,null,3.0,1970-01-01T00:00:06,1970-01-01T00:00:08",
      "null,null,3.0,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "null,null,4.0,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "null,null,4.0,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "null,null,4.0,1970-01-01T00:00:32,1970-01-01T00:00:34",
      "null,null,5.0,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "null,null,5.0,1970-01-01T00:00:06,1970-01-01T00:00:08")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullJoinWithEqualPkNonEqui(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |full outer join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int = bb.b_int and aa.a_bigint < bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullJoinWithFullNotPkNonEqui(): Unit = {
    val query =
      """
        |SELECT bb.b_int, bb.b_bigint, cc.c_bigint, bb.window_start, bb.window_end
        |from
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |full outer join
        |table(tumble(table C, descriptor(cts), interval '2' second)) cc
        |on cc.c_int = bb.b_int and cc.c_bigint < bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,3,2,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,4,3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,5,4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,6,5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,7,6,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullJoinWithPkNonEqui(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |full outer join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int <> bb.b_int and aa.a_bigint = bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,null,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,null,1970-01-01T00:00:32,1970-01-01T00:00:34",
      "null,null,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,null,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "null,null,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "null,null,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "null,null,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullJoinWithEqualPk(): Unit = {
    val query =
      """
        |SELECT aa.a_int, aa.a_bigint, bb.b_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |full outer join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_int = bb.b_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,2,3,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,3,4,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,4,5,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,5,6,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,6,7,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullJoinWithFullNotPk(): Unit = {
    val query =
      """
        |SELECT bb.b_int, bb.b_bigint, cc.c_bigint, bb.window_start, bb.window_end
        |from
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |full outer join
        |table(tumble(table C, descriptor(cts), interval '2' second)) cc
        |on cc.c_int = bb.b_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "1,3,2,1970-01-01T00:00,1970-01-01T00:00:02",
      "2,4,3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "3,5,4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "4,6,5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "5,7,6,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullJoinWithPk(): Unit = {
    val query =
      """
        |SELECT aa.a_bigint, aa.window_start, aa.window_end
        |from
        |table(tumble(table A, descriptor(ats), interval '2' second)) aa
        |full outer join
        |table(tumble(table B, descriptor(bts), interval '2' second)) bb
        |on aa.a_bigint = bb.b_bigint
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    // aa.a_bigint < bb.b_bigint is always true
    val expected = mutable.MutableList(
      "2,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "4,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "5,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "6,1970-01-01T00:00:32,1970-01-01T00:00:34",
      "null,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00:04,1970-01-01T00:00:06",
      "null,1970-01-01T00:00:08,1970-01-01T00:00:10",
      "null,1970-01-01T00:00:16,1970-01-01T00:00:18",
      "null,1970-01-01T00:00:32,1970-01-01T00:00:34")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testNullLeftOuterJoin(): Unit = {
    val query =
      """
        |SELECT dd.d_int, dd.window_start, dd.window_end
        |from
        |table(tumble(table D, descriptor(dts), interval '2' second)) dd
        |left outer join
        |table(tumble(table E, descriptor(ets), interval '2' second)) ee
        |on dd.d_int = ee.e_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02",
      "4,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testNullLeftOuterJoinWithNullCond(): Unit = {
    val query =
      """
        |SELECT dd.d_int, dd.window_start, dd.window_end
        |from
        |table(tumble(table D, descriptor(dts), interval '2' second)) dd
        |left outer join
        |table(tumble(table E, descriptor(ets), interval '2' second)) ee
        |on dd.d_int IS NOT DISTINCT FROM ee.e_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02",
      "4,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testNullRightOuterJoin(): Unit = {
    val query =
      """
        |SELECT dd.d_int, dd.window_start, dd.window_end
        |from
        |table(tumble(table D, descriptor(dts), interval '2' second)) dd
        |right outer join
        |table(tumble(table E, descriptor(ets), interval '2' second)) ee
        |on dd.d_int = ee.e_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testNullRightOuterJoinWithNullCond(): Unit = {
    val query =
      """
        |SELECT dd.d_int, dd.window_start, dd.window_end
        |from
        |table(tumble(table D, descriptor(dts), interval '2' second)) dd
        |right outer join
        |table(tumble(table E, descriptor(ets), interval '2' second)) ee
        |on dd.d_int IS NOT DISTINCT FROM ee.e_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testNullFullOuterJoin(): Unit = {
    val query =
      """
        |SELECT dd.d_int, dd.window_start, dd.window_end
        |from
        |table(tumble(table D, descriptor(dts), interval '2' second)) dd
        |full outer join
        |table(tumble(table E, descriptor(ets), interval '2' second)) ee
        |on dd.d_int = ee.e_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02",
      "4,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testNullFullOuterJoinWithNullCond(): Unit = {
    val query =
      """
        |SELECT dd.d_int, dd.window_start, dd.window_end
        |from
        |table(tumble(table D, descriptor(dts), interval '2' second)) dd
        |full outer join
        |table(tumble(table E, descriptor(ets), interval '2' second)) ee
        |on dd.d_int IS NOT DISTINCT FROM ee.e_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02",
      "4,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02",
      "null,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testJoinWithUDFFilter(): Unit = {

    tEnv.registerFunction("funcWithOpen", new FuncWithOpen)

    val query =
      """
        |SELECT dd.d_int, dd.window_start, dd.window_end
        |from
        |table(tumble(table D, descriptor(dts), interval '2' second)) dd
        |join
        |table(tumble(table E, descriptor(ets), interval '2' second)) ee
        |on funcWithOpen(dd.d_int + ee.e_int) and dd.d_int = ee.e_int
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testJoinOnChangelogSource(): Unit = {
    val orderDataId = TestValuesTableFactory.registerData(TestData.ordersData)
    val ratesDataId = TestValuesTableFactory.registerData(TestData.ratesHistoryData)
    expectedException.expect(classOf[UnsupportedOperationException])
    expectedException.expectMessage(
      "Currently, defining WATERMARK on a changelog source is not supported.")
    tEnv.executeSql(
      s"""
         |CREATE TABLE orders (
         |  amount BIGINT,
         |  currency STRING,
         |  ts as TIMESTAMP '1970-01-01 00:00:01',
         |  watermark for ts as ts
         |) WITH (
         | 'connector' = 'values',
         | 'data-id' = '$orderDataId',
         | 'changelog-mode' = 'I'
         |)
         |""".stripMargin)
    tEnv.executeSql(
      s"""
        |CREATE TABLE rates_history (
        |  currency STRING,
        |  rate BIGINT,
        |  ts as TIMESTAMP '1970-01-01 00:00:01',
        |  watermark for ts as ts
        |) WITH (
        |  'connector' = 'values',
        |  'data-id' = '$ratesDataId',
        |  'changelog-mode' = 'I,UB,UA'
        |)
      """.stripMargin)

    val query =
      """
        |SELECT o.currency, o.amount, r.rate, o.amount * r.rate
        |FROM
        |table(tumble(table orders, descriptor(ts), interval '2' second)) AS o
        |JOIN
        |table(tumble(table rates_history, descriptor(ts), interval '2' second)) AS r
        |ON o.currency = r.currency
        |""".stripMargin

    val result = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect())
        .map(row => row.toString).toList

    val expected = mutable.MutableList(
      "1,1970-01-01T00:00,1970-01-01T00:00:02",
      "3,1970-01-01T00:00,1970-01-01T00:00:02")

    assertEquals(expected.sorted, result.sorted)
  }
}
