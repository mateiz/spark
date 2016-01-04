/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.columnar2

import java.nio.{ByteOrder, ByteBuffer}

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.unsafe.types.UTF8String

class ColumnSetReaderSuite extends SparkFunSuite {
  private def createByteColumn(bytes: Byte*): Column = {
    val buf = ByteBuffer.allocate(bytes.size).order(ByteOrder.nativeOrder())
    buf.put(bytes.toArray)
    buf.rewind()
    new Column(buf, 8, FlatEncoding(8))
  }

  private def createIntColumn(ints: Int*): Column = {
    val buf = ByteBuffer.allocate(4 * ints.size).order(ByteOrder.nativeOrder())
    buf.asIntBuffer().put(ints.toArray)
    buf.rewind()
    new Column(buf, 32, FlatEncoding(32))
  }

  private def createBooleanColumn(bools: Boolean*): Column = {
    createByteColumn(bools.map(b => if (b) 1.toByte else 0.toByte): _*)
  }

  /**
   * Encode a list of strings as (length, data) columns, returned in that order;
   * only works for strings encodable as ASCII
   */
  private def createStringColumns(strings: String*): (Column, Column) = {
    (createIntColumn(strings.map(_.length): _*),
     createByteColumn(strings.mkString("").map(_.toByte): _*))
  }

  private def checkRowsRead(
      reader: ColumnSetReader,
      data: ColumnSet,
      spec: ReadSpec,
      rows: Seq[InternalRow]): Unit = {
    val row = new SpecificMutableRow(spec.fieldsRead.map(i => spec.schema(i).dataType))
    reader.initialize(data, row)
    for (r <- rows) {
      assert(reader.readNext(), "readNext returned false before reading " + rows.length + " rows")
      assert(r === row)
    }
    assert(!reader.readNext(), "readNext returned true even after " + rows.length + " rows")
  }

  test("flat struct") {
    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)
    ))

    val col1 = createIntColumn(1, -2, 0, Int.MaxValue, Int.MinValue)
    val col2 = createIntColumn(-1, 2, 3, Int.MinValue, Int.MaxValue)
    val map = Map("0.data" -> col1, "1.data" -> col2).asJava
    val colSet = new ColumnSet(structType, map, 5)

    val readSpec = ReadSpec(structType, Array(0, 1))

    val reader = new GenerateColumnSetReader().generate(readSpec)
    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(1, -1),
      InternalRow(-2, 2),
      InternalRow(0, 3),
      InternalRow(Int.MaxValue, Int.MinValue),
      InternalRow(Int.MinValue, Int.MaxValue)))
  }

  test("flat struct with nullables") {
    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", IntegerType, nullable = true)
    ))

    val set1 = createBooleanColumn(true, false, true, false, true)
    val col1 = createIntColumn(1, 0, Int.MinValue)
    val set2 = createBooleanColumn(false, true, true, false, true)
    val col2 = createIntColumn(-2, 3, Int.MaxValue)
    val map = Map(
      "0.set" -> set1,
      "0.data" -> col1,
      "1.set" -> set2,
      "1.data" -> col2
    ).asJava
    val colSet = new ColumnSet(structType, map, 5)

    val readSpec = ReadSpec(structType, Array(0, 1))

    val reader = new GenerateColumnSetReader().generate(readSpec)
    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(1, null),
      InternalRow(null, -2),
      InternalRow(0, 3),
      InternalRow(null, null),
      InternalRow(Int.MinValue, Int.MaxValue)))
  }

  test("flat struct with nullables and strings") {
    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", StringType, nullable = true)
    ))

    val set1 = createBooleanColumn(true, false, true, false, true)
    val col1 = createIntColumn(1, 0, Int.MinValue)
    val set2 = createBooleanColumn(false, true, true, false, true)
    val (col2Length, col2Data) = createStringColumns("Hi", "", "there")
    val map = Map(
      "0.set" -> set1,
      "0.data" -> col1,
      "1.set" -> set2,
      "1.length" -> col2Length,
      "1.data" -> col2Data
    ).asJava
    val colSet = new ColumnSet(structType, map, 5)

    val readSpec = ReadSpec(structType, Array(0, 1))

    val reader = new GenerateColumnSetReader().generate(readSpec)
    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(1, null),
      InternalRow(null, UTF8String.fromString("Hi")),
      InternalRow(0, UTF8String.fromString("")),
      InternalRow(null, null),
      InternalRow(Int.MinValue, UTF8String.fromString("there"))))
  }


  test("reading only some fields") {
    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", StringType, nullable = true)
    ))

    val set1 = createBooleanColumn(true, false, true, false, true)
    val data1 = createIntColumn(1, 0, Int.MinValue)
    val set2 = createBooleanColumn(false, true, true, false, true)
    val (length2, data2) = createStringColumns("Hi", "", "there")
    val map = Map(
      "0.set" -> set1,
      "0.data" -> data1,
      "1.set" -> set2,
      "1.length" -> length2,
      "1.data" -> data2
    ).asJava
    val colSet = new ColumnSet(structType, map, 5)

    val readSpec = ReadSpec(structType, Array(1))

    val reader = new GenerateColumnSetReader().generate(readSpec)
    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(null),
      InternalRow(UTF8String.fromString("Hi")),
      InternalRow(UTF8String.fromString("")),
      InternalRow(null),
      InternalRow(UTF8String.fromString("there"))))
  }

  test("nested structs") {
    val nestedType = StructType(Seq(
      StructField("c", IntegerType, nullable = true),
      StructField("d", StringType, nullable = false)
    ))

    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", nestedType, nullable = true)
    ))

    val dataA = createIntColumn(1, 2, 3, 4)
    val setB = createBooleanColumn(true, false, true, true)
    val setC = createBooleanColumn(true, false, true)
    val dataC = createIntColumn(1, 2)
    val (lengthD, dataD) = createStringColumns("foo", "bar", "")

    val map = Map(
      "0.data" -> dataA,
      "1.set" -> setB,
      "1.0.set" -> setC,
      "1.0.data" -> dataC,
      "1.1.length" -> lengthD,
      "1.1.data" -> dataD
    ).asJava

    val colSet = new ColumnSet(structType, map, 4)

    val readSpec = ReadSpec(structType, Array(0, 1))

    val reader = new GenerateColumnSetReader().generate(readSpec)

    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(1, InternalRow(1, UTF8String.fromString("foo"))),
      InternalRow(2, null),
      InternalRow(3, InternalRow(null, UTF8String.fromString("bar"))),
      InternalRow(4, InternalRow(2, UTF8String.fromString("")))))
  }

  test("simple array") {
    val structType = StructType(Seq(
      StructField("a", ArrayType(IntegerType, false), nullable = false)
    ))

    val data = createIntColumn(1, -2, 0, Int.MaxValue, Int.MinValue)
    val length = createIntColumn(2, 0, 3)
    val map = Map("0.elem.data" -> data, "0.length" -> length).asJava
    val colSet = new ColumnSet(structType, map, 3)

    val readSpec = ReadSpec(structType, Array(0))

    val reader = new GenerateColumnSetReader().generate(readSpec)
    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(new GenericArrayData(Seq(1, -2))),
      InternalRow(new GenericArrayData(Seq())),
      InternalRow(new GenericArrayData(Seq(0, Int.MaxValue, Int.MinValue)))))
  }

  test("array with nullable elements") {
    val structType = StructType(Seq(
      StructField("a", ArrayType(IntegerType, true), nullable = false)
    ))

    val data = createIntColumn(1, -2, 0, Int.MaxValue, Int.MinValue)
    val set = createBooleanColumn(true, false, true, false, true, true, true)
    val length = createIntColumn(3, 1, 3)
    val map = Map("0.elem.data" -> data, "0.elem.set" -> set, "0.length" -> length).asJava
    val colSet = new ColumnSet(structType, map, 3)

    val readSpec = ReadSpec(structType, Array(0))

    val reader = new GenerateColumnSetReader().generate(readSpec)
    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(new GenericArrayData(Seq(1, null, -2))),
      InternalRow(new GenericArrayData(Seq(null))),
      InternalRow(new GenericArrayData(Seq(0, Int.MaxValue, Int.MinValue)))))
  }

  test("array of structs") {
    val elemType = StructType(Seq(
      StructField("b", IntegerType, nullable = true),
      StructField("c", IntegerType, nullable = false)
    ))

    val structType = StructType(Seq(
      StructField("a", ArrayType(elemType, false), nullable = false)
    ))

    val bData = createIntColumn(1, 2, 3)
    val bSet = createBooleanColumn(true, false, true, false, true)
    val cData = createIntColumn(5, 4, 3, 2, 1)
    val length = createIntColumn(2, 0, 3)
    val map = Map(
      "0.length" -> length,
      "0.elem.0.data" -> bData,
      "0.elem.0.set" -> bSet,
      "0.elem.1.data" -> cData
    ).asJava
    val colSet = new ColumnSet(structType, map, 3)

    val readSpec = ReadSpec(structType, Array(0))

    val reader = new GenerateColumnSetReader().generate(readSpec)
    checkRowsRead(reader, colSet, readSpec, Seq(
      InternalRow(new GenericArrayData(Seq(
        InternalRow(1, 5), InternalRow(null, 4)))),
      InternalRow(new GenericArrayData(Seq())),
      InternalRow(new GenericArrayData(Seq(
        InternalRow(2, 3), InternalRow(null, 2), InternalRow(3, 1))))))
  }
}
