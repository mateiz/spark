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

import org.scalatest.FunSuite

class ColumnReaderSuite extends FunSuite {
  val bytes = ByteBuffer.wrap(Array[Byte](1, 0, 2, 0, Byte.MaxValue, Byte.MinValue))

  val ints = ByteBuffer.allocate(5 * 4).order(ByteOrder.nativeOrder)
  ints.asIntBuffer().put(Array(1, 2, 0, Int.MaxValue, Int.MinValue))
  ints.rewind()

  test("reading basic types") {
    val r1 = new ColumnReader(Column(bytes, FlatEncoding(8)))
    assert(r1.readByte() == 1)
    assert(r1.readByte() == 0)
    assert(r1.readByte() == 2)
    assert(r1.readByte() == 0)
    assert(r1.readByte() == Byte.MaxValue)
    assert(r1.readByte() == Byte.MinValue)

    val r2 = new ColumnReader(Column(bytes, FlatEncoding(8)))
    assert(r2.readBoolean() == true)
    assert(r2.readBoolean() == false)
    assert(r2.readBoolean() == true)
    assert(r2.readBoolean() == false)
    assert(r2.readBoolean() == true)
    assert(r2.readBoolean() == true)

    val r3 = new ColumnReader(Column(ints, FlatEncoding(32)))
    assert(r3.readInt() == 1)
    assert(r3.readInt() == 2)
    assert(r3.readInt() == 0)
    assert(r3.readInt() == Int.MaxValue)
    assert(r3.readInt() == Int.MinValue)

    val r4 = new ColumnReader(Column(bytes, FlatEncoding(8)))
    val data = Array[Byte](5, 5, 5, 5, 5)
    r4.readBytes(data, 1, 3)
    assert(data.toSeq == Seq[Byte](5, 1, 0, 2, 5))
    assert(r4.readByte() == 0)
    assert(r4.readByte() == Byte.MaxValue)
    assert(r4.readByte() == Byte.MinValue)
  }
}
