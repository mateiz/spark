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

import java.nio.ByteOrder

/**
 * Reads values from a column as primitive types. The column is supposed to only contain values
 * of the same type, but these may be packed in various ways.
 */
private[sql] class ColumnReader(column: Column) {
  // TODO: replace with fast byte buffer reader
  private val buffer = column.data.duplicate().order(ByteOrder.nativeOrder())

  // TODO: support compression in all these methods (we'd probably do it by having a small
  // buffer we periodically decompress into)

  def readBoolean(): Boolean = {
    require(column.encoding == FlatEncoding(8), "encoding must be FlatEncoding(8)")
    buffer.get() != 0
  }

  def readByte(): Byte = {
    require(column.encoding == FlatEncoding(8), "encoding must be FlatEncoding(8)")
    buffer.get()
  }

  def readInt(): Int = {
    require(column.encoding == FlatEncoding(32), "encoding must be FlatEncoding(32)")
    buffer.getInt()
  }

  def readBytes(dest: Array[Byte], offset: Int, length: Int): Unit = {
    require(column.encoding == FlatEncoding(8), "encoding must be FlatEncoding(8)")
    buffer.get(dest, offset, length)
  }
}
