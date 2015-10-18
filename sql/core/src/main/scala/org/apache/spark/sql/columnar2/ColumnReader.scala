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

import java.nio.ByteBuffer

/**
 * Reads values from a column as primitive types. The column is supposed to only contain values
 * of the same type, but they may be compressed or bit-packed into fewer bits, so there are
 * accessor methods for all the primitive types one might expect to read from the column.
 */
private[sql] class ColumnReader(column: Column) {
  def readBoolean(): Boolean = ???

  def readByte(): Boolean = ???

  def readInt(): Boolean = ???

  def readBytes(dest: ByteBuffer, numBytes: Int): Unit = ???
}
