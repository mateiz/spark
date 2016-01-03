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

import org.apache.spark.unsafe.Platform

/**
 * An encoded column of fixed-length values (e.g. bytes, or ints) with the given `bitLength`.
 * These may be encoded into fewer bits, variable-length encodings, etc.
 *
 * @param baseObject object that our bytes are stored in (for use with spark.unsafe)
 * @param baseOffset start offset of our data inside baseObject
 * @param numBytes size in bytes of our encoded data
 * @param bitLength bit length of values this column stores (in their uncompressed form); must be
 *   one of 8, 16, 32 or 64 -- boolean values should use 8
 * @param encoding how the data is encoded
 */
private[sql]
class Column(
    val baseObject: AnyRef,
    val baseOffset: Long,
    val numBytes: Long,
    val bitLength: Int,
    val encoding: ColumnEncoding)
{
  def this(buffer: ByteBuffer, bitLength: Int, encoding: ColumnEncoding) = {
    this(buffer.array, Platform.BYTE_ARRAY_OFFSET, buffer.limit, bitLength, encoding)
  }
}
