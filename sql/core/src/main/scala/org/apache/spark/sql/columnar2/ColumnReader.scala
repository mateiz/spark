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

import org.apache.spark.unsafe.Platform

/**
 * Reads values from a column as primitive types. The column is supposed to only contain values
 * of the same type, but these may be packed in various ways.
 *
 * This class does not do bounds checking -- it is only meant to be called from inside
 * ColumnSetReader, which makes sure not to read past the end of the row batch.
 */
private[sql] class ColumnReader(column: Column) {
  // Object and offset used for reading the raw data
  private val encodedObject = column.baseObject
  private var encodedOffset = column.baseOffset
  private val encodedEnd = column.baseOffset + column.numBytes

  // Object and offset storing uncompressed data, if the data is encoded
  private var decodedObject: AnyRef = null
  private var decodedOffset: Long = 0
  private var decodedEnd: Long = 0

  import ColumnReader.DECODE_BUFFER_SIZE

  decodeNextChunk()

  def readBoolean(): Boolean = {
    //require(column.bitLength == 8, "column must have bit length 8")
    if (decodedOffset == decodedEnd) {
      decodeNextChunk()
    }
    val res = Platform.getByte(decodedObject, decodedOffset)
    decodedOffset += 1
    res != 0
  }

  def readByte(): Byte = {
    //require(column.bitLength == 8, "column must have bit length 8")
    if (decodedOffset == decodedEnd) {
      decodeNextChunk()
    }
    val res = Platform.getByte(decodedObject, decodedOffset)
    decodedOffset += 1
    res
  }

  def readInt(): Int = {
    //require(column.bitLength == 32, "column must have bit length 32")
    if (decodedOffset == decodedEnd) {
      decodeNextChunk()
    }
    val res = Platform.getInt(decodedObject, decodedOffset)
    decodedOffset += 4
    res
  }

  def readBytes(dest: Array[Byte], offset: Int, length: Int): Unit = {
    //require(column.bitLength == 8, "column must have bit length 8")
    var pos = 0L
    while (pos < length) {
      if (decodedOffset == decodedEnd) {
        decodeNextChunk()
      }
      val toRead = math.min(length - pos, decodedEnd - decodedOffset)
      Platform.copyMemory(decodedObject, decodedOffset, dest,
        Platform.BYTE_ARRAY_OFFSET + offset + pos, toRead)
      pos += toRead
      decodedOffset += toRead
    }
  }

  /**
   * Decode a new chunk of data into decodedObject/decodedOffset/decodedEnd, starting at the
   * current dataOffset in dataObject.
   *
   * If the data is flat-encoded with exactly the bit length of the column, we'll simply set
   * decodedObject to point to dataObject. Otherwise, this method will decode a small number
   * of bytes at a time, based on the encoding method. This design lets the read path be the same
   * regardless of the encoding method, and avoids having many branches inside it.
   */
  private def decodeNextChunk(): Unit = {
    if (column.encoding == FlatEncoding(column.bitLength)) {
      // Just point to the data buffers, unless we're called twice, in which case we're at EOF
      if (decodedObject == null) {
        decodedObject = encodedObject
        decodedOffset = encodedOffset
        decodedEnd = encodedEnd
      } else {
        throw new IndexOutOfBoundsException("reading past end of column")
      }
    } else {
      // Make sure we have a byte array to write stuff into
      if (decodedObject == null) {
        decodedObject = new Array[Byte](DECODE_BUFFER_SIZE)
      }
      // Check for EOF
      if (encodedOffset == encodedEnd) {
        throw new IndexOutOfBoundsException("reading past end of column")
      }
      decodedOffset = Platform.BYTE_ARRAY_OFFSET
        // Decode stuff; to avoid a lot of branches, we have a method for every encoding we
      // might pick, which is kind of weird but can hopefully be code-generated later
      column.encoding match {
        case FlatEncoding(1) =>
          decodeFlat1()
        case FlatEncoding(8) =>
          decodeFlat8()
        case _ =>
          throw new NotImplementedError()
      }
    }
  }

  /**
   * Decode bits with FlatEncoding(1) into our decoded buffer, and update decodedEnd and dataEnd.
   */
  private def decodeFlat1(): Unit = {
    var outputOffset = decodedOffset
    if (column.bitLength == 8) {
      val endOffset = math.min(encodedEnd, encodedOffset + DECODE_BUFFER_SIZE / (8 / 1))
      while (encodedOffset < endOffset) {
        val thisByte = Platform.getByte(encodedObject, encodedOffset).toInt
        Platform.putByte(decodedObject, outputOffset + 0, ((thisByte >> 7) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 1, ((thisByte >> 6) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 2, ((thisByte >> 5) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 3, ((thisByte >> 4) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 4, ((thisByte >> 3) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 5, ((thisByte >> 2) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 6, ((thisByte >> 1) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 7, (thisByte & 1).toByte)
        outputOffset += 8
        encodedOffset += 1
      }
      decodedEnd = outputOffset
    } else {
      throw new NotImplementedError()
    }
  }

  /**
   * Decode bits with FlatEncoding(1) into our decoded buffer
   */
  private def decodeFlat8(): Unit = {
    var outputOffset = decodedOffset
    if (column.bitLength == 32) {
      val endOffset = math.min(encodedEnd, encodedOffset + DECODE_BUFFER_SIZE / (32 / 8))
      while (encodedOffset < endOffset) {
        val thisByte = Platform.getByte(encodedObject, encodedOffset)
        Platform.putInt(decodedObject, outputOffset, thisByte & 0xff)
        outputOffset += 4
        encodedOffset += 1
      }
      decodedEnd = outputOffset
    } else {
      throw new NotImplementedError()
    }
  }
}

object ColumnReader {
  private[columnar2] val DECODE_BUFFER_SIZE = 1024
}
