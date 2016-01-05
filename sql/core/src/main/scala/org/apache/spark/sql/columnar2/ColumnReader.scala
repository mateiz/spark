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
private[sql] final class ColumnReader(column: Column) {
  // Object and offset used for reading the raw data
  private[this] val encodedObject = column.baseObject
  private[this] var encodedOffset = column.baseOffset
  private[this] val encodedEnd = column.baseOffset + column.numBytes

  // Object and offset storing uncompressed data, if the data is encoded
  private[this] var decodedObject: AnyRef = null
  private[this] var decodedOffset: Long = 0
  private[this] var decodedEnd: Long = 0

  // Repeats left in current run, if column is encoded using RLE
  private[this] var runLengthLeft = 0

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

  def readInts(dest: Array[Int]): Unit = {
    //require(column.bitLength == 32, "column must have bit length 32")
    var i = 0
    while (i < dest.length) {
      if (decodedOffset == decodedEnd) {
        decodeNextChunk()
      }
      while (i + 32 <= dest.length && decodedOffset + 32 * 4 <= decodedEnd) {
        Platform.copyMemory(decodedObject, decodedOffset, dest, Platform.INT_ARRAY_OFFSET + i * 4, 32 * 4)
        decodedOffset += 32 * 4
        i += 32
      }
      while (i < dest.length && decodedOffset < decodedEnd) {
        dest(i) = Platform.getInt(decodedObject, decodedOffset)
        decodedOffset += 4
        i += 1
      }
    }
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
        case RunLengthEncoding(32) =>
          decodeRLE32()
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
        Platform.putByte(decodedObject, outputOffset + 0, ((thisByte >> 0) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 1, ((thisByte >> 1) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 2, ((thisByte >> 2) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 3, ((thisByte >> 3) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 4, ((thisByte >> 4) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 5, ((thisByte >> 5) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 6, ((thisByte >> 6) & 1).toByte)
        Platform.putByte(decodedObject, outputOffset + 7, ((thisByte >> 7) & 1).toByte)
        outputOffset += 8
        encodedOffset += 1
      }
      decodedEnd = outputOffset
    } else if (column.bitLength == 32) {
      val endOffset = math.min(encodedEnd, encodedOffset + DECODE_BUFFER_SIZE / (32 / 1))
      while (encodedOffset < endOffset) {
        val thisByte = Platform.getByte(encodedObject, encodedOffset).toInt
        Platform.putInt(decodedObject, outputOffset + 0 * 4, ((thisByte >> 0) & 1).toByte)
        Platform.putInt(decodedObject, outputOffset + 1 * 4, ((thisByte >> 1) & 1).toByte)
        Platform.putInt(decodedObject, outputOffset + 2 * 4, ((thisByte >> 2) & 1).toByte)
        Platform.putInt(decodedObject, outputOffset + 3 * 4, ((thisByte >> 3) & 1).toByte)
        Platform.putInt(decodedObject, outputOffset + 4 * 4, ((thisByte >> 4) & 1).toByte)
        Platform.putInt(decodedObject, outputOffset + 5 * 4, ((thisByte >> 5) & 1).toByte)
        Platform.putInt(decodedObject, outputOffset + 6 * 4, ((thisByte >> 6) & 1).toByte)
        Platform.putInt(decodedObject, outputOffset + 7 * 4, ((thisByte >> 7) & 1).toByte)
        outputOffset += 32
        encodedOffset += 1
      }
      decodedEnd = outputOffset
    } else {
      throw new NotImplementedError()
    }
  }

  /**
   * Decode bits with FlatEncoding(1) into our decoded buffer.
   */
  private def decodeFlat8(): Unit = {
    var outputOffset = decodedOffset
    if (column.bitLength == 32) {
      val endOffset = math.min(encodedEnd, encodedOffset + DECODE_BUFFER_SIZE / (32 / 8))
      while (encodedOffset + 8 < endOffset) {
        val thisLong = Platform.getLong(encodedObject, encodedOffset)
        Platform.putInt(decodedObject, outputOffset + 0 * 4, (thisLong >> (0 * 8)).toInt & 0xff)
        Platform.putInt(decodedObject, outputOffset + 1 * 4, (thisLong >> (1 * 8)).toInt & 0xff)
        Platform.putInt(decodedObject, outputOffset + 2 * 4, (thisLong >> (2 * 8)).toInt & 0xff)
        Platform.putInt(decodedObject, outputOffset + 3 * 4, (thisLong >> (3 * 8)).toInt & 0xff)
        Platform.putInt(decodedObject, outputOffset + 4 * 4, (thisLong >> (4 * 8)).toInt & 0xff)
        Platform.putInt(decodedObject, outputOffset + 5 * 4, (thisLong >> (5 * 8)).toInt & 0xff)
        Platform.putInt(decodedObject, outputOffset + 6 * 4, (thisLong >> (6 * 8)).toInt & 0xff)
        Platform.putInt(decodedObject, outputOffset + 7 * 4, (thisLong >> (7 * 8)).toInt & 0xff)
        outputOffset += 4 * 8
        encodedOffset += 1 * 8
      }
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

  /**
   * Decode bits with RunLengthEncoding(32) into our decoded buffer.
   */
  private def decodeRLE32(): Unit = {
    if (column.bitLength == 32) {
      val maxValuesInBuffer = DECODE_BUFFER_SIZE / (32 / 8)
      // Load a new run if the previous one is done
      if (runLengthLeft == 0) {
        val runLength = Platform.getByte(encodedObject, encodedOffset)
        val value = Platform.getInt(encodedObject, encodedOffset + 1)
        encodedOffset += 5
        runLengthLeft = runLength & 0xff
        var i = 0
        val n = math.min(runLengthLeft, maxValuesInBuffer)
        while (i < n) {
          Platform.putInt(decodedObject, decodedOffset + i * 4, value)
          i += 1
        }
      }
      // Mark the right range in our buffer to read; the load code above ensures that the whole
      // buffer is full if runLength >= maxValuesInBuffer, and only the start otherwise
      val toGive = math.min(runLengthLeft, maxValuesInBuffer)
      decodedEnd = decodedOffset + 4 * toGive
      runLengthLeft -= toGive
    } else {
      throw new NotImplementedError()
    }
  }
}

object ColumnReader {
  private[columnar2] val DECODE_BUFFER_SIZE = 1024
}
