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

package org.apache.spark.sql.columnar2;

import org.apache.spark.unsafe.Platform;
import scala.NotImplementedError;

public final class ColumnReader2 {
  static final int DECODE_BUFFER_SIZE = 1024;

  private Column column;

  // Object and offset used for reading the raw data
  private Object encodedObject;
  private long encodedOffset;
  private long encodedEnd;

  // Object and offset storing uncompressed data, if the data is encoded
  private Object decodedObject;
  private long decodedOffset;
  private long decodedEnd;

  public ColumnReader2(Column column) {
    this.column = column;
    this.encodedObject = column.baseObject();
    this.encodedOffset = column.baseOffset();
    this.encodedEnd = column.baseOffset() + column.numBytes();
    ColumnEncoding encoding = column.encoding();
    if (encoding instanceof FlatEncoding && ((FlatEncoding) encoding).bits() == column.bitLength()) {
      // Just point to the data buffers
      decodedObject = encodedObject;
      decodedOffset = encodedOffset;
      decodedEnd = encodedEnd;
    } else {
      decodedObject = new byte[DECODE_BUFFER_SIZE];
      decodeNextChunk();
    }
  }

  public int readInt() {
    //require(column.bitLength == 32, "column must have bit length 32")
    //if (decodedOffset == decodedEnd) {
    //  decodeNextChunk();
    //}
    int res = Platform.getInt(decodedObject, decodedOffset);
    decodedOffset += 4;
    return res;
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
  private void decodeNextChunk() {
    ColumnEncoding encoding = column.encoding();
    if (decodedObject == encodedObject) {
      // decodeNextChunk was called after already reading through the whole original object
      throw new IndexOutOfBoundsException("reading past end of column");
    } else {
      // Check for EOF
      if (encodedOffset == encodedEnd) {
        throw new IndexOutOfBoundsException("reading past end of column");
      }
      decodedOffset = Platform.BYTE_ARRAY_OFFSET;
      // Decode stuff; to avoid a lot of branches, we have a method for every encoding we
      // might pick, which is kind of weird but can hopefully be code-generated later
      if (encoding instanceof FlatEncoding) {
        FlatEncoding fe = (FlatEncoding) encoding;
        if (fe.bits() == 8) {
          decodeFlat8();
        }
      }
      throw new NotImplementedError();
    }
  }

  /**
   * Decode bits with FlatEncoding(1) into our decoded buffer
   */
  private void decodeFlat8() {
    long outputOffset = decodedOffset;
    if (column.bitLength() == 32) {
      long endOffset = Math.min(encodedEnd, encodedOffset + DECODE_BUFFER_SIZE / (32 / 8));
      while (encodedOffset + 8 < endOffset) {
        long thisLong = Platform.getLong(encodedObject, encodedOffset);
        Platform.putInt(decodedObject, outputOffset + 0 * 4, (int) (thisLong >> (0 * 8)) & 0xff);
        Platform.putInt(decodedObject, outputOffset + 1 * 4, (int) (thisLong >> (1 * 8)) & 0xff);
        Platform.putInt(decodedObject, outputOffset + 2 * 4, (int) (thisLong >> (2 * 8)) & 0xff);
        Platform.putInt(decodedObject, outputOffset + 3 * 4, (int) (thisLong >> (3 * 8)) & 0xff);
        Platform.putInt(decodedObject, outputOffset + 4 * 4, (int) (thisLong >> (4 * 8)) & 0xff);
        Platform.putInt(decodedObject, outputOffset + 5 * 4, (int) (thisLong >> (5 * 8)) & 0xff);
        Platform.putInt(decodedObject, outputOffset + 6 * 4, (int) (thisLong >> (6 * 8)) & 0xff);
        Platform.putInt(decodedObject, outputOffset + 7 * 4, (int) (thisLong >> (7 * 8)) & 0xff);
        outputOffset += 4 * 8;
        encodedOffset += 1 * 8;
      }
      while (encodedOffset < endOffset) {
        byte thisByte = Platform.getByte(encodedObject, encodedOffset);
        Platform.putInt(decodedObject, outputOffset, thisByte & 0xff);
        outputOffset += 4;
        encodedOffset += 1;
      }
      decodedEnd = outputOffset;
    } else {
      throw new NotImplementedError();
    }
  }
}
