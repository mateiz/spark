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

import org.apache.spark.unsafe.Platform

/**
 * This class can be run manually for performance benchmarks.
 */
object ColumnReaderBenchmark {
  private def time(code: => Unit): Double = {
    val start = System.nanoTime()
    code
    (System.nanoTime() - start) / 1.0e9
  }

  private def benchmark(name: String, bytes: Long, attempts: Int = 8)(code: => Unit): Unit = {
    val gbs = bytes / (1024.0 * 1024.0 * 1024.0)
    for (i <- 1 to attempts) {
      val secs = time(code)
      printf("%s run %d: %.03fs (%.1f GB/s)\n", name, i, secs, gbs / secs)
    }
    println()
  }

  private def arrayIntSum(ints: Array[Int]): Int = {
    var i = 0
    var sum = 0
    while (i < ints.length) {
      sum += ints(i)
      i += 1
    }
    sum
  }

  private def naiveIntSum(data: ByteBuffer): Int = {
    data.rewind()
    var i = 0
    val total = data.limit() / 4
    var sum = 0
    while (i < total) {
      sum += data.getInt
      i += 1
    }
    sum
  }

  private def unsafeIntSum(data: ByteBuffer): Int = {
    data.rewind()
    val array = data.array()
    var sum = 0
    var offset = Platform.BYTE_ARRAY_OFFSET.toLong
    var i = 0
    var end = data.limit() / 4
    while (i < end) {
      sum += Platform.getInt(array, offset)
      offset += 4
      i += 1
    }
    sum
  }

  private def unsafeFlat8Sum(data: ByteBuffer): Int = {
    data.rewind()
    val array = data.array()
    var sum = 0
    var offset = Platform.BYTE_ARRAY_OFFSET.toLong
    var i = 0
    val end = data.limit
    while (i < end) {
      sum += Platform.getByte(array, offset).toInt & 0xff
      offset += 1
      i += 1
    }
    sum
  }

  private def columnSum(column: Column, numInts: Int, vectorize: Boolean): Int = {
    val reader = new ColumnReader(column)
    var i = 0
    var sum = 0
    if (vectorize) {
      val ints = new Array[Int](32)
      while (i < numInts) {
        reader.readInts(ints)
        var j = 0
        while (j < 32) {
          sum += ints(j)
          j += 1
        }
        i += 32
      }
    } else {
      while (i < numInts) {
        sum += reader.readInt()
        i += 1
      }
    }
    sum
  }

  private def flat32IntSum(data: ByteBuffer, numInts: Int, vectorize: Boolean = false): Int = {
    columnSum(new Column(data, 32, FlatEncoding(32)), numInts, vectorize)
  }

  private def flat8IntSum(data: ByteBuffer, numInts: Int, vectorize: Boolean = false): Int = {
    columnSum(new Column(data, 32, FlatEncoding(8)), numInts, vectorize)
  }

  private def flat1IntSum(data: ByteBuffer, numInts: Int, vectorize: Boolean = false): Int = {
    columnSum(new Column(data, 32, FlatEncoding(1)), numInts, vectorize)
  }

  private def rle32IntSum(data: ByteBuffer, numInts: Int, vectorize: Boolean = false): Int = {
    columnSum(new Column(data, 32, RunLengthEncoding(32)), numInts, vectorize)
  }

  def main(args: Array[String]): Unit = {
    val numRawBytes = 32 * 1024 * 1024   // Should be multiple of 64
    val numInts = numRawBytes / 4
    val ints = (0 until numInts).map(i => (i / 64) % 2).toArray
    val expectedSum = ints.sum

    val flat32Data = ByteBuffer.allocate(numRawBytes).order(ByteOrder.nativeOrder())
    flat32Data.asIntBuffer().put(ints)
    flat32Data.rewind()

    val flat8Data = ByteBuffer.allocate(numInts).order(ByteOrder.nativeOrder())
    val intsAsBytes = ints.map(_.toByte)
    flat8Data.put(intsAsBytes)
    flat8Data.rewind()

    val flat1Data = ByteBuffer.allocate(numInts / 8).order(ByteOrder.nativeOrder())
    for (i <- 0 until numInts) {
      val value = ints(i) & 1
      flat1Data.put(i / 8, (flat1Data.get(i / 8) | (value << (i % 8))).toByte)
    }
    flat1Data.rewind()

    val rle32Data = ByteBuffer.allocate(numInts / 64 * 5).order(ByteOrder.nativeOrder())
    for (i <- 0 until numInts / 64) {
      val value = i % 2
      rle32Data.put(i * 5, 64.toByte)
      rle32Data.putInt(i * 5 + 1, value)
    }
    rle32Data.rewind()

    benchmark("Array", numRawBytes) { require(arrayIntSum(ints) == expectedSum) }
    benchmark("Naive", numRawBytes) { require(naiveIntSum(flat32Data) == expectedSum) }
    benchmark("Unsafe32", numRawBytes) { require(unsafeIntSum(flat32Data) == expectedSum) }
    benchmark("Unsafe8", numRawBytes) { require(unsafeFlat8Sum(flat8Data) == expectedSum) }
    benchmark("Flat32", numRawBytes) { require(flat32IntSum(flat32Data, numInts) == expectedSum) }
    benchmark("Flat8", numRawBytes) { require(flat8IntSum(flat8Data, numInts) == expectedSum) }
    benchmark("Flat1", numRawBytes) { require(flat1IntSum(flat1Data, numInts) == expectedSum) }
    benchmark("RLE32", numRawBytes) { require(rle32IntSum(rle32Data, numInts) == expectedSum) }
    benchmark("Flat32Vec", numRawBytes) { require(flat32IntSum(flat32Data, numInts, true) == expectedSum) }
    benchmark("Flat8Vec", numRawBytes) { require(flat8IntSum(flat8Data, numInts, true) == expectedSum) }
    benchmark("Flat1Vec", numRawBytes) { require(flat1IntSum(flat1Data, numInts, true) == expectedSum) }
    benchmark("RLE32Vec", numRawBytes) { require(rle32IntSum(rle32Data, numInts, true) == expectedSum) }
  }
}
