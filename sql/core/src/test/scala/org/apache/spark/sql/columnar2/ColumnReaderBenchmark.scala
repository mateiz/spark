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

  private def benchmark(name: String, bytes: Long, attempts: Int = 10)(code: => Unit): Unit = {
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
    val end = data.limit().toLong + Platform.BYTE_ARRAY_OFFSET.toLong
    while (offset < end) {
      sum += Platform.getInt(array, offset)
      offset += 4
    }
    sum
  }

  private def flat32IntSum(data: ByteBuffer): Int = {
    data.rewind()
    val column = new Column(data, 32, FlatEncoding(32))
    val reader = new ColumnReader(column)
    var i = 0
    val total = data.limit() / 4
    var sum = 0
    while (i < total) {
      sum += reader.readInt()
      i += 1
    }
    sum
  }

  def main(args: Array[String]): Unit = {
    val numBytes = 16 * 1024 * 1024
    val numInts = numBytes / 4
    val data = ByteBuffer.allocate(numBytes).order(ByteOrder.nativeOrder())
    val ints = (0 until numInts).map(_ / 10).toArray
    data.asIntBuffer().put(ints)
    data.rewind()
    val expectedSum = ints.sum

    benchmark("Array", numBytes) { require(arrayIntSum(ints) == expectedSum) }
    benchmark("Naive", numBytes) { require(naiveIntSum(data) == expectedSum) }
    benchmark("Unsafe", numBytes) { require(unsafeIntSum(data) == expectedSum) }
    benchmark("Flat32", numBytes) { require(flat32IntSum(data) == expectedSum) }
  }
}
