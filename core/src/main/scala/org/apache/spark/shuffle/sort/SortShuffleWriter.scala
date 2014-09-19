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

package org.apache.spark.shuffle.sort

import org.apache.spark.serializer.Serializer
import org.apache.spark.util.MutablePair
import org.apache.spark.{MapOutputTracker, SparkEnv, Logging, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{IndexShuffleBlockManager, ShuffleWriter, BaseShuffleHandle}
import org.apache.spark.storage.{BlockObjectWriter, ShuffleBlockId}
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockManager: IndexShuffleBlockManager,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics.shuffleWriteMetrics = Some(writeMetrics)

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    // THIS IS A HACK. For the real write, see write0.
    val (numRecords, pointers) = records.next().asInstanceOf[(Long, Array[Long])]
    assert(numRecords <= pointers.length)

    val outputFile = shuffleBlockManager.getDataFile(dep.shuffleId, mapId)
    val blockId = shuffleBlockManager.consolidateId(dep.shuffleId, mapId)

    // Write the sorted output out.
    val ser = Serializer.getSerializer(dep.serializer)
    // Track location of each range in the output file
    val partitionLengths = new Array[Long](dep.partitioner.numPartitions)

    val startTime = System.currentTimeMillis()

    var i = 0
    var lastPid = -1
    var writer: BlockObjectWriter = null
    val pair = new MutablePair[Long, Long]
    while (i < numRecords) {
      val pid = dep.partitioner.getPartition(pointers(i))

      if (pid != lastPid) {
        // This is a new pid. update the index.
        if (writer != null) {
          writer.commitAndClose()
          partitionLengths(lastPid) = writer.fileSegment().length
        }

        writer = blockManager.getDiskWriter(
          blockId, outputFile, ser, 128 * 1024, context.taskMetrics.shuffleWriteMetrics.get)
        lastPid = pid
      }

      pair._1 = pointers(i)
      writer.write(pair)
      i += 1
    }

    // Handle the last partition
    writer.commitAndClose()
    partitionLengths(lastPid) = writer.fileSegment().length

    shuffleBlockManager.writeIndexFile(dep.shuffleId, mapId, partitionLengths)

    val timeTaken = System.currentTimeMillis() - startTime
    logInfo("Time taken to write shuffle files: " + timeTaken)
    println("Time taken to write shuffle files: " + timeTaken)

    mapStatus = new MapStatus(blockManager.blockManagerId,
      partitionLengths.map(MapOutputTracker.compressSize))
  }


  def write0(records: Iterator[_ <: Product2[K, V]]): Unit = {
    if (dep.mapSideCombine) {
      if (!dep.aggregator.isDefined) {
        throw new IllegalStateException("Aggregator is empty for map-side combine")
      }
      sorter = new ExternalSorter[K, V, C](
        dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
      sorter.insertAll(records)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      sorter = new ExternalSorter[K, V, V](
        None, Some(dep.partitioner), None, dep.serializer)
      sorter.insertAll(records)
    }

    val outputFile = shuffleBlockManager.getDataFile(dep.shuffleId, mapId)
    val blockId = shuffleBlockManager.consolidateId(dep.shuffleId, mapId)
    val partitionLengths = sorter.writePartitionedFile(blockId, context, outputFile)
    shuffleBlockManager.writeIndexFile(dep.shuffleId, mapId, partitionLengths)

    mapStatus = new MapStatus(blockManager.blockManagerId,
      partitionLengths.map(MapOutputTracker.compressSize))
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        // The map task failed, so delete our output data.
        shuffleBlockManager.removeDataByMap(dep.shuffleId, mapId)
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        sorter.stop()
        sorter = null
      }
    }
  }
}
