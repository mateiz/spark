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

/**
 * Various options for encoding an individual column (i.e. values with the same bit length).
 */
private[sql] sealed trait ColumnEncoding

/**
 * Flat values one after another, all of the same bit length.
 */
private[sql] case class FlatEncoding(bits: Int) extends ColumnEncoding

/**
 * Run-length encoding of values of a given bit length (multiple of 8); run lengths are all 1 byte.
 */
private[sql] case class RunLengthEncoding(valueBits: Int) extends ColumnEncoding
