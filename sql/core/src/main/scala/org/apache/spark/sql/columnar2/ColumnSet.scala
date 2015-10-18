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

import java.util.{Map => JMap}

import org.apache.spark.sql.types.{StructType, DataType}

/**
 * A set of columns representing a list of values of type dataType.
 *
 * For simplicity, we key all the columns using strings that represent the path to that field
 * inside dataType; for example, for a struct(x, struct(y, z)), we might have columns named
 * "0.data" for x, "1.0.data" for y and "1.1.data" for z. Some data types might require multiple
 * columns, e.g. both a "data" and "length" for lists. Technically this encoding with string
 * names is not needed, but it makes the code easier for now.
 *
 * TODO: Might want an additional map saying which fields are dictionary-encoded, though that can
 * also be inferred from the columns we generate from them.
 */
class ColumnSet(
  val dataType: StructType,
  val columns: JMap[String, Column],
  val numRows: Int)
