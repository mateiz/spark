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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.types._

/**
 * Specifies data to be read from a ColumnSet. This requires knowing both the original schema
 * and the fields we want to read (i.e. what we're projecting out).
 *
 * @param schema
 * @param fieldsRead
 */
private[sql] case class ReadSpec(schema: StructType, fieldsRead: Array[Int])

/**
 * Code generator for ColumnSetReaders.
 */
private[sql]
class GenerateColumnSetReader extends CodeGenerator[ReadSpec, ColumnSetReader] {
  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  override protected def canonicalize(in: ReadSpec): ReadSpec = in

  /** Binds an input expression to a given input schema */
  override protected def bind(in: ReadSpec, inputSchema: Seq[Attribute]): ReadSpec = in

  /**
   * Generates a class for a given input expression.  Called when there is not cached code
   * already available.
   */
  override protected def create(spec: ReadSpec): ColumnSetReader = {
    val code = s"""
      import java.util.HashMap;

      public MyReader generate($exprType[] exprs) {
        return new MyReader();
      }

      class MyReader extends ${classOf[ColumnSetReader].getName} {
        private int numRows;
        private int rowsRead;
        private MutableRow outputRow;
        ${spec.fieldsRead.map(i => columnDeclarations(spec.schema(i), "_" + i))}

        public void initialize(ColumnSet data, MutableRow outputRow) {
          this.numRows = data.numRows();
          this.rowsRead = 0;
          this.outputRow = outputRow;
          HashMap<String, ColumnSet> cols = data.columns();
          ${spec.fieldsRead.map(i => columnInitializers(spec.schema(i), "_" + i))}
        }

        public boolean readNext() {
          if (this.rowsRead == this.numRows) {
            return false;
          }
          this.rowsRead++;
          ${spec.fieldsRead.map(i => readField(spec.schema(i), "_" + i, "outputRow", i))}
          return true;
        }
      }
      """

    println(code)

    ???
  }

  private def columnDeclarations(field: StructField, pathPrefix: String): String = {
    val nullable = if (field.nullable) {
      s"""ColumnReader ${pathPrefix}_set;\n"""
    } else {
      ""
    }

    val data = field.dataType match {
      case IntegerType =>
        s"""ColumnReader ${pathPrefix}_data;"""

      case StringType =>
        s"""ColumnReader ${pathPrefix}_data;
            ColumnReader ${pathPrefix}_length;"""

      case StructType(fields) =>
        fields.zipWithIndex.map {
          p => columnDeclarations(p._1, pathPrefix + "_" + p._2)
        }.mkString("\n")
    }

    nullable + data
  }

  private def columnInitializers(field: StructField, pathPrefix: String): String = {
    val nullable = if (field.nullable) {
      s"""${pathPrefix}_set = new ColumnReader(cols.get("${mapKey(pathPrefix + "_set")}"));\n"""
    } else {
      ""
    }

    val data = field.dataType match {
      case IntegerType =>
        s"""${pathPrefix}_data = new ColumnReader(cols.get("${mapKey(pathPrefix + "_data")}"));"""

      case StringType =>
        s"""${pathPrefix}_data = new ColumnReader(cols.get("${mapKey(pathPrefix + "_data")}"));
            ${pathPrefix}_length = new ColumnReader(cols.get("${mapKey(pathPrefix + "_length")}"));
          """

      case StructType(fields) =>
        fields.zipWithIndex.map {
          p => columnInitializers(p._1, pathPrefix + "_" + p._2)
        }.mkString("\n")
    }

    nullable + data
  }

  private def readField(
      field: StructField,
      pathPrefix: String,
      rowVar: String,
      ordinalInRow: Int): String = {
    // Uniquely named local variables
    val length = s"${pathPrefix}_v_length"
    val bytes = s"${pathPrefix}_v_bytes"
    val row = s"${pathPrefix}_v_row"

    val dataReadCode = field.dataType match {
      case IntegerType =>
        s"""$rowVar.setInt($ordinalInRow, ${pathPrefix}_data.readInt());"""

      case StringType =>
        s"""int $length = ${pathPrefix}_length.readInt();
            byte[] $bytes = new byte[$length];
            ${pathPrefix}_data.readBytes($bytes, 0, $length);
            $rowVar.update($ordinalInRow, UTF8String.fromBytes($bytes));
          """

      case StructType(fields) =>
        s"""$row = new SpecificMutableRow();
            ${fields.zipWithIndex.map {
              p => readField(p._1, pathPrefix + "_" + p._2, row, p._2)
            }.mkString("\n")}
         """
    }

    if (field.nullable) {
      s"""if (${pathPrefix}_set.readBoolean()) {
            $dataReadCode
          } else {
            $rowVar.setNullAt($ordinalInRow);
          }
       """.stripMargin
    } else {
      dataReadCode
    }
  }

  /**
   * Convert a path name as used in our generated code (e.g. "_0_data") to the corresponding key in
   * a ColumnSet's hash map (e.g. "0.data").
   */
  private def mapKey(path: String): Unit = {
    path.substring(1).replace('_', '.')
  }
}
