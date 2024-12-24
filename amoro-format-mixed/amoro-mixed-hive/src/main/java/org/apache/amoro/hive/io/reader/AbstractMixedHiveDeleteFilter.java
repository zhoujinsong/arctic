/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.hive.io.reader;

import org.apache.amoro.data.DataTreeNode;
import org.apache.amoro.io.AuthenticatedFileIO;
import org.apache.amoro.io.reader.MixedDeleteFilter;
import org.apache.amoro.scan.KeyedTableScanTask;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.amoro.utils.map.StructLikeCollections;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.AdaptHiveGenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.AdaptHiveParquet;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Abstract implementation of MixedDeleteFilter to adapt hive when open equality delete files.
 *
 * @param <T> to indicate the record data type.
 */
public abstract class AbstractMixedHiveDeleteFilter<T> extends MixedDeleteFilter<T> {

  protected AbstractMixedHiveDeleteFilter(
      KeyedTableScanTask keyedTableScanTask,
      Schema tableSchema,
      Schema requestedSchema,
      PrimaryKeySpec primaryKeySpec,
      Set<DataTreeNode> sourceNodes,
      StructLikeCollections structLikeCollections) {
    super(
        keyedTableScanTask,
        tableSchema,
        requestedSchema,
        primaryKeySpec,
        sourceNodes,
        structLikeCollections);
  }

  @Override
  protected CloseableIterable<Record> openParquet(
      InputFile input, Schema deleteSchema, Map<Integer, Object> idToConstant) {
    AdaptHiveParquet.ReadBuilder builder =
        AdaptHiveParquet.read(input)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(
                fileSchema ->
                    AdaptHiveGenericParquetReaders.buildReader(
                        deleteSchema, fileSchema, idToConstant));

    return builder.build();
  }

  static class GenericMixedHiveDeleteFilter<T> extends AbstractMixedHiveDeleteFilter<T> {

    protected final Function<T, StructLike> asStructLike;
    protected final AuthenticatedFileIO fileIO;

    GenericMixedHiveDeleteFilter(
        KeyedTableScanTask keyedTableScanTask,
        Schema tableSchema,
        Schema requestedSchema,
        PrimaryKeySpec primaryKeySpec,
        Set<DataTreeNode> sourceNodes,
        StructLikeCollections structLikeCollections,
        Function<Schema, Function<T, StructLike>> toStructLikeFunction,
        AuthenticatedFileIO fileIO) {
      super(
          keyedTableScanTask,
          tableSchema,
          requestedSchema,
          primaryKeySpec,
          sourceNodes,
          structLikeCollections);
      this.asStructLike = toStructLikeFunction.apply(requiredSchema());
      this.fileIO = fileIO;
    }

    @Override
    protected StructLike asStructLike(T record) {
      return asStructLike.apply(record);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return fileIO.newInputFile(location);
    }

    @Override
    protected AuthenticatedFileIO getFileIO() {
      return fileIO;
    }
  }
}
