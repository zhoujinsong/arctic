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

import org.apache.amoro.io.AuthenticatedFileIO;
import org.apache.amoro.io.reader.AbstractMergeDataReader;
import org.apache.amoro.io.reader.MixedDeleteFilter;
import org.apache.amoro.scan.KeyedTableScanTask;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.amoro.utils.map.StructLikeCollections;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.AdaptHiveParquet;
import org.apache.iceberg.types.Type;

import java.util.Map;
import java.util.function.BiFunction;

public abstract class AbstractMixedHiveMergeDataReader<T> extends AbstractMergeDataReader<T> {

  public AbstractMixedHiveMergeDataReader(
      AuthenticatedFileIO fileIO,
      Schema tableSchema,
      Schema projectedSchema,
      PrimaryKeySpec primaryKeySpec,
      String nameMapping,
      boolean caseSensitive,
      BiFunction<Type, Object, Object> convertConstant,
      boolean reuseContainer,
      StructLikeCollections structLikeCollections,
      boolean reuseChangeDataCache) {
    super(
        fileIO,
        tableSchema,
        projectedSchema,
        primaryKeySpec,
        nameMapping,
        caseSensitive,
        convertConstant,
        reuseContainer,
        structLikeCollections,
        reuseChangeDataCache);
  }

  @Override
  protected MixedDeleteFilter<T> createMixedDeleteFilter(
      KeyedTableScanTask keyedTableScanTask,
      Schema tableSchema,
      Schema projectedSchema,
      PrimaryKeySpec primaryKeySpec,
      StructLikeCollections structLikeCollections) {
    return new AbstractMixedHiveDeleteFilter.GenericMixedHiveDeleteFilter<>(
        keyedTableScanTask,
        tableSchema,
        projectedSchema,
        primaryKeySpec,
        filterNode(keyedTableScanTask).map(Sets::newHashSet).orElse(null),
        structLikeCollections,
        toStructLikeFunction(),
        fileIO);
  }

  @Override
  protected CloseableIterable<T> newParquetIterable(
      FileScanTask task, Schema schema, Map<Integer, ?> idToConstant, boolean reuseContainer) {
    AdaptHiveParquet.ReadBuilder builder =
        AdaptHiveParquet.read(fileIO.newInputFile(task.file().path().toString()))
            .split(task.start(), task.length())
            .project(schema)
            .createReaderFunc(getParquetReaderFunction(schema, idToConstant))
            .filter(task.residual())
            .caseSensitive(caseSensitive);

    if (reuseContainer) {
      builder.reuseContainers();
    }
    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }
}
