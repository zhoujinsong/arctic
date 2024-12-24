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

package org.apache.amoro.io.reader;

import org.apache.amoro.io.AuthenticatedFileIO;
import org.apache.amoro.scan.KeyedTableScanTask;
import org.apache.amoro.scan.NodeFileScanTask;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.MetadataColumns;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.amoro.utils.NodeFilter;
import org.apache.amoro.utils.map.StructLikeCollections;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Abstract implementation of mixed-format table data reader for replace merge function.
 *
 * @param <T> to indicate the record data type.
 */
public abstract class AbstractReplaceDataReader<T> extends AbstractKeyedDataReader<T> {
  public AbstractReplaceDataReader(
      AuthenticatedFileIO fileIO,
      Schema tableSchema,
      Schema projectedSchema,
      PrimaryKeySpec primaryKeySpec,
      String nameMapping,
      boolean caseSensitive,
      BiFunction<Type, Object, Object> convertConstant,
      boolean reuseContainer,
      StructLikeCollections structLikeCollections) {
    super(
        fileIO,
        tableSchema,
        projectedSchema,
        primaryKeySpec,
        nameMapping,
        caseSensitive,
        convertConstant,
        reuseContainer,
        structLikeCollections);
  }

  @Override
  public CloseableIterator<T> readData(KeyedTableScanTask keyedTableScanTask) {
    if (hasDeleteData(keyedTableScanTask)) {
      MixedDeleteFilter<T> mixedDeleteFilter =
          createMixedDeleteFilter(
              keyedTableScanTask,
              tableSchema,
              projectedSchema,
              primaryKeySpec,
              structLikeCollections);
      Schema requiredSchema = mixedDeleteFilter.requiredSchema();

      CloseableIterable<T> dataIterable =
          mixedDeleteFilter.filter(readDataTask(keyedTableScanTask, requiredSchema));
      return dataIterable.iterator();
    } else {
      return readDataTask(keyedTableScanTask, projectedSchema).iterator();
    }
  }

  @Override
  public CloseableIterator<T> readDeletedData(KeyedTableScanTask keyedTableScanTask) {
    if (hasDeleteData(keyedTableScanTask)) {
      MixedDeleteFilter<T> mixedDeleteFilter =
          createMixedDeleteFilter(
              keyedTableScanTask,
              tableSchema,
              projectedSchema,
              primaryKeySpec,
              structLikeCollections);

      Schema requiredSchema = mixedDeleteFilter.requiredSchema();

      // Do not include change record for rewrite position delete file optimizing process
      CloseableIterable<T> dataIterable =
          mixedDeleteFilter.filterNegate(readDataTask(keyedTableScanTask, requiredSchema));
      return dataIterable.iterator();
    } else {
      return CloseableIterator.empty();
    }
  }

  private CloseableIterable<T> readDataTask(KeyedTableScanTask scanTask, Schema projectedSchema) {
    Optional<NodeFilter<T>> baseNodeFilter = createNodeFilter(scanTask, projectedSchema);
    CloseableIterable<T> baseRecords =
        CloseableIterable.concat(
            CloseableIterable.transform(
                CloseableIterable.withNoopClose(scanTask.baseTasks()),
                fileScanTask -> readFile(fileScanTask, projectedSchema)));
    if (baseNodeFilter.isPresent()) {
      baseRecords = baseNodeFilter.get().filter(baseRecords);
    }

    CloseableIterable<T> insertRecords =
        CloseableIterable.concat(
            CloseableIterable.transform(
                CloseableIterable.withNoopClose(scanTask.insertTasks()),
                fileScanTask -> readFile(fileScanTask, projectedSchema)));
    if (baseNodeFilter.isPresent()) {
      insertRecords = baseNodeFilter.get().filter(insertRecords);
    }

    if (((NodeFileScanTask) scanTask).isIncludeChangeDataRecords()) {
      Schema changeProjectedSchema =
          TypeUtil.join(projectedSchema, new Schema(MetadataColumns.CHANGE_ACTION_FIELD));
      CloseableIterable<T> changeRecords =
          CloseableIterable.concat(
              CloseableIterable.transform(
                  CloseableIterable.withNoopClose(scanTask.changeTasks()),
                  fileScanTask -> readFile(fileScanTask, changeProjectedSchema)));
      Accessor<StructLike> changeActionAccessor =
          changeProjectedSchema.accessorForField(MetadataColumns.CHANGE_ACTION_ID);
      Function<T, StructLike> asStructLike =
          this.toStructLikeFunction().apply(changeProjectedSchema);
      Optional<NodeFilter<T>> changeNodeFilter = createNodeFilter(scanTask, changeProjectedSchema);
      changeRecords =
          CloseableIterable.filter(
              changeRecords,
              record -> {
                int changeActionCode = (int) changeActionAccessor.get(asStructLike.apply(record));
                // Change action is INSERT or UPDATE_AFTER
                return changeActionCode == 0 || changeActionCode == 2;
              });
      if (changeNodeFilter.isPresent()) {
        changeRecords = changeNodeFilter.get().filter(changeRecords);
      }

      return CloseableIterable.concat(
          Lists.newArrayList(baseRecords, insertRecords, changeRecords));
    } else {
      return CloseableIterable.concat(Lists.newArrayList(baseRecords, insertRecords));
    }
  }
}
