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

import org.apache.amoro.data.PrimaryKeyedFile;
import org.apache.amoro.io.AuthenticatedFileIO;
import org.apache.amoro.scan.KeyedTableScanTask;
import org.apache.amoro.scan.MixedFileScanTask;
import org.apache.amoro.scan.NodeFileScanTask;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.MetadataColumns;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.amoro.utils.NodeFilter;
import org.apache.amoro.utils.map.StructLikeCollections;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Abstract implementation of mixed-format table data reader for other merge functions except
 * replace.
 *
 * @param <T> to indicate the record data type.
 */
public abstract class AbstractMergeDataReader<T> extends AbstractKeyedDataReader<T> {

  private ChangeDataMap<T> changeDataMap;

  public AbstractMergeDataReader(
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
    Schema changeRequiredSchema = changeRequiredSchema();
    ChangeDataMap<T> changeMap = constructChangeMap(keyedTableScanTask, changeRequiredSchema);

    MixedDeleteFilter<T> deleteFilter =
        createMixedDeleteFilter(
            new NodeFileScanTask(
                ((NodeFileScanTask) keyedTableScanTask).treeNode(), keyedTableScanTask.baseTasks()),
            tableSchema,
            changeRequiredSchema,
            primaryKeySpec,
            structLikeCollections);
    Schema baseRequiredSchema = deleteFilter.requiredSchema();
    CloseableIterable<T> baseRecords =
        readBaseData(keyedTableScanTask, baseRequiredSchema);
    baseRecords = deleteFilter.filter(baseRecords);
    StructProjection changeProjectRow =
        StructProjection.create(baseRequiredSchema, primaryKeySpec.getPkSchema());
    Function<T, StructLike> asStructLike = this.toStructLikeFunction().apply(baseRequiredSchema);
    baseRecords =
        CloseableIterable.transform(
            baseRecords,
            record -> {
              StructLike key = changeProjectRow.copyFor(asStructLike.apply(record));
              T changeData = changeMap.remove(key);
              if (changeData != null) {
                return mergeFunction().merge(record, changeData);
              } else {
                return record;
              }
            });
    return CloseableIterable.concat(Lists.newArrayList(baseRecords,
        CloseableIterable.withNoopClose(changeMap.valuesIterable()))).iterator();
  }

  @Override
  public CloseableIterator<T> readDeletedData(KeyedTableScanTask keyedTableScanTask) {
    Schema changeRequiredSchema = changeRequiredSchema();
    ChangeDataMap<T> changeMap = constructChangeMap(keyedTableScanTask, changeRequiredSchema);
    MixedDeleteFilter<T> deleteFilter =
        createMixedDeleteFilter(
            new NodeFileScanTask(
                ((NodeFileScanTask) keyedTableScanTask).treeNode(), keyedTableScanTask.baseTasks()),
            tableSchema,
            changeRequiredSchema,
            primaryKeySpec,
            structLikeCollections);
    Schema baseRequiredSchema = deleteFilter.requiredSchema();
    CloseableIterable<T> baseRecords =
        readBaseData(keyedTableScanTask, baseRequiredSchema);
    Predicate<T> deletedPredicate = deleteFilter.deletedPredicate();
    StructProjection changeProjectRow =
        StructProjection.create(baseRequiredSchema, primaryKeySpec.getPkSchema());
    Function<T, StructLike> asStructLike = this.toStructLikeFunction().apply(baseRequiredSchema);
    CloseableIterable<T> deletedBaseRecords =
        CloseableIterable.filter(
            baseRecords,
            record -> {
              StructLike key = changeProjectRow.copyFor(asStructLike.apply(record));
              if (!deletedPredicate.test(record)) {
                T changeData = changeMap.get(key);
                if (changeData != null) {
                  changeMap.forcePut(key, mergeFunction().merge(record, changeData));
                  return true;
                }
                return false;
              } else {
                return true;
              }
            });
    return deletedBaseRecords.iterator();
  }

  private ChangeDataMap<T> constructChangeMap(KeyedTableScanTask keyedTableScanTask, Schema requiredSchema) {
    //Reuse change data map for self-optimizing process
    if (changeDataMap != null) {
      return changeDataMap;
    }
    ChangeDataMap<T> changeMap =
        new ChangeDataMap<>(primaryKeySpec.primaryKeyStruct(), mergeFunction());
    CloseableIterable<T> changeRecords =
        CloseableIterable.concat(
            CloseableIterable.transform(
                CloseableIterable.withNoopClose(sortChangeFiles(keyedTableScanTask.changeTasks())),
                fileScanTask -> readFile(fileScanTask, requiredSchema, false)));
    Optional<NodeFilter<T>> changeNodeFilter =
        createNodeFilter(keyedTableScanTask, requiredSchema);
    StructProjection changeProjectRow =
        StructProjection.create(requiredSchema, primaryKeySpec.getPkSchema());

    if (changeNodeFilter.isPresent()) {
      changeRecords = changeNodeFilter.get().filter(changeRecords);
    }
    Function<T, StructLike> asStructLike = this.toNonResueStructLikeFunction().apply(requiredSchema);
    changeRecords.forEach(
        record -> changeMap.put(changeProjectRow.copyFor(asStructLike.apply(record)), record));
    return changeMap;
  }

  private CloseableIterable<T> readBaseData(
      KeyedTableScanTask keyedTableScanTask, Schema requiredSchema) {
    CloseableIterable<T> baseRecords =
        CloseableIterable.concat(
            CloseableIterable.transform(
                CloseableIterable.withNoopClose(keyedTableScanTask.baseTasks()),
                fileScanTask -> readFile(fileScanTask, requiredSchema)));
    Optional<NodeFilter<T>> baseNodeFilter = createNodeFilter(keyedTableScanTask, requiredSchema);
    if (baseNodeFilter.isPresent()) {
      baseRecords = baseNodeFilter.get().filter(baseRecords);
    }
    return baseRecords;
  }

  private List<MixedFileScanTask> sortChangeFiles(List<MixedFileScanTask> changeTasks) {
    changeTasks.sort(
        (task1, task2) -> {
          PrimaryKeyedFile file1 = task1.file();
          PrimaryKeyedFile file2 = task2.file();
          if (file1.transactionId().equals(file2.transactionId())) {
            Long offset1 =
                Conversions.fromByteBuffer(
                    Types.LongType.get(),
                    file1.lowerBounds().get(MetadataColumns.FILE_OFFSET_FILED_ID));
            Long offset2 =
                Conversions.fromByteBuffer(
                    Types.LongType.get(),
                    file2.lowerBounds().get(MetadataColumns.FILE_OFFSET_FILED_ID));
            if (offset1 != null && offset2 != null) {
              return offset1.compareTo(offset2);
            } else {
              return 0;
            }
          } else {
            return file1.transactionId().compareTo(file2.transactionId());
          }
        });
    return changeTasks;
  }

  private Schema changeRequiredSchema() {
    Set<Integer> projectedIds = TypeUtil.getProjectedIds(projectedSchema);
    Set<Integer> primaryKeyIds = primaryKeySpec.primaryKeyIds();
    List<Types.NestedField> columns = Lists.newArrayList(projectedSchema.columns());
    for (Integer fieldId : primaryKeyIds) {
      if (!projectedIds.contains(fieldId)) {
        Types.NestedField field = tableSchema.asStruct().field(fieldId);
        Preconditions.checkArgument(field != null, "Cannot find required field for ID %s", fieldId);
        columns.add(field);
      }
    }
    columns.add(MetadataColumns.TRANSACTION_ID_FILED);
    columns.add(MetadataColumns.FILE_OFFSET_FILED);

    return new Schema(columns);
  }

  protected abstract MergeFunction<T> mergeFunction();

  protected abstract Function<Schema, Function<T, StructLike>> toNonResueStructLikeFunction();
}
