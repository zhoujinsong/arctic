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

package org.apache.amoro.optimizing;

import org.apache.amoro.data.DataFileType;
import org.apache.amoro.data.DataTreeNode;
import org.apache.amoro.data.PrimaryKeyedFile;
import org.apache.amoro.io.reader.AbstractKeyedDataReader;
import org.apache.amoro.io.reader.GenericMergeDataReader;
import org.apache.amoro.io.reader.GenericReplaceDataReader;
import org.apache.amoro.scan.BasicMixedFileScanTask;
import org.apache.amoro.scan.MixedFileScanTask;
import org.apache.amoro.scan.NodeFileScanTask;
import org.apache.amoro.table.KeyedTable;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.utils.map.StructLikeCollections;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.PropertyUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MixedIcebergOptimizingDataReader implements OptimizingDataReader {
  public static final String NODE_ID = "node-id";
  private final MixedTable table;

  private final StructLikeCollections structLikeCollections;

  private final RewriteFilesInput input;

  public MixedIcebergOptimizingDataReader(
      MixedTable table, StructLikeCollections structLikeCollections, RewriteFilesInput input) {
    this.table = table;
    this.structLikeCollections = structLikeCollections;
    this.input = input;
  }

  @Override
  public CloseableIterable<Record> readData() {
    AbstractKeyedDataReader<Record> reader = mixedTableDataReader(table.schema());

    // Change returned value by readData  from Iterator to Iterable in future
    CloseableIterator<Record> closeableIterator =
        reader.readData(nodeFileScanTask(input.rewrittenDataFilesForMixed()));
    return wrapIterator2Iterable(closeableIterator);
  }

  @Override
  public CloseableIterable<Record> readDeletedData() {
    Schema schema =
        new Schema(
            MetadataColumns.FILE_PATH,
            MetadataColumns.ROW_POSITION,
            org.apache.amoro.table.MetadataColumns.TREE_NODE_FIELD);
    AbstractKeyedDataReader<Record> reader = mixedTableDataReader(schema);
    return wrapIterator2Iterable(
        reader.readDeletedData(nodeFileScanTask(input.rePosDeletedDataFilesForMixed())));
  }

  @Override
  public void close() {}

  protected AbstractKeyedDataReader<Record> mixedTableDataReader(Schema requiredSchema) {

    PrimaryKeySpec primaryKeySpec = PrimaryKeySpec.noPrimaryKey();
    if (table.isKeyedTable()) {
      KeyedTable keyedTable = table.asKeyedTable();
      primaryKeySpec = keyedTable.primaryKeySpec();
    }

    String mergeFunction =
        PropertyUtil.propertyAsString(
            table.properties(),
            org.apache.amoro.table.TableProperties.MERGE_FUNCTION,
            TableProperties.MERGE_FUNCTION_DEFAULT);

    if (TableProperties.MERGE_FUNCTION_REPLACE.equals(mergeFunction)) {
      return new GenericReplaceDataReader(
          table.io(),
          table.schema(),
          requiredSchema,
          primaryKeySpec,
          table.properties().get(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING),
          false,
          IdentityPartitionConverters::convertConstant,
          false,
          structLikeCollections);
    } else if (TableProperties.MERGE_FUNCTION_PARTIAL_UPDATE.equals(mergeFunction)) {
      return new GenericMergeDataReader(
          table.io(),
          table.schema(),
          requiredSchema,
          primaryKeySpec,
          table.properties().get(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING),
          false,
          IdentityPartitionConverters::convertConstant,
          false,
          structLikeCollections);
    } else {
      throw new IllegalArgumentException("unsupported merge function: " + mergeFunction);
    }
  }

  private NodeFileScanTask nodeFileScanTask(List<PrimaryKeyedFile> dataFiles) {
    List<DeleteFile> posDeleteList = input.positionDeleteForMixed();

    // Filter change files as they are included in equality delete files too.
    List<PrimaryKeyedFile> allTaskFiles = dataFiles.stream()
        .filter(file -> !file.type().equals(DataFileType.CHANGE_FILE))
        .collect(Collectors.toList());
    allTaskFiles.addAll(input.equalityDeleteForMixed());

    List<MixedFileScanTask> fileScanTasks =
        allTaskFiles.stream()
            .map(file -> new BasicMixedFileScanTask(file, posDeleteList, table.spec()))
            .collect(Collectors.toList());
    String nodeId = input.getOptions().get(NODE_ID);
    if (nodeId == null) {
      throw new IllegalArgumentException("Node id is null");
    }
    return new NodeFileScanTask(DataTreeNode.ofId(Long.parseLong(nodeId)), fileScanTasks);
  }

  private CloseableIterable<Record> wrapIterator2Iterable(CloseableIterator<Record> iterator) {
    return new CloseableIterable<Record>() {
      @Override
      public CloseableIterator<Record> iterator() {
        return iterator;
      }

      @Override
      public void close() throws IOException {
        iterator.close();
      }
    };
  }
}
