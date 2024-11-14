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

import org.apache.amoro.data.DataTreeNode;
import org.apache.amoro.io.AuthenticatedFileIO;
import org.apache.amoro.scan.KeyedTableScanTask;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.amoro.utils.map.StructLikeCollections;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.OrcRowReader;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Abstract implementation of mixed-format data reader consuming {@link KeyedTableScanTask}.
 *
 * @param <T> to indicate the record data type.
 */
public abstract class AbstractKeyedDataReader<T> implements Serializable {

  protected final AuthenticatedFileIO fileIO;
  protected final Schema tableSchema;
  protected final Schema projectedSchema;
  protected final String nameMapping;
  protected final boolean caseSensitive;
  protected final Set<DataTreeNode> sourceNodes;
  protected final BiFunction<Type, Object, Object> convertConstant;
  protected final PrimaryKeySpec primaryKeySpec;
  protected final boolean reuseContainer;
  protected StructLikeCollections structLikeCollections = StructLikeCollections.DEFAULT;

  public AbstractKeyedDataReader(
      AuthenticatedFileIO fileIO,
      Schema tableSchema,
      Schema projectedSchema,
      PrimaryKeySpec primaryKeySpec,
      String nameMapping,
      boolean caseSensitive,
      BiFunction<Type, Object, Object> convertConstant,
      Set<DataTreeNode> sourceNodes,
      boolean reuseContainer,
      StructLikeCollections structLikeCollections) {
    this.fileIO = fileIO;
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.primaryKeySpec = primaryKeySpec;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.convertConstant = convertConstant;
    this.sourceNodes = sourceNodes != null ? Collections.unmodifiableSet(sourceNodes) : null;
    this.reuseContainer = reuseContainer;
    if (structLikeCollections != null) {
      this.structLikeCollections = structLikeCollections;
    }
  }

  protected CloseableIterable<T> readFile(FileScanTask fileScanTask, Schema projectedSchema) {
    switch (fileScanTask.file().format()) {
      case PARQUET:
        return newParquetIterable(
            fileScanTask,
            projectedSchema,
            DataReaderCommon.getIdToConstant(fileScanTask, projectedSchema, convertConstant));
      case ORC:
        return newOrcIterable(
            fileScanTask,
            projectedSchema,
            DataReaderCommon.getIdToConstant(fileScanTask, projectedSchema, convertConstant));
      default:
        throw new UnsupportedOperationException(
            "Cannot read unknown format: " + fileScanTask.file().format());
    }
  }

  protected MixedDeleteFilter<T> createMixedDeleteFilter(
      KeyedTableScanTask keyedTableScanTask,
      Schema tableSchema,
      Schema projectedSchema,
      PrimaryKeySpec primaryKeySpec,
      Set<DataTreeNode> sourceNodes,
      StructLikeCollections structLikeCollections) {
    return new GenericMixedDeleteFilter(
        keyedTableScanTask,
        tableSchema,
        projectedSchema,
        primaryKeySpec,
        sourceNodes,
        structLikeCollections);
  }

  protected CloseableIterable<T> newParquetIterable(
      FileScanTask task, Schema schema, Map<Integer, ?> idToConstant) {
    Parquet.ReadBuilder builder =
        Parquet.read(fileIO.newInputFile(task.file().path().toString()))
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

    return fileIO.doAs(builder::build);
  }

  protected CloseableIterable<T> newOrcIterable(
      FileScanTask task, Schema schema, Map<Integer, ?> idToConstant) {
    Schema readSchemaWithoutConstantAndMetadataFields =
        TypeUtil.selectNot(
            schema,
            Sets.union(
                idToConstant.keySet(), org.apache.iceberg.MetadataColumns.metadataFieldIds()));

    ORC.ReadBuilder builder =
        ORC.read(fileIO.newInputFile(task.file().path().toString()))
            .project(readSchemaWithoutConstantAndMetadataFields)
            .split(task.start(), task.length())
            .createReaderFunc(getOrcReaderFunction(schema, idToConstant))
            .filter(task.residual())
            .caseSensitive(caseSensitive);

    if (nameMapping != null) {
      builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
    }

    return builder.build();
  }

  protected class GenericMixedDeleteFilter extends MixedDeleteFilter<T> {

    protected Function<T, StructLike> asStructLike;

    protected GenericMixedDeleteFilter(
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
      this.asStructLike =
          AbstractKeyedDataReader.this.toStructLikeFunction().apply(requiredSchema());
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

  public abstract CloseableIterator<T> readData(KeyedTableScanTask keyedTableScanTask);

  public abstract CloseableIterator<T> readDeletedData(KeyedTableScanTask keyedTableScanTask);

  protected abstract Function<MessageType, ParquetValueReader<?>> getParquetReaderFunction(
      Schema projectSchema, Map<Integer, ?> idToConstant);

  protected abstract Function<TypeDescription, OrcRowReader<?>> getOrcReaderFunction(
      Schema projectSchema, Map<Integer, ?> idToConstant);

  protected abstract Function<Schema, Function<T, StructLike>> toStructLikeFunction();
}
