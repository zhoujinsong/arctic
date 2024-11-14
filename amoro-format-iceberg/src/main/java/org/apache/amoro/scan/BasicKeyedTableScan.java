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

package org.apache.amoro.scan;

import org.apache.amoro.data.DataTreeNode;
import org.apache.amoro.data.DefaultKeyedFile;
import org.apache.amoro.scan.expressions.BasicPartitionEvaluator;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.BasicKeyedTable;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.utils.FileScanTaskUtil;
import org.apache.amoro.utils.MixedTableUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Basic implementation of {@link KeyedTableScan}, including the merge-on-read plan logical */
public class BasicKeyedTableScan implements KeyedTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BasicKeyedTableScan.class);

  private final BasicKeyedTable table;
  private final List<NodeFileScanTask> fileScanTasks = new ArrayList<>();
  private Expression expression;

  public BasicKeyedTableScan(BasicKeyedTable table) {
    this.table = table;
  }

  /**
   * Config this scan with filter by the {@link Expression}. For Change Table, only filters related
   * to partition will take effect.
   *
   * @param expr a filter expression
   * @return scan based on this with results filtered by the expression
   */
  @Override
  public KeyedTableScan filter(Expression expr) {
    if (expression == null) {
      expression = expr;
    } else {
      expression = Expressions.and(expr, expression);
    }
    return this;
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    // base file
    CloseableIterable<MixedFileScanTask> baseFileList;
    baseFileList = planBaseFiles();

    // change file
    CloseableIterable<MixedFileScanTask> changeFileList;
    if (table.primaryKeySpec().primaryKeyExisted()) {
      changeFileList = planChangeFiles();
    } else {
      changeFileList = CloseableIterable.empty();
    }

    StructLikeMap<Collection<MixedFileScanTask>> partitionedFiles =
        groupFilesByPartition(table.spec(), changeFileList, baseFileList);
    partitionedFiles.forEach(this::partitionPlan);
    return CloseableIterable.transform(
        CloseableIterable.withNoopClose(fileScanTasks), BaseCombinedScanTask::new);
  }

  private CloseableIterable<MixedFileScanTask> planBaseFiles() {
    TableScan scan = table.baseTable().newScan();
    if (this.expression != null) {
      scan = scan.filter(this.expression);
    }
    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();
    return CloseableIterable.transform(
        fileScanTasks,
        fileScanTask ->
            new BasicMixedFileScanTask(
                DefaultKeyedFile.parseBase(fileScanTask.file()),
                fileScanTask.deletes(),
                fileScanTask.spec(),
                expression));
  }

  private CloseableIterable<MixedFileScanTask> planChangeFiles() {
    StructLikeMap<Long> partitionOptimizedSequence = MixedTableUtil.readOptimizedSequence(table);
    Expression partitionExpressions = Expressions.alwaysTrue();
    if (expression != null) {
      // Only push down filters related to partition
      partitionExpressions = new BasicPartitionEvaluator(table.spec()).project(expression);
    }

    ChangeTableIncrementalScan changeTableScan =
        table.changeTable().newScan().fromSequence(partitionOptimizedSequence);

    changeTableScan = changeTableScan.filter(partitionExpressions);

    return CloseableIterable.transform(changeTableScan.planFiles(), s -> (MixedFileScanTask) s);
  }

  /**
   * Construct tree node task for every partition:
   *
   * <ol>
   *   <li>Group file scan task by node
   *   <li>Reconstruct node file scan task according the read level
   * </ol>
   */
  private void partitionPlan(StructLike partition, Collection<MixedFileScanTask> keyedTableTasks) {
    Map<DataTreeNode, NodeFileScanTask> originalNodeFileScanTaskMap = new HashMap<>();
    // plan files cannot guarantee the uniqueness of the file,
    // so Set<path> here is used to remove duplicate files
    Set<String> pathSets = new HashSet<>();
    keyedTableTasks.forEach(
        task -> {
          if (!pathSets.contains(task.file().path().toString())) {
            pathSets.add(task.file().path().toString());
            DataTreeNode treeNode = task.file().node();
            NodeFileScanTask nodeFileScanTask =
                originalNodeFileScanTaskMap.computeIfAbsent(treeNode, NodeFileScanTask::new);
            nodeFileScanTask.addFile(task);
          }
        });

    // Split the scan tasks based on the writing bucket of the current BaseStore,
    // which should be evaluated according to the data size of the table.
    int scanBucketCount =
        PropertyUtil.propertyAsInt(
            table.properties(),
            TableProperties.BASE_FILE_INDEX_HASH_BUCKET,
            TableProperties.BASE_FILE_INDEX_HASH_BUCKET_DEFAULT);

    Map<DataTreeNode, NodeFileScanTask> result =
        FileScanTaskUtil.generateScanTreeNodes(scanBucketCount).stream()
            .collect(Collectors.toMap(n -> n, NodeFileScanTask::new));

    originalNodeFileScanTaskMap.forEach(
        (treeNode, nodeFileScanTask) ->
            result.forEach(
                (treeNode1, nodeFileScanTask1) -> {
                  if (treeNode1.equals(treeNode)
                      || treeNode1.isSonOf(treeNode)
                      || treeNode.isSonOf(treeNode1)) {
                    result.get(treeNode1).addTasks(nodeFileScanTask.dataTasks());
                  }
                }));

    fileScanTasks.addAll(
        result.values().stream().filter(NodeFileScanTask::isDataNode).collect(Collectors.toList()));
  }

  public StructLikeMap<Collection<MixedFileScanTask>> groupFilesByPartition(
      PartitionSpec partitionSpec,
      CloseableIterable<MixedFileScanTask> changeTasks,
      CloseableIterable<MixedFileScanTask> baseTasks) {
    StructLikeMap<Collection<MixedFileScanTask>> filesGroupedByPartition =
        StructLikeMap.create(partitionSpec.partitionType());
    try {
      changeTasks.forEach(
          task ->
              filesGroupedByPartition
                  .computeIfAbsent(task.file().partition(), k -> Lists.newArrayList())
                  .add(task));
      baseTasks.forEach(
          task ->
              filesGroupedByPartition
                  .computeIfAbsent(task.file().partition(), k -> Lists.newArrayList())
                  .add(task));
      return filesGroupedByPartition;
    } finally {
      try {
        changeTasks.close();
        baseTasks.close();
      } catch (IOException e) {
        LOG.warn("Failed to close table scan of {} ", table.id(), e);
      }
    }
  }
}
