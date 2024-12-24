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

import org.apache.amoro.data.DataFileType;
import org.apache.amoro.data.DataTreeNode;
import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.utils.FileScanTaskUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Implementation of {@link KeyedTableScanTask} with files in one single {@link DataTreeNode} */
public class NodeFileScanTask implements KeyedTableScanTask {
  private static final Logger LOG = LoggerFactory.getLogger(NodeFileScanTask.class);

  private List<MixedFileScanTask> baseTasks = new ArrayList<>();
  private List<MixedFileScanTask> insertTasks = new ArrayList<>();
  private List<MixedFileScanTask> deleteTasks = new ArrayList<>();
  private List<MixedFileScanTask> changeTasks = new ArrayList<>();
  private long cost = 0;
  private final long openFileCost = TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
  private DataTreeNode treeNode;
  private long rowNums = 0;
  private boolean includeChangeDataRecords = true;

  public NodeFileScanTask() {}

  public NodeFileScanTask(DataTreeNode treeNode) {
    this.treeNode = treeNode;
  }

  public NodeFileScanTask(DataTreeNode treeNode, List<MixedFileScanTask> allTasks) {
    this.treeNode = treeNode;
    Map<DataFileType, List<MixedFileScanTask>> tasksByType =
        allTasks.stream().collect(Collectors.groupingBy(t -> t.file().type()));

    this.baseTasks = tasksByType.getOrDefault(DataFileType.BASE_FILE, Collections.emptyList());
    this.changeTasks = tasksByType.getOrDefault(DataFileType.CHANGE_FILE, Collections.emptyList());
    this.insertTasks = tasksByType.getOrDefault(DataFileType.INSERT_FILE, Collections.emptyList());
    this.deleteTasks =
        tasksByType.getOrDefault(DataFileType.EQ_DELETE_FILE, Collections.emptyList());

    allTasks.forEach(
        task -> {
          cost = cost + Math.max(task.file().fileSizeInBytes(), openFileCost);
          rowNums = rowNums + task.file().recordCount();
        });
  }

  public void setTreeNode(DataTreeNode treeNode) {
    this.treeNode = treeNode;
  }

  public long cost() {
    return cost;
  }

  public long recordCount() {
    return rowNums;
  }

  @Override
  public List<MixedFileScanTask> baseTasks() {
    return baseTasks;
  }

  @Override
  public List<MixedFileScanTask> changeTasks() {
    return changeTasks;
  }

  @Override
  public List<MixedFileScanTask> insertTasks() {
    return insertTasks;
  }

  @Override
  public List<MixedFileScanTask> mixedEquityDeletes() {
    return deleteTasks;
  }

  @Override
  public List<MixedFileScanTask> dataTasks() {
    return Stream.of(baseTasks, insertTasks, changeTasks)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  @Override
  public List<MixedFileScanTask> deleteTasks() {
    return Stream.of(changeTasks, deleteTasks).flatMap(List::stream).collect(Collectors.toList());
  }

  public void addFile(MixedFileScanTask task) {

    DataFileType fileType = task.fileType();
    if (fileType == null) {
      throw new IllegalArgumentException("file type is null");
    }
    if (fileType == DataFileType.BASE_FILE || fileType == DataFileType.INSERT_FILE) {
      cost = cost + Math.max(task.file().fileSizeInBytes(), openFileCost);
      rowNums = rowNums + task.file().recordCount();
    }
    switch (fileType) {
      case BASE_FILE:
        baseTasks.add(task);
        break;
      case INSERT_FILE:
        insertTasks.add(task);
        break;
      case EQ_DELETE_FILE:
        deleteTasks.add(task);
        break;
      case CHANGE_FILE:
        changeTasks.add(task);
        break;
      default:
        LOG.warn("file type is {}, not add in node", fileType);
        // ignore the object
    }
  }

  public void addTasks(List<MixedFileScanTask> files) {
    files.forEach(this::addFile);
  }

  public Boolean isDataNode() {
    return !baseTasks.isEmpty() || !insertTasks.isEmpty() || !changeTasks.isEmpty();
  }

  public DataTreeNode treeNode() {
    return treeNode;
  }

  public boolean isIncludeChangeDataRecords() {
    return includeChangeDataRecords;
  }

  public void setIncludeChangeDataRecords(boolean includeChangeDataRecords) {
    this.includeChangeDataRecords = includeChangeDataRecords;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("\nbaseTasks", FileScanTaskUtil.toString(baseTasks))
        .add("\ninsertTasks", FileScanTaskUtil.toString(insertTasks))
        .add("\ndeleteTasks", FileScanTaskUtil.toString(deleteTasks))
        .add("\nchangeTasks", FileScanTaskUtil.toString(changeTasks))
        .add("\ntreeNode", treeNode)
        .add("\nincludeChangeDataRecords", includeChangeDataRecords)
        .toString();
  }
}
