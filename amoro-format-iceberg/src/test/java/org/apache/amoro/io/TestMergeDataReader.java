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

package org.apache.amoro.io;

import static org.apache.amoro.table.TableProperties.BASE_FILE_INDEX_HASH_BUCKET;
import static org.apache.amoro.table.TableProperties.CHANGE_FILE_INDEX_HASH_BUCKET;

import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableTestHelper;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.catalog.CatalogTestHelper;
import org.apache.amoro.catalog.TableTestBase;
import org.apache.amoro.data.ChangeAction;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestMergeDataReader extends TableTestBase {

  @Parameterized.Parameters(name = "{0}, {1}")
  public static Object[] parameters() {
    return new Object[][] {
      {
        new BasicCatalogTestHelper(TableFormat.MIXED_ICEBERG),
        new BasicTableTestHelper(true, true, buildTableProperties(FileFormat.PARQUET, 4, 4))
      },
      {
        new BasicCatalogTestHelper(TableFormat.MIXED_ICEBERG),
        new BasicTableTestHelper(true, true, buildTableProperties(FileFormat.ORC, 4, 4))
      },
      {
        new BasicCatalogTestHelper(TableFormat.MIXED_ICEBERG),
        new BasicTableTestHelper(true, true, buildTableProperties(FileFormat.PARQUET, 4, 8))
      },
      {
        new BasicCatalogTestHelper(TableFormat.MIXED_ICEBERG),
        new BasicTableTestHelper(true, true, buildTableProperties(FileFormat.PARQUET, 8, 4))
      },
    };
  }

  public TestMergeDataReader(CatalogTestHelper catalogTestHelper, TableTestHelper tableTestHelper) {
    super(catalogTestHelper, tableTestHelper);
  }

  @Before
  public void initData() {
    List<Record> baseRecords = Lists.newArrayListWithCapacity(6);
    baseRecords.add(tableTestHelper().generateTestRecord(1, "john", 0, "2022-01-01T12:00:00"));
    baseRecords.add(tableTestHelper().generateTestRecord(2, "lily", 0, "2022-01-02T12:00:00"));
    baseRecords.add(tableTestHelper().generateTestRecord(3, "jake", 0, "2022-01-03T12:00:00"));
    baseRecords.add(tableTestHelper().generateTestRecord(4, "sam", 0, "2022-01-04T12:00:00"));
    baseRecords.add(tableTestHelper().generateTestRecord(5, "mary", 0, "2022-01-01T12:00:00"));
    baseRecords.add(tableTestHelper().generateTestRecord(6, "mack", 0, "2022-01-01T12:00:00"));

    // write base with transaction id:1, (id=1),(id=2),(id=3),(id=4),(id=5),(id=6)
    List<DataFile> baseFiles =
        tableTestHelper().writeBaseStore(getMixedTable().asKeyedTable(), 1L, baseRecords, false);
    DataFile dataFileForPositionDelete =
        baseFiles.stream()
            .filter(s -> s.path().toString().contains("op_time_day=2022-01-04"))
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Cannot find data file to delete"));
    AppendFiles baseAppend = getMixedTable().asKeyedTable().baseTable().newAppend();
    baseFiles.forEach(baseAppend::appendFile);
    baseAppend.commit();

    // write position with transaction id:4, (id=4)
    DeleteFile posDeleteFiles =
        MixedDataTestHelpers.writeBaseStorePosDelete(
                getMixedTable(), 4L, dataFileForPositionDelete, Collections.singletonList(0L))
            .stream()
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Cannot get delete file from writer"));

    getMixedTable().asKeyedTable().baseTable().newRowDelta().addDeletes(posDeleteFiles).commit();

    List<Record> changeRecords = Lists.newArrayListWithCapacity(2);
    changeRecords.add(tableTestHelper().generateTestRecord(1, "john1", 0, "2022-01-01T12:00:00"));
    changeRecords.add(tableTestHelper().generateTestRecord(2, null, 1, "2022-01-02T12:00:00"));

    // write change insert with transaction id:2, (id=1),(id=2)
    writeChangeStore(2L, ChangeAction.INSERT, changeRecords);
  }

  protected void writeChangeStore(Long txId, ChangeAction insert, List<Record> records) {
    List<DataFile> insertFiles =
        tableTestHelper()
            .writeChangeStore(getMixedTable().asKeyedTable(), txId, insert, records, false);
    AppendFiles changeAppendInsert = getMixedTable().asKeyedTable().changeTable().newAppend();
    insertFiles.forEach(changeAppendInsert::appendFile);
    changeAppendInsert.commit();
  }

  @Test
  public void testReadData() {
    Set<Record> records =
        Sets.newHashSet(
            tableTestHelper()
                .readKeyedTable(
                    getMixedTable().asKeyedTable(), Expressions.alwaysTrue(), null, false, false));
    // expect: (id=1),(id=2),(id=3),(id=5),(id=6)
    Set<Record> expectRecords = Sets.newHashSet();
    expectRecords.add(tableTestHelper().generateTestRecord(1, "john1", 0, "2022-01-01T12:00:00"));
    expectRecords.add(tableTestHelper().generateTestRecord(2, "lily", 1, "2022-01-02T12:00:00"));
    expectRecords.add(tableTestHelper().generateTestRecord(3, "jake", 0, "2022-01-03T12:00:00"));
    expectRecords.add(tableTestHelper().generateTestRecord(5, "mary", 0, "2022-01-01T12:00:00"));
    expectRecords.add(tableTestHelper().generateTestRecord(6, "mack", 0, "2022-01-01T12:00:00"));
    Assert.assertEquals(expectRecords, records);
  }

  @Test
  public void testReadDeletedData() {
    Set<Record> records =
        Sets.newHashSet(
            tableTestHelper()
                .readKeyedTable(
                    getMixedTable().asKeyedTable(), Expressions.alwaysTrue(), null, false, true));
    // expect: (id=4),(id=5),(id=6)
    Set<Record> expectRecords = Sets.newHashSet();
    expectRecords.add(tableTestHelper().generateTestRecord(1, "john", 0, "2022-01-01T12:00:00"));
    expectRecords.add(tableTestHelper().generateTestRecord(2, "lily", 0, "2022-01-02T12:00:00"));
    expectRecords.add(tableTestHelper().generateTestRecord(4, "sam", 0, "2022-01-04T12:00:00"));
    Assert.assertEquals(expectRecords, records);
  }

  private static Map<String, String> buildTableProperties(
      FileFormat fileFormat, int changeBucket, int baseBucket) {
    Map<String, String> tableProperties = BasicTableTestHelper.buildTableFormat(fileFormat.name());
    tableProperties.put(
        org.apache.amoro.table.TableProperties.MERGE_FUNCTION,
        org.apache.amoro.table.TableProperties.MERGE_FUNCTION_PARTIAL_UPDATE);
    tableProperties.put(BASE_FILE_INDEX_HASH_BUCKET, String.valueOf(baseBucket));
    tableProperties.put(CHANGE_FILE_INDEX_HASH_BUCKET, String.valueOf(changeBucket));
    return tableProperties;
  }
}
