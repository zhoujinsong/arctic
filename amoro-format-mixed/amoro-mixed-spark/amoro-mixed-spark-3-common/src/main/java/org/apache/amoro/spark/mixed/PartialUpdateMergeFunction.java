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

package org.apache.amoro.spark.mixed;

import org.apache.amoro.io.reader.MergeFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class PartialUpdateMergeFunction implements MergeFunction<InternalRow> {

  private static final PartialUpdateMergeFunction INSTANCE = new PartialUpdateMergeFunction();

  public static PartialUpdateMergeFunction getInstance() {
    return INSTANCE;
  }

  @Override
  public InternalRow merge(InternalRow record, InternalRow update) {
    return UpdatedRow.of(record, update);
  }

  static class UpdatedRow extends InternalRow {

    private final InternalRow originalRow;
    private final InternalRow updateRow;

    private UpdatedRow(InternalRow originalRow, InternalRow updateRow) {
      this.originalRow = originalRow;
      this.updateRow = updateRow;
    }

    public static UpdatedRow of(InternalRow originalRow, InternalRow updateRow) {
      return new UpdatedRow(originalRow, updateRow);
    }

    @Override
    public int numFields() {
      return originalRow.numFields();
    }

    @Override
    public void setNullAt(int i) {
      originalRow.setNullAt(i);
      updateRow.setNullAt(i);
    }

    @Override
    public void update(int i, Object value) {
      updateRow.update(i, value);
    }

    @Override
    public InternalRow copy() {
      return new UpdatedRow(originalRow, updateRow);
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return originalRow.isNullAt(ordinal) && updateRow.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getBoolean(ordinal)
          : updateRow.getBoolean(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getByte(ordinal)
          : updateRow.getByte(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getShort(ordinal)
          : updateRow.getShort(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
      return updateRow.isNullAt(ordinal) ? originalRow.getInt(ordinal) : updateRow.getInt(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getLong(ordinal)
          : updateRow.getLong(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getFloat(ordinal)
          : updateRow.getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getDouble(ordinal)
          : updateRow.getDouble(ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getDecimal(ordinal, precision, scale)
          : updateRow.getDecimal(ordinal, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getUTF8String(ordinal)
          : updateRow.getUTF8String(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getBinary(ordinal)
          : updateRow.getBinary(ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getInterval(ordinal)
          : updateRow.getInterval(ordinal);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getStruct(ordinal, numFields)
          : updateRow.getStruct(ordinal, numFields);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.getArray(ordinal)
          : updateRow.getArray(ordinal);
    }

    @Override
    public MapData getMap(int ordinal) {
      return updateRow.isNullAt(ordinal) ? originalRow.getMap(ordinal) : updateRow.getMap(ordinal);
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      return updateRow.isNullAt(ordinal)
          ? originalRow.get(ordinal, dataType)
          : updateRow.get(ordinal, dataType);
    }
  }
}
