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

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ChangeDataMap<T> implements Map<StructLike, T> {

  private final MergeFunction<T> mergeFunction;
  private final StructLikeMap<T> changeMap;

  public ChangeDataMap(Types.StructType keyType, MergeFunction<T> mergeFunction) {
    this.mergeFunction = mergeFunction;
    this.changeMap = StructLikeMap.create(keyType);
  }

  @Override
  public int size() {
    return changeMap.size();
  }

  @Override
  public boolean isEmpty() {
    return changeMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return changeMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return changeMap.containsValue(value);
  }

  @Override
  public T get(Object key) {
    return changeMap.get(key);
  }

  @Nullable
  @Override
  public T put(StructLike key, T value) {
    T oldValue = get(key);
    T newValue = value;
    if (oldValue != null) {
      newValue = mergeFunction.merge(oldValue, value);
    }
    changeMap.put(key, newValue);
    return oldValue;
  }

  @Override
  public T remove(Object key) {
    return changeMap.remove(key);
  }

  @Override
  public void putAll(@NotNull Map<? extends StructLike, ? extends T> m) {
    m.forEach(this::put);
  }

  @Override
  public void clear() {
    changeMap.clear();
  }

  @NotNull
  @Override
  public Set<StructLike> keySet() {
    return changeMap.keySet();
  }

  @NotNull
  @Override
  public Collection<T> values() {
    return changeMap.values();
  }

  @NotNull
  @Override
  public Set<Entry<StructLike, T>> entrySet() {
    return changeMap.entrySet();
  }

  public Iterable<T> valuesIterable() {
    return new Iterable<T>() {
      @NotNull
      @Override
      public Iterator<T> iterator() {
        return changeMap.values().iterator();
      }
    };
  }

  public T forcePut(StructLike key, T value) {
    return changeMap.put(key, value);
  }
}
