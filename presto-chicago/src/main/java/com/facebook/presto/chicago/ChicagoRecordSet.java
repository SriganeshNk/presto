/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.chicago;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Redis specific record set. Returns a cursor for a table which iterates over a Redis values.
 */
public class ChicagoRecordSet implements RecordSet
{
  private final ChicagoSplit split;
  private final ChicagoClientManager chicagoClientManager;

  private final RowDecoder keyDecoder;
  private final RowDecoder valueDecoder;
  private final Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders;
  private final Map<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoders;

  private final List<DecoderColumnHandle> columnHandles;
  private final List<Type> columnTypes;

  ChicagoRecordSet(
      ChicagoSplit split,
      ChicagoClientManager chicagoClientManager,
      List<DecoderColumnHandle> columnHandles,
      RowDecoder keyDecoder,
      RowDecoder valueDecoder,
      Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders,
      Map<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoders)
  {
    this.split = requireNonNull(split, "split is null");

    this.chicagoClientManager = requireNonNull(chicagoClientManager, "chicago Client Manager is null");

    this.keyDecoder = requireNonNull(keyDecoder, "keyDecoder is null");
    this.valueDecoder = requireNonNull(valueDecoder, "valueDecoder is null");
    this.keyFieldDecoders = requireNonNull(keyFieldDecoders, "keyFieldDecoders is null");
    this.valueFieldDecoders = requireNonNull(valueFieldDecoders, "valueFieldDecoders is null");
    this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

    ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
    for (DecoderColumnHandle handle : columnHandles) {
      typeBuilder.add(handle.getType());
    }
    this.columnTypes = typeBuilder.build();
  }

  @Override
  public List<Type> getColumnTypes()
  {
    return columnTypes;
  }

  @Override
  public RecordCursor cursor()
  {
    return new ChicagoRecordCursor(keyDecoder, valueDecoder, keyFieldDecoders, valueFieldDecoders, split, columnHandles, chicagoClientManager);
  }
}
