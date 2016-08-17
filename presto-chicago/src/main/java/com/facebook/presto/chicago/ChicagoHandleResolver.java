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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Redis specific {@link ConnectorHandleResolver} implementation.
 */
public class ChicagoHandleResolver implements ConnectorHandleResolver
{
  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass()
  {
    return ChicagoTableHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass()
  {
    return ChicagoColumnHandle.class;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass()
  {
    return ChicagoSplit.class;
  }

  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
  {
    return ChicagoTableLayoutHandle.class;
  }

  static ChicagoTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
  {
    requireNonNull(tableHandle, "tableHandle is null");
    checkArgument(tableHandle instanceof ChicagoTableHandle, "tableHandle is not an instance of RedisTableHandle");
    return (ChicagoTableHandle) tableHandle;
  }

  static ChicagoColumnHandle convertColumnHandle(ColumnHandle columnHandle)
  {
    requireNonNull(columnHandle, "columnHandle is null");
    checkArgument(columnHandle instanceof ChicagoColumnHandle, "columnHandle is not an instance of RedisColumnHandle");
    return (ChicagoColumnHandle) columnHandle;
  }

  static ChicagoSplit convertSplit(ConnectorSplit split)
  {
    requireNonNull(split, "split is null");
    checkArgument(split instanceof ChicagoSplit, "split is not an instance of RedisSplit");
    return (ChicagoSplit) split;
  }

  static ChicagoTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout)
  {
    requireNonNull(layout, "layout is null");
    checkArgument(layout instanceof ChicagoTableLayoutHandle, "layout is not an instance of RedisTableLayoutHandle");
    return (ChicagoTableLayoutHandle) layout;
  }
}
