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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import static com.facebook.presto.chicago.ChicagoHandleResolver.convertLayout;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Chicago specific implementation of {@link ConnectorSplitManager}.
 */
public class ChicagoSplitManager implements ConnectorSplitManager
{
  private final String connectorId;
  private final ChicagoConnectorConfig chicagoConnectorConfig;

  @Inject
  public ChicagoSplitManager(
      ChicagoConnectorId connectorId,
      ChicagoConnectorConfig chicagoConnectorConfig)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.chicagoConnectorConfig = requireNonNull(chicagoConnectorConfig, "chicagoConfig is null");
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorSession session, ConnectorTableLayoutHandle layout)
  {
    /**
     * This is probably not needed
     * Because the fetching the data would be taken care among splits by zk and chicago server
     * So we don't have to worry about the splits.
     * May be we can have some discussions with J.R
     */
    ChicagoTableHandle chicagoTableHandle = convertLayout(layout).getTable();

    String nodes = chicagoConnectorConfig.getZkString();

    checkState(nodes == null, "ZKString  no longer exists");
    ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

    ChicagoSplit split = new ChicagoSplit(connectorId,
        chicagoTableHandle.getSchemaName(),
        chicagoTableHandle.getTableName(),
        chicagoTableHandle.getKeyDataFormat(),
        chicagoTableHandle.getValueDataFormat(),
        chicagoTableHandle.getKeyName());

    builder.add(split);

    return new FixedSplitSource(builder.build());
  }
}
