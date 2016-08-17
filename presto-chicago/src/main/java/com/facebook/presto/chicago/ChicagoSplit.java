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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a Chicago specific {@link ConnectorSplit}.
 */
public final class ChicagoSplit implements ConnectorSplit
{
  private final String connectorId;
  private final String schemaName;
  private final String tableName; // This will be the colFam Name
  private final String keyDataFormat;
  private final String keyName;
  private final String valueDataFormat;

  private final ChicagoDataType valueDataType;
  private final ChicagoDataType keyDataType;

  @JsonCreator
  public ChicagoSplit(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("keyDataFormat") String keyDataFormat,
      @JsonProperty("valueDataFormat") String valueDataFormat,
      @JsonProperty("keyName") String keyName)
  {
    this.connectorId = requireNonNull(connectorId, "connector id is null");
    this.schemaName = requireNonNull(schemaName, "schemaName is null");
    this.tableName = requireNonNull(tableName, "dataFormat is null");
    this.keyDataFormat = requireNonNull(keyDataFormat, "KeydataFormat is null");
    this.valueDataFormat = requireNonNull(valueDataFormat, "valueDataFormat is null");
    this.keyName = keyName;
    this.valueDataType = toChicagoDataType(valueDataFormat);
    this.keyDataType = toChicagoDataType(keyDataFormat);
  }

  @JsonProperty
  public String getConnectorId()
  {
    return connectorId;
  }

  @JsonProperty
  public String getSchemaName()
  {
    return schemaName;
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }

  @JsonProperty
  public String getKeyDataFormat()
  {
    return keyDataFormat;
  }

  @JsonProperty
  public String getValueDataFormat()
  {
    return valueDataFormat;
  }

  @JsonProperty
  public String getKeyName()
  {
    return keyName;
  }

  public ChicagoDataType getValueDataType()
  {
    return valueDataType;
  }

  public ChicagoDataType getKeyDataType()
  {
    return keyDataType;
  }

  @Override
  public boolean isRemotelyAccessible()
  {
    return true;
  }

  @Override
  public List<HostAddress> getAddresses()
  {
    return null;  /** Need to see how to resolve this */
  }

  @Override
  public Object getInfo()
  {
    return this;
  }

  private static ChicagoDataType toChicagoDataType(String dataFormat)
  {
    return ChicagoDataType.STRING;
  }

  @Override
  public String toString()
  {
    return toStringHelper(this)
        .add("connectorId", connectorId)
        .add("schemaName", schemaName)
        .add("tableName", tableName)
        .add("keyDataFormat", keyDataFormat)
        .add("valueDataFormat", valueDataFormat)
        .add("keyName", keyName)
        .toString();
  }
}
