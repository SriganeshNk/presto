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

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.chicago.ChicagoHandleResolver.convertColumnHandle;
import static com.facebook.presto.chicago.ChicagoHandleResolver.convertLayout;
import static com.facebook.presto.chicago.ChicagoHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Chicago connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link ChicagoInternalFieldDescription} for a list
 * of additional columns.
 */
public class ChicagoMetadata implements ConnectorMetadata
{
  private static final Logger log = Logger.get(ChicagoMetadata.class);

  private final String connectorId;
  private final boolean hideInternalColumns;

  private final Supplier<Map<SchemaTableName, ChicagoTableDescription>> chicagoTableDescriptionSupplier;
  private final Set<ChicagoInternalFieldDescription> internalFieldDescriptions;

  @Inject
  ChicagoMetadata(
      ChicagoConnectorId connectorId,
      ChicagoConnectorConfig chicagoConnectorConfig,
      Supplier<Map<SchemaTableName, ChicagoTableDescription>> chicagoTableDescriptionSupplier,
      Set<ChicagoInternalFieldDescription> internalFieldDescriptions)
   {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

    requireNonNull(chicagoConnectorConfig, "chicagoConfig is null");
    hideInternalColumns = chicagoConnectorConfig.isHideInternalColumns();

    log.debug("Loading chicago table definitions from %s", chicagoConnectorConfig.getTableDescriptionDir().getAbsolutePath());

    this.chicagoTableDescriptionSupplier = Suppliers.memoize(chicagoTableDescriptionSupplier);
    this.internalFieldDescriptions = requireNonNull(internalFieldDescriptions, "internalFieldDescriptions is null");
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session)
  {
    Set<String> schemas = getDefinedTables().keySet().stream()
        .map(SchemaTableName::getSchemaName)
        .collect(Collectors.toCollection(LinkedHashSet::new));

    return ImmutableList.copyOf(schemas);
  }

  @Override
  public ChicagoTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
  {
    ChicagoTableDescription table = getDefinedTables().get(schemaTableName);
    if (table == null) {
      return null;
    }

    String keyName = null;
    if (table.getKey() != null) {
      keyName = table.getKey().getName();
    }

    return new ChicagoTableHandle(
        connectorId,
        schemaTableName.getSchemaName(),
        schemaTableName.getTableName(),
        getDataFormat(table.getKey()),
        getDataFormat(table.getValue()),
        keyName);
  }

  private static String getDataFormat(ChicagoTableFieldGroup fieldGroup)
  {
    return (fieldGroup == null) ? DummyRowDecoder.NAME : fieldGroup.getDataFormat();
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
  {
    return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(
      ConnectorSession session,
      ConnectorTableHandle table,
      Constraint<ColumnHandle> constraint,
      Optional<Set<ColumnHandle>> desiredColumns)
  {
    ChicagoTableHandle tableHandle = convertTableHandle(table);

    ConnectorTableLayout layout = new ConnectorTableLayout(new ChicagoTableLayoutHandle(tableHandle));

    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
  {
    ChicagoTableLayoutHandle layout = convertLayout(handle);

    // tables in this connector have a single layout
    return getTableLayouts(session, layout.getTable(), Constraint.<ColumnHandle>alwaysTrue(), Optional.empty())
        .get(0)
        .getTableLayout();
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
  {
    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
    for (SchemaTableName tableName : getDefinedTables().keySet()) {
      if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
        builder.add(tableName);
      }
    }

    return builder.build();
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
  {
    ChicagoTableHandle redisTableHandle = convertTableHandle(tableHandle);

    ChicagoTableDescription redisTableDescription = getDefinedTables().get(redisTableHandle.toSchemaTableName());
    if (redisTableDescription == null) {
      throw new TableNotFoundException(redisTableHandle.toSchemaTableName());
    }

    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

    int index = 0;
    ChicagoTableFieldGroup key = redisTableDescription.getKey();
    if (key != null) {
      List<ChicagoTableFieldDescription> fields = key.getFields();
      if (fields != null) {
        for (ChicagoTableFieldDescription field : fields) {
          columnHandles.put(field.getName(), field.getColumnHandle(connectorId, true, index));
          index++;
        }
      }
    }

    ChicagoTableFieldGroup value = redisTableDescription.getValue();
    if (value != null) {
      List<ChicagoTableFieldDescription> fields = value.getFields();
      if (fields != null) {
        for (ChicagoTableFieldDescription field : fields) {
          columnHandles.put(field.getName(), field.getColumnHandle(connectorId, false, index));
          index++;
        }
      }
    }

    for (ChicagoInternalFieldDescription field : internalFieldDescriptions) {
      columnHandles.put(field.getName(), field.getColumnHandle(connectorId, index, hideInternalColumns));
      index++;
    }

    return columnHandles.build();
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
  {
    requireNonNull(prefix, "prefix is null");

    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

    List<SchemaTableName> tableNames;
    if (prefix.getSchemaName() == null) {
      tableNames = listTables(session, null);
    }
    else {
      tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    for (SchemaTableName tableName : tableNames) {
      ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
      // table can disappear during listing operation
      if (tableMetadata != null) {
        columns.put(tableName, tableMetadata.getColumns());
      }
    }
    return columns.build();
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
  {
    convertTableHandle(tableHandle);
    return convertColumnHandle(columnHandle).getColumnMetadata();
  }

  @VisibleForTesting
  Map<SchemaTableName, ChicagoTableDescription> getDefinedTables()
  {
    return chicagoTableDescriptionSupplier.get();
  }

  @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
  private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
  {
    ChicagoTableDescription table = getDefinedTables().get(schemaTableName);
    if (table == null) {
      throw new TableNotFoundException(schemaTableName);
    }

    ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

    appendFields(builder, table.getKey());
    appendFields(builder, table.getValue());

    for (ChicagoInternalFieldDescription fieldDescription : internalFieldDescriptions) {
      builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
    }

    return new ConnectorTableMetadata(schemaTableName, builder.build());
  }

  private static void appendFields(ImmutableList.Builder<ColumnMetadata> builder, ChicagoTableFieldGroup group)
  {
    if (group != null) {
      List<ChicagoTableFieldDescription> fields = group.getFields();
      if (fields != null) {
        for (ChicagoTableFieldDescription fieldDescription : fields) {
          builder.add(fieldDescription.getColumnMetadata());
        }
      }
    }
  }
}
