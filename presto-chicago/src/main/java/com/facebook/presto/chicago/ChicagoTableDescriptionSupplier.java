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
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class ChicagoTableDescriptionSupplier implements Supplier<Map<SchemaTableName, ChicagoTableDescription>>
{
  private static final Logger log = Logger.get(ChicagoTableDescriptionSupplier.class);

  private final ChicagoConnectorConfig chicagoConnectorConfig;
  private final JsonCodec<ChicagoTableDescription> tableDescriptionCodec;

  @Inject
  ChicagoTableDescriptionSupplier(ChicagoConnectorConfig chicagoConnectorConfig, JsonCodec<ChicagoTableDescription> tableDescriptionCodec)
  {
    this.chicagoConnectorConfig = requireNonNull(chicagoConnectorConfig, "chicagoConnectorConfig is null");
    this.tableDescriptionCodec = requireNonNull(tableDescriptionCodec, "tableDescriptionCodec is null");
  }

  @Override
  public Map<SchemaTableName, ChicagoTableDescription> get()
  {
    ImmutableMap.Builder<SchemaTableName, ChicagoTableDescription> builder = ImmutableMap.builder();

    try {
      for (File file : listFiles(chicagoConnectorConfig.getTableDescriptionDir())) {
        if (file.isFile() && file.getName().endsWith(".json")) {
          ChicagoTableDescription table = tableDescriptionCodec.fromJson(Files.toByteArray(file));
          String schemaName = firstNonNull(table.getSchemaName(), chicagoConnectorConfig.getDefaultSchema());
          log.debug("Chicago table %s.%s: %s", schemaName, table.getTableName(), table);
          builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
        }
      }

      Map<SchemaTableName, ChicagoTableDescription> tableDefinitions = builder.build();

      log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

      builder = ImmutableMap.builder();
      for (String definedTable : chicagoConnectorConfig.getTableNames()) {
        SchemaTableName tableName;
        try {
          tableName = SchemaTableName.valueOf(definedTable);
        }
        catch (IllegalArgumentException iae) {
          tableName = new SchemaTableName(chicagoConnectorConfig.getDefaultSchema(), definedTable);
        }

        if (tableDefinitions.containsKey(tableName)) {
          ChicagoTableDescription chicagoTable = tableDefinitions.get(tableName);
          log.debug("Found Table definition for %s: %s", tableName, chicagoTable);
          builder.put(tableName, chicagoTable);
        }
        else {
          // A dummy table definition only supports the internal columns.
          /**
           * This is where you should bring in the description support automatically
           * based on the type of data that we are going to decode
           */
          log.debug("Created dummy Table definition for %s", tableName);
          builder.put(tableName, new ChicagoTableDescription(tableName.getTableName(),
              tableName.getSchemaName(),
              new ChicagoTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.<ChicagoTableFieldDescription>of()),
              new ChicagoTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.<ChicagoTableFieldDescription>of())));
        }
      }

      return builder.build();
    }
    catch (IOException e) {
      log.warn(e, "Error: ");
      throw Throwables.propagate(e);
    }
  }

  private static List<File> listFiles(File dir)
  {
    if ((dir != null) && dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        log.debug("Considering files: %s", asList(files));
        return ImmutableList.copyOf(files);
      }
    }
    return ImmutableList.of();
  }
}
