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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

public class ChicagoConnectorConfig
{
  /**
   * Might not be needed.
   */
  private static final int CHICAGO_DEFAULT_PORT = 12000;

  /**
   * Seed nodes for Chicago cluster.
   * Mostly the zkconnection string is what is needed.
   */
  private String zkString;

  /**
   * The default quorum size for the Chicago servers.
   */
  private int quorumSize = 3;

  /**
   * Count parameter for Redis scan command.
   * This is not needed. I suppose.
   * May be we might need when we get the number of column families.
   */
  private int chicagoScanCount = 100;

  /**
   * The schema name to use in the connector.
   */
  private String defaultSchema = "default";

  /**
   * Set of tables known to this connector.
   * This needs to be generated at random. [Set of column families.]
   */
  private Set<String> tableNames = ImmutableSet.of();

  /**
   * Folder holding the JSON description files for Chicago values.
   * No description files needed. I think, not sure
   */
  private File tableDescriptionDir = new File("etc/chicago/");

  /**
   * Whether internal columns are shown in table metadata or not. Default is no.
   * This should always be false. Never hide.
   */
  private boolean hideInternalColumns = true;

  @NotNull
  public String getZkString()
  {
    return this.zkString;
  }

  @Config("chicago.zkstring")
  public ChicagoConnectorConfig setZkSring(String zk)
  {
    this.zkString = zk;
    return this;
  }

  @NotNull
  public File getTableDescriptionDir()
  {
    return tableDescriptionDir;
  }

  @Config("chicago.table-description-dir")
  public ChicagoConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
  {
    this.tableDescriptionDir = tableDescriptionDir;
    return this;
  }

  /**
   * I think you shouldn't keep this is as notnull, because, we need to generate the tables.
   * @return
   */
  //  @NotNull
  public Set<String> getTableNames()
  {
    return tableNames;
  }

  @Config("chicago.table-names")
  public ChicagoConnectorConfig setTableNames(String tableNames)
  {
    this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
    return this;
  }

  @NotNull
  public String getDefaultSchema()
  {
    return defaultSchema;
  }

  @Config("chicago.default-schema")
  public ChicagoConnectorConfig setDefaultSchema(String defaultSchema)
  {
    this.defaultSchema = defaultSchema;
    return this;
  }

  @NotNull
  public String getNodes()
  {
    return this.zkString;
  }

  @Config("chicago.zkString")
  public ChicagoConnectorConfig setNodes(String nodes)
  {
    this.zkString = nodes;
    return this;
  }

  @NotNull
  public int getQuorumSize()
  {
    return this.quorumSize;
  }

  @Config("chicago.quorum")
  public ChicagoConnectorConfig setQuorumSize(int q)
  {
    this.quorumSize = q;
    return this;
  }

  public int getChicagoScanCount()
  {
    return chicagoScanCount;
  }

  @Config("chicago.scan-count")
  public ChicagoConnectorConfig setRedisScanCount(int chicagoScanCount)
  {
    this.chicagoScanCount = chicagoScanCount;
    return this;
  }

  public boolean isHideInternalColumns()
  {
    return hideInternalColumns;
  }

  @Config("chicago.hide-internal-columns")
  public ChicagoConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
  {
    this.hideInternalColumns = hideInternalColumns;
    return this;
  }
}
