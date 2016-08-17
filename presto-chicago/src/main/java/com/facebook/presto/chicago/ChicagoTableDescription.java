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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Json description to parse a Redis key/value pairs. See the documentation for the exact JSON syntax.
 */
public class ChicagoTableDescription
{
  private final String tableName;
  private final String schemaName;
  private final ChicagoTableFieldGroup key;
  private final ChicagoTableFieldGroup value;

  @JsonCreator
  public ChicagoTableDescription(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("key") ChicagoTableFieldGroup key,
      @JsonProperty("value") ChicagoTableFieldGroup value)
  {
    /**
     * This probably need not be true, you could make this to point to the default colFamily in chicago.
     * Lets do this later.
     */
    checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
    this.tableName = tableName;
    this.schemaName = schemaName;
    this.key = key;
    this.value = value;
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }

  @JsonProperty
  public String getSchemaName()
  {
    return schemaName;
  }

  @JsonProperty
  public ChicagoTableFieldGroup getKey()
  {
    return key;
  }

  @JsonProperty
  public ChicagoTableFieldGroup getValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("schemaName", schemaName)
        .add("key", key)
        .add("value", value)
        .toString();
  }
}
