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

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.xjeffrose.chicago.client.ChicagoClient;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Chicago nodes
 */
public class ChicagoClientManager
{
  private static final Logger log = Logger.get(ChicagoClientManager.class);

  private final LoadingCache<String, ChicagoClient> chicagoClientCache;

  private final ChicagoConnectorConfig chicagoConnectorConfig;

  @Inject
  ChicagoClientManager(ChicagoConnectorConfig chicagoConnectorConfig)
  {
    this.chicagoConnectorConfig = requireNonNull(chicagoConnectorConfig, "chicagoConfig is null");
    this.chicagoClientCache = CacheBuilder.newBuilder().build(new ChicagoClientLoader());
  }

  @PreDestroy
  public void tearDown()
  {
    for (Map.Entry<String, ChicagoClient> entry : chicagoClientCache.asMap().entrySet()) {
      try {
        entry.getValue().stop();
      }
      catch (Exception e) {
        log.warn(e, "While stopping chicago client %s:", entry.getKey());
      }
    }
  }

  public ChicagoConnectorConfig getChicagoConnectorConfig()
  {
    return chicagoConnectorConfig;
  }

  public ChicagoClient getChicagoClient(String host)
  {
    requireNonNull(host, "host is null");
    try {
      return chicagoClientCache.get(host);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  private class ChicagoClientLoader extends CacheLoader<String, ChicagoClient>
  {
    @Override
    public ChicagoClient load(String zkConnectionString) throws Exception
    {
      log.info("Creating new ChicagoClient for %s", zkConnectionString);
      /**
       * I don't know if this is the right way to load the colFamilies as table names
       * I don't know if this happens in the start. i.e., show tables;
       */
      ChicagoClient cc = new ChicagoClient(zkConnectionString, chicagoConnectorConfig.getQuorumSize());
      cc.startAndWaitForNodes(chicagoConnectorConfig.getQuorumSize());
      List<String> tables = cc.scanColFamily();
      chicagoConnectorConfig.setTableNames(tables.toString());
      return cc;
    }
  }
}
