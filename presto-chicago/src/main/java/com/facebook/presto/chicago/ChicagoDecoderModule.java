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

import com.facebook.presto.decoder.DecoderModule;
import com.facebook.presto.decoder.DecoderRegistry;
import com.facebook.presto.decoder.csv.CsvDecoderModule;
import com.facebook.presto.decoder.dummy.DummyDecoderModule;
import com.facebook.presto.decoder.json.JsonDecoderModule;
import com.facebook.presto.decoder.raw.RawDecoderModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

/**
 * Chicago decoder specific module. Installs the registry and all known decoder submodules.
 */
public class ChicagoDecoderModule extends DecoderModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(DecoderRegistry.class).in(Scopes.SINGLETON);

    binder.install(new DummyDecoderModule());
    binder.install(new CsvDecoderModule());
    binder.install(new JsonDecoderModule());
    binder.install(new RawDecoderModule());
    /**
     * They had the hash and set decoder module for the REDIS. we don't have anything
     * Let me see if the BYTE_ARRAY module might require some decoding.
     * First need to check where the type check happens.
     */
  }
}
