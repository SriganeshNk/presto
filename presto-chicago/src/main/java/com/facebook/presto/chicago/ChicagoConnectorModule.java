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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

/**
 * Guice module for the Redis connector.
 */
public class ChicagoConnectorModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(ChicagoConnector.class).in(Scopes.SINGLETON);

    binder.bind(ChicagoMetadata.class).in(Scopes.SINGLETON);
    binder.bind(ChicagoSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(ChicagoRecordSetProvider.class).in(Scopes.SINGLETON);

    binder.bind(ChicagoClientManager.class).in(Scopes.SINGLETON);

    configBinder(binder).bindConfig(ChicagoConnectorConfig.class);

    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    jsonCodecBinder(binder).bindJsonCodec(ChicagoTableDescription.class);

    binder.install(new ChicagoDecoderModule());

    for (ChicagoInternalFieldDescription internalFieldDescription : ChicagoInternalFieldDescription.getInternalFields()) {
      bindInternalColumn(binder, internalFieldDescription);
    }
  }

  private static void bindInternalColumn(Binder binder, ChicagoInternalFieldDescription fieldDescription)
  {
    Multibinder<ChicagoInternalFieldDescription> fieldDescriptionBinder = Multibinder.newSetBinder(binder, ChicagoInternalFieldDescription.class);
    fieldDescriptionBinder.addBinding().toInstance(fieldDescription);
  }

  public static final class TypeDeserializer extends FromStringDeserializer<Type>
  {
    private static final long serialVersionUID = 1L;

    private final TypeManager typeManager;

    @Inject
    public TypeDeserializer(TypeManager typeManager)
    {
      super(Type.class);
      this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected Type _deserialize(String value, DeserializationContext context)
    {
      Type type = typeManager.getType(parseTypeSignature(value));
      checkArgument(type != null, "Unknown type %s", value);
      return type;
    }
  }
}
