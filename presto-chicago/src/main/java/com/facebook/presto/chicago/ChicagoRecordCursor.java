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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.client.ChicagoAsyncClient;
import com.xjeffrose.chicago.client.ChicagoClient;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.chicago.ChicagoInternalFieldDescription.KEY_CORRUPT_FIELD;
import static com.facebook.presto.chicago.ChicagoInternalFieldDescription.KEY_FIELD;
import static com.facebook.presto.chicago.ChicagoInternalFieldDescription.KEY_LENGTH_FIELD;
import static com.facebook.presto.chicago.ChicagoInternalFieldDescription.VALUE_CORRUPT_FIELD;
import static com.facebook.presto.chicago.ChicagoInternalFieldDescription.VALUE_FIELD;
import static com.facebook.presto.chicago.ChicagoInternalFieldDescription.VALUE_LENGTH_FIELD;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ChicagoRecordCursor implements RecordCursor
{
  private static final Logger log = Logger.get(ChicagoRecordCursor.class);
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final RowDecoder keyDecoder;
  private final RowDecoder valueDecoder;
  private final Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders;
  private final Map<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoders;

  private final ChicagoSplit split;
  private final List<DecoderColumnHandle> columnHandles;
  private final ChicagoAsyncClient cc;
  private Iterator<String> keysIterator;

  private final AtomicBoolean reported = new AtomicBoolean();

  private FieldValueProvider[] fieldValueProviders;

  private String valueString;

  private long totalBytes;
  private long totalValues;

  ChicagoRecordCursor(
      RowDecoder keyDecoder,
      RowDecoder valueDecoder,
      Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders,
      Map<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoders,
      ChicagoSplit split,
      List<DecoderColumnHandle> columnHandles,
      ChicagoClientManager chicagoClientManager)
  {
    this.keyDecoder = keyDecoder;
    this.valueDecoder = valueDecoder;
    this.keyFieldDecoders = keyFieldDecoders;
    this.valueFieldDecoders = valueFieldDecoders;
    this.split = split;
    this.columnHandles = columnHandles;
    //this.chicagoClientManager = chicagoClientManager;
    // this i probably a hack. Need to see how to make it more seemless.
    this.cc = chicagoClientManager.getChicagoClient(chicagoClientManager.getChicagoConnectorConfig().getZkString());
    //this.scanParms = setScanParms(); // I don't know what to set here
    fetchKeys(); // I know I need to scan and get the keys in this place. For now just hard code to what ever we would require.
  }

  @Override
  public long getTotalBytes()
  {
    return totalBytes;
  }

  @Override
  public long getCompletedBytes()
  {
    return totalBytes;
  }

  @Override
  public long getReadTimeNanos()
  {
    return 0;
  }

  @Override
  public Type getType(int field)
  {
    checkArgument(field < columnHandles.size(), "Invalid field index");
    return columnHandles.get(field).getType();
  }

  @Override
  public boolean advanceNextPosition()
  {
    while (!keysIterator.hasNext()) {
      return endOfData();
    }
    return nextRow(keysIterator.next());
  }

  private boolean endOfData()
  {
    if (!reported.getAndSet(true)) {
      log.debug("Read a total of %d values with %d bytes.", totalValues, totalBytes);
    }
    return false;
  }

  private boolean nextRow(String keyString)
  {
    fetchData(keyString);

    byte[] keyData = keyString.getBytes(StandardCharsets.UTF_8);

    byte[] valueData = EMPTY_BYTE_ARRAY;
    if (valueString != null) {
      valueData = valueString.getBytes(StandardCharsets.UTF_8);
    }

    totalBytes += valueData.length;
    totalValues++;

    Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

    fieldValueProviders.add(KEY_FIELD.forByteValue(keyData));
    fieldValueProviders.add(VALUE_FIELD.forByteValue(valueData));
    fieldValueProviders.add(KEY_LENGTH_FIELD.forLongValue(keyData.length));
    fieldValueProviders.add(VALUE_LENGTH_FIELD.forLongValue(valueData.length));
    fieldValueProviders.add(KEY_CORRUPT_FIELD.forBooleanValue(keyDecoder.decodeRow(
        keyData,
        null,
        fieldValueProviders,
        columnHandles,
        keyFieldDecoders)));
    fieldValueProviders.add(VALUE_CORRUPT_FIELD.forBooleanValue(valueDecoder.decodeRow(
        valueData,
        null, //valueMap,
        fieldValueProviders,
        columnHandles,
        valueFieldDecoders)));

    this.fieldValueProviders = new FieldValueProvider[columnHandles.size()];

    // If a value provider for a requested internal column is present, assign the
    // value to the internal cache. It is possible that an internal column is present
    // where no value provider exists (e.g. the '_corrupt' column with the DummyRowDecoder).
    // In that case, the cache is null (and the column is reported as null).
    for (int i = 0; i < columnHandles.size(); i++) {
      for (FieldValueProvider fieldValueProvider : fieldValueProviders) {
        if (fieldValueProvider.accept(columnHandles.get(i))) {
          this.fieldValueProviders[i] = fieldValueProvider;
          break;
        }
      }
    }

    // Advanced successfully.
    return true;
  }

  @SuppressWarnings("SimplifiableConditionalExpression")
  @Override
  public boolean getBoolean(int field)
  {
    checkArgument(field < columnHandles.size(), "Invalid field index");

    checkFieldType(field, boolean.class);
    return isNull(field) ? false : fieldValueProviders[field].getBoolean();
  }

  @Override
  public long getLong(int field)
  {
    checkArgument(field < columnHandles.size(), "Invalid field index");

    checkFieldType(field, long.class);
    return isNull(field) ? 0L : fieldValueProviders[field].getLong();
  }

  @Override
  public double getDouble(int field)
  {
    checkArgument(field < columnHandles.size(), "Invalid field index");

    checkFieldType(field, double.class);
    return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
  }

  @Override
  public Slice getSlice(int field)
  {
    checkArgument(field < columnHandles.size(), "Invalid field index");

    checkFieldType(field, Slice.class);
    return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
  }

  @Override
  public boolean isNull(int field)
  {
    checkArgument(field < columnHandles.size(), "Invalid field index");

    return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
  }

  @Override
  public Object getObject(int field)
  {
    checkArgument(field < columnHandles.size(), "Invalid field index");

    throw new IllegalArgumentException(format("Type %s is not supported", getType(field)));
  }

  private void checkFieldType(int field, Class<?> expected)
  {
    Class<?> actual = getType(field).getJavaType();
    checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
  }

  @Override
  public void close()
  {
  }

  /**
   * Now this is where I need the API. I need to get the keys for that particular colFam
   * I got the API ready.
   * I need to use the ZKClient from the ChicagoClient to get the data
   * But probably need to check with JEff about exposing the ZKClient from chicagoclient.
   * @return
   */
  private boolean fetchKeys()
  {
    try {
      switch (split.getKeyDataType()) {
        case STRING: {
          ListenableFuture<byte[]> f = cc.scanKeys(this.split.getTableName().getBytes());
          byte[] b = f.get();
          String resp = new String(b);
          List<String> keys = Arrays.asList(resp.split("\0")[0].split("@@@"));
          keysIterator = keys.iterator();
        }
        break;
        default:
          log.debug("Chicago type of key %s is unsupported", split.getKeyDataFormat());
          return false;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return true;
  }

  private boolean fetchData(String keyString)
  {
    valueString = null;
    log.info("--------------");
    log.info(keyString);
    log.info("--------------");
    log.info(split.getKeyDataFormat());
    log.info("--------------");
    log.info(split.getValueDataFormat());
    try {
      switch (split.getValueDataType()) {
        case STRING:
          ListenableFuture<byte[]> resp = cc.read(split.getTableName().getBytes(), keyString.getBytes());
          valueString = Arrays.asList(new String(resp.get()).split("@@@")).toString();
          if (valueString == null) {
            log.warn("Chicago data modified while query was running, string value at key %s deleted", keyString);
            return false;
          }
          break;
        default:
          log.debug("Chicago type for key %s is unsupported", keyString);
          return false;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return true;
  }
}
