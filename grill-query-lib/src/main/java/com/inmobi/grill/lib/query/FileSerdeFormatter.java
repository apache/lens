package com.inmobi.grill.lib.query;

/*
 * #%L
 * Grill Query Library
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.query.InMemoryOutputFormatter;
import com.inmobi.grill.server.api.query.QueryContext;

@SuppressWarnings("deprecation")
public class FileSerdeFormatter extends WrappedFileFormatter implements InMemoryOutputFormatter {  
  private SerDe outputSerde;
  private Converter converter;
  private ObjectInspector convertedOI;

  public FileSerdeFormatter() { 
  }

  public void init(QueryContext ctx, GrillResultSetMetadata metadata) throws IOException {
    super.init(ctx, metadata);
    initOutputSerde();
  }

  @SuppressWarnings("unchecked")
  private void initOutputSerde() {
    try {
      outputSerde = ReflectionUtils.newInstance(ctx.getConf().getClass(
          GrillConfConstants.QUERY_OUTPUT_SERDE,
          (Class<? extends AbstractSerDe>)Class.forName(GrillConfConstants.DEFAULT_OUTPUT_SERDE),
          SerDe.class), ctx.getConf());

      Properties props = new Properties();
      if (columnNames.size() > 0) {
        props.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(columnNames, ","));
      }   
      if (types.length() > 0) {
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      outputSerde.initialize(ctx.getConf(), props);

      Map<ObjectInspector, Boolean> oiSettableProperties = new HashMap<ObjectInspector, Boolean>();
      ObjectInspector inputOI = ObjectInspectorFactory.getStandardStructObjectInspector(
          columnNames, columnOIs);
      convertedOI = ObjectInspectorConverters.getConvertedOI(inputOI,
          outputSerde.getObjectInspector(), oiSettableProperties);
      converter = ObjectInspectorConverters.getConverter(inputOI, convertedOI);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    } catch (SerDeException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void writeRow(ResultRow row) throws IOException {
    try {
      Writable rowWritable = outputSerde.serialize(converter.convert(
          row.getValues()), convertedOI);
      writeRow(rowWritable.toString());
    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  SerDe getSerde() {
    return outputSerde;
  }
}
