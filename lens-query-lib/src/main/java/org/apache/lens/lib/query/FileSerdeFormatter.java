/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.lib.query;

import java.io.IOException;
import java.util.Properties;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.query.InMemoryOutputFormatter;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * File format which provides implementation for {@link InMemoryOutputFormatter}
 * <p></p>
 * This is a wrapped formatter, which serializes the rows of the result with configured serde. It would only accept the
 * Serde's whose serialization class is Text
 */
@SuppressWarnings("deprecation")
public class FileSerdeFormatter extends WrappedFileFormatter implements InMemoryOutputFormatter {

  /**
   * The output serde.
   */
  private SerDe outputSerde;

  /**
   * The input oi.
   */
  private ObjectInspector inputOI;

  /**
   * Instantiates a new file serde formatter.
   */
  public FileSerdeFormatter() {
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.lib.query.WrappedFileFormatter#init(org.apache.lens.server.api.query.QueryContext,
   * org.apache.lens.server.api.driver.LensResultSetMetadata)
   */
  public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException {
    super.init(ctx, metadata);
    initOutputSerde();
  }

  /**
   * Inits the output serde.
   */
  @SuppressWarnings("unchecked")
  private void initOutputSerde() {
    try {
      outputSerde = ReflectionUtils.newInstance(
        ctx.getConf().getClass(LensConfConstants.QUERY_OUTPUT_SERDE,
          (Class<? extends AbstractSerDe>) Class.forName(LensConfConstants.DEFAULT_OUTPUT_SERDE), SerDe.class),
        ctx.getConf());

      Properties props = new Properties();
      if (columnNames.size() > 0) {
        props.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(escapedColumnNames, ","));
      }
      if (types.length() > 0) {
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      outputSerde.initialize(ctx.getConf(), props);
      inputOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    } catch (SerDeException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.query.InMemoryOutputFormatter#writeRow(org.apache.lens.api.query.ResultRow)
   */
  @Override
  public void writeRow(ResultRow row) throws IOException {
    try {
      Writable rowWritable = outputSerde.serialize(row.getValues(), inputOI);
      writeRow(rowWritable.toString());
    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  SerDe getSerde() {
    return outputSerde;
  }
}
