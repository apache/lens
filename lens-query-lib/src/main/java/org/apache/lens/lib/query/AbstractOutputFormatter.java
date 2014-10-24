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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.QueryOutputFormatter;

import org.apache.commons.lang.StringEscapeUtils;

/**
 * Provides abstract implementation of the query output formatter.
 *
 * In this it initializes column names, types column object inspectors Also provides methods to construct header from
 * serde
 *
 */
@SuppressWarnings("deprecation")
public abstract class AbstractOutputFormatter implements QueryOutputFormatter {

  /** The Constant HEADER_TYPE. */
  public static final String HEADER_TYPE = "string";

  /** The ctx. */
  protected QueryContext ctx;

  /** The metadata. */
  protected LensResultSetMetadata metadata;

  /** The column names. */
  protected List<String> columnNames = new ArrayList<String>();

  /** The escaped column names. */
  protected List<String> escapedColumnNames = new ArrayList<String>();

  /** The column types. */
  protected List<TypeInfo> columnTypes = new ArrayList<TypeInfo>();

  /** The column o is. */
  protected List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();

  /** The column header o is. */
  protected List<ObjectInspector> columnHeaderOIs = new ArrayList<ObjectInspector>();

  /** The htypes. */
  protected String htypes;

  /** The types. */
  protected String types;

  /** The header oi. */
  protected ObjectInspector headerOI;

  /** The header serde. */
  protected SerDe headerSerde;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryOutputFormatter#init(org.apache.lens.server.api.query.QueryContext,
   * org.apache.lens.server.api.driver.LensResultSetMetadata)
   */
  @Override
  public void init(QueryContext ctx, LensResultSetMetadata metadata) throws IOException {
    this.ctx = ctx;
    this.metadata = metadata;
    initColumnFields(metadata);
  }

  @Override
  public LensResultSetMetadata getMetadata() {
    return metadata;
  }

  /**
   * Inits the column fields.
   *
   * @param metadata
   *          the metadata
   */
  private void initColumnFields(LensResultSetMetadata metadata) {
    StringBuilder typesSb = new StringBuilder();
    StringBuilder headerTypes = new StringBuilder();

    if ((metadata != null) && (!metadata.getColumns().isEmpty())) {
      for (int pos = 0; pos < metadata.getColumns().size(); pos++) {
        if (pos != 0) {
          typesSb.append(",");
          headerTypes.append(",");
        }
        String name = metadata.getColumns().get(pos).getName();
        String type = LensResultSetMetadata.getQualifiedTypeName(metadata.getColumns().get(pos).getTypeDescriptor());
        typesSb.append(type);
        columnNames.add(name);
        escapedColumnNames.add(StringEscapeUtils.escapeCsv(name));
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
        columnTypes.add(typeInfo);
        columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo));
        columnHeaderOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(TypeInfoUtils
            .getTypeInfoFromTypeString(HEADER_TYPE)));
        headerTypes.append(HEADER_TYPE);
      }
    }

    types = typesSb.toString();
    htypes = headerTypes.toString();
  }

  /**
   * Inits the header serde.
   *
   * @throws ClassNotFoundException
   *           the class not found exception
   * @throws SerDeException
   *           the ser de exception
   */
  @SuppressWarnings("unchecked")
  private void initHeaderSerde() throws ClassNotFoundException, SerDeException {
    if (headerSerde == null) {
      headerSerde = ReflectionUtils.newInstance(
          ctx.getConf().getClass(LensConfConstants.QUERY_OUTPUT_SERDE,
              (Class<? extends AbstractSerDe>) Class.forName(LensConfConstants.DEFAULT_OUTPUT_SERDE), SerDe.class),
          ctx.getConf());

      Properties hprops = new Properties();
      if (columnNames.size() > 0) {
        hprops.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(escapedColumnNames, ","));
      }
      if (htypes.length() > 0) {
        hprops.setProperty(serdeConstants.LIST_COLUMN_TYPES, htypes);
      }
      headerSerde.initialize(ctx.getConf(), hprops);

      headerOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnHeaderOIs);
    }
  }

  protected String getHeaderFromSerde() throws IOException {
    Writable rowWritable;
    try {
      initHeaderSerde();
      rowWritable = headerSerde.serialize(columnNames, headerOI);
    } catch (SerDeException e) {
      throw new IOException(e);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    return rowWritable.toString();
  }
}
