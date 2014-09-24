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

import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.query.QueryContext;
import com.inmobi.grill.server.api.query.QueryOutputFormatter;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * Provides abstract implementation of the query output formatter.
 *
 * In this it initializes column names, types column object inspectors
 * Also provides methods to construct header from serde
 * 
 */
@SuppressWarnings("deprecation")
public abstract class AbstractOutputFormatter implements QueryOutputFormatter {

  public static final String HEADER_TYPE = "string";

  protected QueryContext ctx;
  protected GrillResultSetMetadata metadata;
  protected List<String> columnNames = new ArrayList<String>();
  protected List<String> escapedColumnNames = new ArrayList<String>();
  protected List<TypeInfo> columnTypes = new ArrayList<TypeInfo>();
  protected List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
  protected List<ObjectInspector> columnHeaderOIs = new ArrayList<ObjectInspector>();
  protected String htypes;
  protected String types;
  protected ObjectInspector headerOI;
  protected SerDe headerSerde;

  @Override
  public void init(QueryContext ctx, GrillResultSetMetadata metadata) throws IOException {
    this.ctx = ctx;
    this.metadata = metadata;
    initColumnFields(metadata);
  }

  @Override
  public GrillResultSetMetadata getMetadata() {
    return metadata;
  }

  private void initColumnFields(GrillResultSetMetadata metadata) {
    StringBuilder typesSb = new StringBuilder();
    StringBuilder headerTypes = new StringBuilder();

    if ((metadata != null) && (!metadata.getColumns().isEmpty())) {
      for (int pos = 0; pos < metadata.getColumns().size(); pos++) {
        if (pos != 0) {
          typesSb.append(",");
          headerTypes.append(",");
        }
        String name = metadata.getColumns().get(pos).getName();
        String type = GrillResultSetMetadata.getQualifiedTypeName(
            metadata.getColumns().get(pos).getTypeDescriptor());
        typesSb.append(type);
        columnNames.add(name);
        escapedColumnNames.add(StringEscapeUtils.escapeCsv(name));
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
        columnTypes.add(typeInfo);
        columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo));
        columnHeaderOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
            TypeInfoUtils.getTypeInfoFromTypeString(HEADER_TYPE)));
        headerTypes.append(HEADER_TYPE);
      }
    }

    types = typesSb.toString();
    htypes = headerTypes.toString();
  }

  @SuppressWarnings("unchecked")
  private void initHeaderSerde() throws ClassNotFoundException, SerDeException {
    if (headerSerde == null) {
      headerSerde = ReflectionUtils.newInstance(ctx.getConf().getClass(
          GrillConfConstants.QUERY_OUTPUT_SERDE,
          (Class<? extends AbstractSerDe>)Class.forName(
              GrillConfConstants.DEFAULT_OUTPUT_SERDE),
          SerDe.class), ctx.getConf());

      Properties hprops = new Properties();
      if (columnNames.size() > 0) {
        hprops.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(escapedColumnNames, ","));
      }
      if (htypes.length() > 0) {
        hprops.setProperty(serdeConstants.LIST_COLUMN_TYPES, htypes);
      }
      headerSerde.initialize(ctx.getConf(), hprops);

      headerOI = ObjectInspectorFactory.getStandardStructObjectInspector(
          columnNames, columnHeaderOIs);
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
