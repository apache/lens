/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es.client.jest;

import java.util.List;
import java.util.Map;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.driver.es.client.ESResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import lombok.NonNull;

/**
 * The class ESResultSetTransformer, transforms json responses of elastic search
 * to 2D result sets that lens expects
 */
public abstract class JestResultSetTransformer {

  /**
   * The class AggregateTransformer, takes care of transforming results of
   * aggregate queries
   */
  static class AggregateTransformer extends JestResultSetTransformer {

    private List<ResultRow> rows = Lists.newArrayList();

    public AggregateTransformer(JsonObject result, List<String> schema, List<String> selectedColumns) {
      super(result, schema, selectedColumns);
    }

    /**
     * The json response from jest will be nested in case of group bys
     *  g1-val           (group 1)
     *  |      \
     *  g2-val1  g2-val2 (group 2)
     *  |        |
     *  5, 10    10, 15  (metrics)
     *
     *  The method will traverse the tree and collect all the paths ->
     *  keyCol1 keyCol2  aggr1 aggr2
     *  g1-val  g2-val1  5     10
     *  g1-val  g2-val2  10    15
     *
     * @param object, json response of jest
     * @param currentPath, current row
     * @param length, length of the current row
     * @param keyCol, Col name of the keys that will be parsed in the recursion
     */
    private void collectAllRows(JsonObject object, List<Object> currentPath, int length, String keyCol) {
      for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
        JsonElement element = entry.getValue();
        final String key = entry.getKey();
        if (key.equals(ResultSetConstants.KEY_STRING)) {
          Validate.isTrue(keyCol != null, "Key not available");
          final int index = columnAliases.indexOf(keyCol);
          currentPath.set(
            index,
            getTypedValue(index, entry.getValue())
          );
          length++;
          if (length == columnAliases.size()) {
            rows.add(new ResultRow(Lists.newArrayList(currentPath)));
          }
        } else if (element instanceof JsonObject && ((JsonObject) element).get(ResultSetConstants.VALUE_KEY) != null) {
          final int index = columnAliases.indexOf(key);
          currentPath.set(
            index,
            getTypedValue(index, ((JsonObject) element).get(ResultSetConstants.VALUE_KEY))
          );
          length++;
          if (length == columnAliases.size()) {
            rows.add(new ResultRow(Lists.newArrayList(currentPath)));
          }
        } else if (element instanceof JsonObject) {
          collectAllRows((JsonObject) element, Lists.newArrayList(currentPath), length, key);
        } else if (element instanceof JsonArray && key.equals(ResultSetConstants.BUCKETS_KEY)) {
          JsonArray array = (JsonArray) element;
          for (JsonElement arrayElement : array) {
            collectAllRows((JsonObject) arrayElement, Lists.newArrayList(currentPath), length, keyCol);
          }
        }
      }
    }

    @Override
    public ESResultSet transform() {
      collectAllRows(
        result.getAsJsonObject(ResultSetConstants.AGGREGATIONS_KEY)
          .getAsJsonObject(ResultSetConstants.FILTER_WRAPPER_KEY),
        getEmptyRow(),
        0,
        null
      );
      return new ESResultSet(
        rows.size(),
        rows,
        getMetaData(columnAliases)
      );
    }
  }

  /**
   * The class TermTransformer, takes care of transforming results of simple select queries
   */
  static class TermTransformer extends JestResultSetTransformer {

    public TermTransformer(JsonObject result, List<String> schema, List<String> selectedColumns) {
      super(result, schema, selectedColumns);
    }

    @Override
    public ESResultSet transform() {

      JsonArray jsonArray = result
        .getAsJsonObject(ResultSetConstants.HITS_KEY)
        .getAsJsonArray(ResultSetConstants.HITS_KEY);

      final List<ResultRow> rows = Lists.newArrayList();
      for (JsonElement element : jsonArray) {
        final List<Object> objects = getEmptyRow();
        for (Map.Entry<String, JsonElement> entry
          : element
            .getAsJsonObject()
            .getAsJsonObject(ResultSetConstants.FIELDS_KEY)
            .entrySet()) {
          int index = columnNames.indexOf(entry.getKey());
          objects.set(
            index
            , getTypedValue(index, entry.getValue().getAsJsonArray().get(0))
          );
        }
        rows.add(new ResultRow(objects));
      }
      return new ESResultSet(rows.size(), rows, getMetaData(columnAliases));
    }

  }

  @NonNull
  protected final JsonObject result;
  @NonNull
  protected final List<String> columnAliases;
  @NonNull
  protected final List<String> columnNames;
  protected final List<Type> columnDataTypes = Lists.newArrayList();

  public JestResultSetTransformer(JsonObject result, List<String> columnAliases, List<String> columnNames) {
    this.columnAliases = columnAliases;
    this.result = result;
    this.columnNames = columnNames;
    for(int i=0; i< columnAliases.size(); i++) {
      columnDataTypes.add(Type.NULL_TYPE);
    }
  }

  public static ESResultSet transformFrom(JsonObject jsonResult, List<String> schema, List<String> selectedColumns) {
    if (jsonResult.getAsJsonObject(ResultSetConstants.AGGREGATIONS_KEY) != null) {
      return new AggregateTransformer(jsonResult, schema, selectedColumns).transform();
    } else {
      return new TermTransformer(jsonResult, schema, selectedColumns).transform();
    }
  }

  protected List<Object> getEmptyRow() {
    List<Object> objects = Lists.newArrayList();
    int i = 0;
    while (i++ < columnAliases.size()) {
      objects.add(null);
    }
    return objects;
  }

  protected Object getTypedValue(int colPosition, JsonElement jsonObjectValue) {
    final Type type = getDataType(colPosition, jsonObjectValue);
    switch (type) {
    case NULL_TYPE:
      return null;
    case DOUBLE_TYPE:
      return jsonObjectValue.getAsDouble();
    case BOOLEAN_TYPE:
      return jsonObjectValue.getAsBoolean();
    default:
      return jsonObjectValue.getAsString();
    }
  }

  private Type getDataType(int colPosition, JsonElement jsonObjectValue) {
    if (columnDataTypes.get(colPosition) != Type.NULL_TYPE) {
      return columnDataTypes.get(colPosition);
    }

    final JsonPrimitive jsonPrimitive = jsonObjectValue.getAsJsonPrimitive();
    if (jsonPrimitive.isJsonNull()) {
      return Type.NULL_TYPE;
    }

    final Type type;
    if (jsonPrimitive.isBoolean()) {
      type = Type.BOOLEAN_TYPE;
    } else if (jsonPrimitive.isNumber()) {
      type = Type.DOUBLE_TYPE;
    } else {
      type = Type.STRING_TYPE;
    }
    columnDataTypes.set(colPosition, type);
    return type;
  }

  public abstract ESResultSet transform();

  protected LensResultSetMetadata getMetaData(final List<String> schema) {
    return new LensResultSetMetadata() {
      @Override
      public List<ColumnDescriptor> getColumns() {
        List<ColumnDescriptor> descriptors = Lists.newArrayList();
        int i = 0;
        for (final String col : schema) {
          descriptors.add(
            new ColumnDescriptor(col, col, new TypeDescriptor(columnDataTypes.get(i)), i)
          );
          i++;
        }
        return descriptors;
      }
    };
  }

  protected static class ResultSetConstants {
    public static final String AGGREGATIONS_KEY = "aggregations";
    public static final String KEY_STRING = "key";
    public static final String VALUE_KEY = "value";
    public static final String BUCKETS_KEY = "buckets";
    public static final String FILTER_WRAPPER_KEY = "filter_wrapper";
    public static final String HITS_KEY = "hits";
    public static final String FIELDS_KEY = "fields";
  }
}
