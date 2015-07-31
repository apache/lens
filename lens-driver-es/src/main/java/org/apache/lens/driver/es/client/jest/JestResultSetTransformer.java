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
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.TypeDescriptor;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
          currentPath.set(schema.indexOf(keyCol), entry.getValue().getAsString());
          length++;
          if (length == schema.size()) {
            rows.add(new ResultRow(Lists.newArrayList(currentPath)));
          }
        } else if (element instanceof JsonObject && ((JsonObject) element).get(ResultSetConstants.VALUE_KEY) != null) {
          currentPath.set(schema.indexOf(key),
            ((JsonObject) element).get(ResultSetConstants.VALUE_KEY).getAsString());
          length++;
          if (length == schema.size()) {
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
        getMetaData(schema)
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
          objects.set(
            selectedColumns.indexOf(entry.getKey())
            , entry.getValue().getAsString()
          );
        }
        rows.add(new ResultRow(objects));
      }
      return new ESResultSet(rows.size(), rows, getMetaData(schema));
    }


  }

  @NonNull
  protected final JsonObject result;
  @NonNull
  protected final List<String> schema;
  @NonNull
  protected final List<String> selectedColumns;

  public JestResultSetTransformer(JsonObject result, List<String> schema, List<String> selectedColumns) {
    this.schema = schema;
    this.result = result;
    this.selectedColumns = selectedColumns;
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
    while (i++ < schema.size()) {
      objects.add(null);
    }
    return objects;
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
            new ColumnDescriptor(col, col, new TypeDescriptor(Type.STRING_TYPE), i++)
          );
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
