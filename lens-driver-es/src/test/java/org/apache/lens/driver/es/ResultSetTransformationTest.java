/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.driver.es.client.ESResultSet;
import org.apache.lens.driver.es.client.jest.JestResultSetTransformer;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import junit.framework.Assert;

public class ResultSetTransformationTest extends ESDriverTest {

  private static final ImmutableMap<Result, ESResultSet> VALID_TRANSFORMATIONS;
  private static final ImmutableMap<Result, ESResultSet> IN_VALID_TRANSFORMATIONS;
  private static final JsonParser JSON_PARSER = new JsonParser();

  static {
    ImmutableMap.Builder<Result, ESResultSet> reponsesBuilder = ImmutableMap.builder();

    /**
     * Sample term query result transformation
     */
    reponsesBuilder.put(
      new Result(
        Lists.newArrayList("col1_alias", "col2"),
        Lists.newArrayList("col1", "col2"),
        (JsonObject) JSON_PARSER.parse("{\n"
          + "  \"took\": 653,\n"
          + "  \"timed_out\": false,\n"
          + "  \"_shards\": {\n"
          + "    \"total\": 5,\n"
          + "    \"successful\": 5,\n"
          + "    \"failed\": 0\n"
          + "  },\n"
          + "  \"hits\": {\n"
          + "    \"total\": 100,\n"
          + "    \"max_score\": 1,\n"
          + "    \"hits\": [\n"
          + "      {\n"
          + "        \"_index\": \"index\",\n"
          + "        \"_type\": \"type\",\n"
          + "        \"_id\": \"12345\",\n"
          + "        \"_score\": 1,\n"
          + "        \"fields\": {\n"
          + "          \"col1\": [\n"
          + "            \"val1\"\n"
          + "          ],\n"
          + "          \"col2\": [\n"
          + "            \"val2\"\n"
          + "          ]\n"
          + "        }\n"
          + "      },\n"
          + "      {\n"
          + "        \"_index\": \"index\",\n"
          + "        \"_type\": \"type\",\n"
          + "        \"_id\": \"123456\",\n"
          + "        \"_score\": 1,\n"
          + "        \"fields\": {\n"
          + "          \"col1\": [\n"
          + "            \"val3\"\n"
          + "          ],\n"
          + "          \"col2\": [\n"
          + "            \"val4\"\n"
          + "          ]\n"
          + "        }\n"
          + "      }\n"
          + "    ]\n"
          + "  }\n"
          + "}"),
        ESQuery.QueryType.AGGR
      ),
      new ESResultSet(
        2,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("val1", "val2")),
          new ResultRow(Lists.<Object>newArrayList("val3", "val4"))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1_alias", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 1)
            );
          }
        })
    );

    /**
     * Sample aggregate query transformation
     */
    reponsesBuilder.put(
      new Result(
        Lists.newArrayList("col1", "col2", "aggr_col"),
        Lists.newArrayList("col1", "col2", "aggr_col"),
        (JsonObject) JSON_PARSER.parse("{\n"
          + "  \"took\": 3356,\n"
          + "  \"timed_out\": false,\n"
          + "  \"_shards\": {\n"
          + "    \"total\": 5,\n"
          + "    \"successful\": 5,\n"
          + "    \"failed\": 0\n"
          + "  },\n"
          + "  \"hits\": {\n"
          + "    \"total\": 100,\n"
          + "    \"max_score\": 0,\n"
          + "    \"hits\": []\n"
          + "  },\n"
          + "  \"aggregations\": {\n"
          + "    \"filter_wrapper\": {\n"
          + "      \"doc_count\": 100,\n"
          + "      \"col1\": {\n"
          + "        \"doc_count_error_upper_bound\": 333,\n"
          + "        \"sum_other_doc_count\": 18017550,\n"
          + "        \"buckets\": [\n"
          + "          {\n"
          + "            \"key\": \"g1v1\",\n"
          + "            \"doc_count\": 14099915,\n"
          + "            \"col2\": {\n"
          + "              \"doc_count_error_upper_bound\": 0,\n"
          + "              \"sum_other_doc_count\": 0,\n"
          + "              \"buckets\": [\n"
          + "                {\n"
          + "                  \"key\": \"g2v1\",\n"
          + "                  \"doc_count\": 10432335,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 1.0\n"
          + "                  }\n"
          + "                },\n"
          + "                {\n"
          + "                  \"key\": \"g2v2\",\n"
          + "                  \"doc_count\": 2,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 2.0\n"
          + "                  }\n"
          + "                }\n"
          + "              ]\n"
          + "            }\n"
          + "          },\n"
          + "          {\n"
          + "            \"key\": \"g1v2\",\n"
          + "            \"doc_count\": 8608107,\n"
          + "            \"col2\": {\n"
          + "              \"doc_count_error_upper_bound\": 0,\n"
          + "              \"sum_other_doc_count\": 0,\n"
          + "              \"buckets\": [\n"
          + "                {\n"
          + "                  \"key\": \"g2v3\",\n"
          + "                  \"doc_count\": 3,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 3.0\n"
          + "                  }\n"
          + "                }\n"
          + "              ]\n"
          + "            }\n"
          + "          }\n"
          + "        ]\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "}"),
        ESQuery.QueryType.AGGR
      ),
      new ESResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v1", 1.0)),
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v2", 2.0)),
          new ResultRow(Lists.<Object>newArrayList("g1v2", "g2v3", 3.0))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 1),
              new ColumnDescriptor("aggr_col", "", new TypeDescriptor(Type.DOUBLE_TYPE), 2)
            );
          }
        })
    );

    reponsesBuilder.put(
      new Result(
        Lists.newArrayList("col1", "aggr_col", "col2"),
        Lists.newArrayList("col1", "aggr_col", "col2"),
        (JsonObject) JSON_PARSER.parse("{\n"
          + "  \"took\": 3356,\n"
          + "  \"timed_out\": false,\n"
          + "  \"_shards\": {\n"
          + "    \"total\": 5,\n"
          + "    \"successful\": 5,\n"
          + "    \"failed\": 0\n"
          + "  },\n"
          + "  \"hits\": {\n"
          + "    \"total\": 100,\n"
          + "    \"max_score\": 0,\n"
          + "    \"hits\": []\n"
          + "  },\n"
          + "  \"aggregations\": {\n"
          + "    \"filter_wrapper\": {\n"
          + "      \"doc_count\": 100,\n"
          + "      \"col1\": {\n"
          + "        \"doc_count_error_upper_bound\": 333,\n"
          + "        \"sum_other_doc_count\": 18017550,\n"
          + "        \"buckets\": [\n"
          + "          {\n"
          + "            \"key\": \"g1v1\",\n"
          + "            \"doc_count\": 14099915,\n"
          + "            \"col2\": {\n"
          + "              \"doc_count_error_upper_bound\": 0,\n"
          + "              \"sum_other_doc_count\": 0,\n"
          + "              \"buckets\": [\n"
          + "                {\n"
          + "                  \"key\": \"g2v1\",\n"
          + "                  \"doc_count\": 10432335,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 1.0\n"
          + "                  }\n"
          + "                },\n"
          + "                {\n"
          + "                  \"key\": \"g2v2\",\n"
          + "                  \"doc_count\": 2,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 2.0\n"
          + "                  }\n"
          + "                }\n"
          + "              ]\n"
          + "            }\n"
          + "          },\n"
          + "          {\n"
          + "            \"key\": \"g1v2\",\n"
          + "            \"doc_count\": 8608107,\n"
          + "            \"col2\": {\n"
          + "              \"doc_count_error_upper_bound\": 0,\n"
          + "              \"sum_other_doc_count\": 0,\n"
          + "              \"buckets\": [\n"
          + "                {\n"
          + "                  \"key\": \"g2v3\",\n"
          + "                  \"doc_count\": 3,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 3.0\n"
          + "                  }\n"
          + "                }\n"
          + "              ]\n"
          + "            }\n"
          + "          }\n"
          + "        ]\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "}"),
        ESQuery.QueryType.AGGR
      ),
      new ESResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", 1.0, "g2v1")),
          new ResultRow(Lists.<Object>newArrayList("g1v1", 2.0, "g2v2")),
          new ResultRow(Lists.<Object>newArrayList("g1v2", 3.0, "g2v3"))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("aggr_col", "", new TypeDescriptor(Type.DOUBLE_TYPE), 1),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 2)
            );
          }
        })
    );


    VALID_TRANSFORMATIONS = reponsesBuilder.build();

    ImmutableMap.Builder<Result, ESResultSet> invalidResponsesBuilder = ImmutableMap.builder();
    /**
     * invalid aliases
     */
    invalidResponsesBuilder.put(
      new Result(
        Lists.newArrayList("col1", "col2"),
        Lists.newArrayList("col1", "col2"),
        (JsonObject) JSON_PARSER.parse("{\n"
          + "  \"took\": 653,\n"
          + "  \"timed_out\": false,\n"
          + "  \"_shards\": {\n"
          + "    \"total\": 5,\n"
          + "    \"successful\": 5,\n"
          + "    \"failed\": 0\n"
          + "  },\n"
          + "  \"hits\": {\n"
          + "    \"total\": 100,\n"
          + "    \"max_score\": 1,\n"
          + "    \"hits\": [\n"
          + "      {\n"
          + "        \"_index\": \"index\",\n"
          + "        \"_type\": \"type\",\n"
          + "        \"_id\": \"12345\",\n"
          + "        \"_score\": 1,\n"
          + "        \"fields\": {\n"
          + "          \"col1\": [\n"
          + "            \"val1\"\n"
          + "          ],\n"
          + "          \"col2\": [\n"
          + "            \"val2\"\n"
          + "          ]\n"
          + "        }\n"
          + "      },\n"
          + "      {\n"
          + "        \"_index\": \"index\",\n"
          + "        \"_type\": \"type\",\n"
          + "        \"_id\": \"123456\",\n"
          + "        \"_score\": 1,\n"
          + "        \"fields\": {\n"
          + "          \"col1\": [\n"
          + "            \"val3\"\n"
          + "          ],\n"
          + "          \"col2\": [\n"
          + "            \"val4\"\n"
          + "          ]\n"
          + "        }\n"
          + "      }\n"
          + "    ]\n"
          + "  }\n"
          + "}"),
        ESQuery.QueryType.AGGR
      ),
      new ESResultSet(
        2,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("val1", "val2")),
          new ResultRow(Lists.<Object>newArrayList("val3", "val4"))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1_alias", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 1)
            );
          }
        })
    );

    /**
     * Invalid aggregate transformations, missing aliases
     */
    invalidResponsesBuilder.put(
      new Result(
        Lists.newArrayList("col1", "aggr_col"),
        Lists.newArrayList("col1", "aggr_col"),
        (JsonObject) JSON_PARSER.parse("{\n"
          + "  \"took\": 3356,\n"
          + "  \"timed_out\": false,\n"
          + "  \"_shards\": {\n"
          + "    \"total\": 5,\n"
          + "    \"successful\": 5,\n"
          + "    \"failed\": 0\n"
          + "  },\n"
          + "  \"hits\": {\n"
          + "    \"total\": 100,\n"
          + "    \"max_score\": 0,\n"
          + "    \"hits\": []\n"
          + "  },\n"
          + "  \"aggregations\": {\n"
          + "    \"filter_wrapper\": {\n"
          + "      \"doc_count\": 100,\n"
          + "      \"col1\": {\n"
          + "        \"doc_count_error_upper_bound\": 333,\n"
          + "        \"sum_other_doc_count\": 18017550,\n"
          + "        \"buckets\": [\n"
          + "          {\n"
          + "            \"key\": \"g1v1\",\n"
          + "            \"doc_count\": 14099915,\n"
          + "            \"col2\": {\n"
          + "              \"doc_count_error_upper_bound\": 0,\n"
          + "              \"sum_other_doc_count\": 0,\n"
          + "              \"buckets\": [\n"
          + "                {\n"
          + "                  \"key\": \"g2v1\",\n"
          + "                  \"doc_count\": 10432335,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 1.0\n"
          + "                  }\n"
          + "                },\n"
          + "                {\n"
          + "                  \"key\": \"g2v2\",\n"
          + "                  \"doc_count\": 2,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 2.0\n"
          + "                  }\n"
          + "                }\n"
          + "              ]\n"
          + "            }\n"
          + "          },\n"
          + "          {\n"
          + "            \"key\": \"g1v2\",\n"
          + "            \"doc_count\": 8608107,\n"
          + "            \"col2\": {\n"
          + "              \"doc_count_error_upper_bound\": 0,\n"
          + "              \"sum_other_doc_count\": 0,\n"
          + "              \"buckets\": [\n"
          + "                {\n"
          + "                  \"key\": \"g2v3\",\n"
          + "                  \"doc_count\": 3,\n"
          + "                  \"aggr_col\": {\n"
          + "                    \"value\": 3.0\n"
          + "                  }\n"
          + "                }\n"
          + "              ]\n"
          + "            }\n"
          + "          }\n"
          + "        ]\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "}"),
        ESQuery.QueryType.AGGR
      ),
      new ESResultSet(
        3,
        Lists.newArrayList(
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v1", 1.0)),
          new ResultRow(Lists.<Object>newArrayList("g1v1", "g2v2", 2.0)),
          new ResultRow(Lists.<Object>newArrayList("g1v2", "g2v3", 3.0))
        ),
        new LensResultSetMetadata() {
          @Override
          public List<ColumnDescriptor> getColumns() {
            return Lists.newArrayList(
              new ColumnDescriptor("col1", "", new TypeDescriptor(Type.STRING_TYPE), 0),
              new ColumnDescriptor("col2", "", new TypeDescriptor(Type.STRING_TYPE), 1),
              new ColumnDescriptor("aggr_col", "", new TypeDescriptor(Type.DOUBLE_TYPE), 2)
            );
          }
        })
    );
    IN_VALID_TRANSFORMATIONS = invalidResponsesBuilder.build();

  }

  @BeforeTest
  @Override
  public void beforeTest() throws LensException {
    super.beforeTest();
  }

  @Override
  protected void initializeConfig(Configuration config) {
    config.setInt(ESDriverConfig.TERM_FETCH_SIZE_KEY, 10000);
    config.setInt(ESDriverConfig.QUERY_TIME_OUT_LENS_KEY, 10000);
    config.setInt(ESDriverConfig.MAX_ROW_SIZE_KEY, -1);
    config.setInt(ESDriverConfig.AGGR_BUCKET_SIZE_LENS_KEY, 100);
    config.setStrings(ESDriverConfig.CLIENT_CLASS_KEY, MockClientES.class.getCanonicalName());
    config.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    config.setStrings(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "es_storage");
  }

  private void assertResultsAreEqual(ESResultSet resultSet1, ESResultSet resultSet2) {
    final Collection<ColumnDescriptor> columns1 = resultSet1.getMetadata().getColumns();
    final Collection<ColumnDescriptor> columns2 = resultSet2.getMetadata().getColumns();
    Assert.assertEquals(columns1.size(), columns2.size());
    final Iterator<ColumnDescriptor> iterator1 = columns1.iterator();
    final Iterator<ColumnDescriptor> iterator2 = columns2.iterator();
    while (iterator1.hasNext()) {
      final ColumnDescriptor column1 = iterator1.next();
      final ColumnDescriptor column2 = iterator2.next();
      Assert.assertEquals("Column aliases are different! " + column1.getName() + " " + column1.getName(),
        column1.getName(), column2.getName());
      Assert.assertEquals("Column positions are different! " + column1.getName() + " " + column1.getName(),
        column1.getOrdinalPosition(), column2.getOrdinalPosition());
      Assert.assertEquals("Column types are different! " + column1.getName() + " " + column1.getName(),
        column1.getType(), column2.getType());
    }

    Assert.assertEquals(resultSet1.size(), resultSet2.size());
    while (resultSet1.hasNext()) {
      final ResultRow row1 = resultSet1.next();
      final ResultRow row2 = resultSet2.next();
      Assert.assertEquals("Row length is different", row1.getValues().size(), row2.getValues().size());
      Iterator<Object> values1 = row1.getValues().iterator();
      Iterator<Object> values2 = row2.getValues().iterator();
      while (values1.hasNext()) {
        Assert.assertEquals("Values are different", values1.next(), values2.next());
      }
    }

  }

  @Test
  public void testTransformations() {
    for (Map.Entry<Result, ESResultSet> entry : VALID_TRANSFORMATIONS.entrySet()) {
      final Result rawResult = entry.getKey();
      ESResultSet resultSet =
        JestResultSetTransformer.transformFrom(rawResult.object, rawResult.schema, rawResult.cols);
      assertResultsAreEqual(resultSet, entry.getValue());
    }
  }

  @Test
  public void testInvalidTranformations() {
    for (Map.Entry<Result, ESResultSet> entry : IN_VALID_TRANSFORMATIONS.entrySet()) {
      boolean failed = false;
      try {
        final Result rawResult = entry.getKey();
        ESResultSet resultSet =
          JestResultSetTransformer.transformFrom(rawResult.object, rawResult.schema, rawResult.cols);
        assertResultsAreEqual(resultSet, entry.getValue());
        failed = true;
        throw new RuntimeException("Result sets are equal - ");
      } catch (Throwable e) {
        if (failed) {
          Assert.fail("Results sets are equal Expected - not equal" + e.getMessage());
        }
      }
    }
  }

  static class Result {
    final List<String> schema;
    final List<String> cols;
    final JsonObject object;
    final ESQuery.QueryType queryType;

    Result(List<String> schema, List<String> cols, JsonObject object, ESQuery.QueryType queryType) {
      this.schema = schema;
      this.cols = cols;
      this.object = object;
      this.queryType = queryType;
    }
  }


}
