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
package org.apache.lens.driver.es;

import java.io.IOException;

import org.apache.lens.cube.parse.BetweenTimeRangeWriter;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.driver.es.translator.ESVisitor;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableList;

public class QueryTranslationTest extends ESDriverTest {

  private static class ValidQuery {
    private final String name;
    private final String hql;
    private final JsonNode expectedJson;

    @JsonCreator
    ValidQuery(@JsonProperty("name") String name,
               @JsonProperty("hql") String hql,
               @JsonProperty("expectedJson") JsonNode expectedJson) {
      this.name = name;
      this.hql = hql;
      this.expectedJson = expectedJson;
    }
  }

  private static class InvalidQuery {
    private final String name;
    private final String hql;

    @JsonCreator
    InvalidQuery(@JsonProperty("name") String name,
                 @JsonProperty("hql") String hql) {
      this.name = name;
      this.hql = hql;
    }
  }

  private static <T> T loadResource(String resourcePath, Class<T> type) {
    try {
      return OBJECT_MAPPER.readValue(
        QueryTranslationTest.class.getClassLoader()
          .getResourceAsStream(resourcePath),
        type
      );
    } catch (IOException e) {
      throw new RuntimeException("FATAL! Cannot initialize test resource : " + resourcePath);
    }
  }

  public static final String VALID_QUERIES_RESOURCE_PATH = "valid-queries.data";
  public static final String INVALID_QUERIES_RESOURCE_PATH = "invalid-queries.data";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ImmutableList<ValidQuery> VALID_QUERIES;
  private static final ImmutableList<InvalidQuery> IN_VALID_QUERIES;
  static {
    OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true); // Jackson 1.2+
    VALID_QUERIES = ImmutableList.copyOf(loadResource(VALID_QUERIES_RESOURCE_PATH, ValidQuery[].class));
    IN_VALID_QUERIES = ImmutableList.copyOf(loadResource(INVALID_QUERIES_RESOURCE_PATH, InvalidQuery[].class));
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
    config.setStrings(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, BetweenTimeRangeWriter.class.getCanonicalName());
    config.setStrings(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ss");
  }

  @Test
  public void testQueryTranslation() throws LensException {
    for(final ValidQuery query : VALID_QUERIES) {
      Assert.assertEquals(
        ESVisitor.rewrite(esDriverConfig, query.hql).getQuery(),
        query.expectedJson.toString(),
        "Test case '" + query.name + "' failed."
      );
    }
  }

  @Test
  public void testInvalidQueries() {
    for(final InvalidQuery invalidQuery : IN_VALID_QUERIES) {
      try {
        ESVisitor.rewrite(esDriverConfig, invalidQuery.hql);
        Assert.fail("The invalid query"+ invalidQuery.name +"did not suffer any exception");
      } catch (Throwable e) {
        continue;
      }
    }
  }

}
