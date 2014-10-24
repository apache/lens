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

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.lens.lib.query.JSonSerde;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The Class TestJSONSerde.
 */
@SuppressWarnings("unchecked")
public class TestJSONSerde {

  /** The json serde. */
  private final JSonSerde jsonSerde = new JSonSerde();

  /** The props. */
  private final Properties props = new Properties();

  /**
   * Setup.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeTest
  public void setup() throws Exception {
    props.put(serdeConstants.LIST_COLUMNS, "a,b,c,d");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "string,varchar(20),int,char(10)");
  }

  /**
   * Test deseralize.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testDeseralize() throws Exception {

    jsonSerde.initialize(null, props);

    Text in = new Text("{\n" + "  \"a\":\"hello\",\n" + "  \"b\":\"yes\",\n" + "  \"c\":1,\n" + "  \"d\":\"char\"\n"
        + "}");

    List<Object> row = (List<Object>) jsonSerde.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes", row.get(1).toString());
    Assert.assertEquals(1, row.get(2));
    Assert.assertEquals("char", row.get(3).toString());

    in = new Text("{\n" + "  \"a\":\"hello\",\n" + "  \"c\":1,\n" + "  \"d\":\"char\"\n" + "}");

    row = (List<Object>) jsonSerde.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals(null, row.get(1));
    Assert.assertEquals(1, row.get(2));
    Assert.assertEquals("char", row.get(3).toString());

    in = new Text("{\n" + "  \"a\":\"hello\",\n" + "  \"b\":\"yes\"\n" + "}");
    row = (List<Object>) jsonSerde.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes", row.get(1).toString());
    Assert.assertEquals(null, row.get(2));
    Assert.assertEquals(null, row.get(3));

    props.put(serdeConstants.LIST_COLUMNS, "a,b,c");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "array<int>,map<string,string>,struct<a:int,b:int>");

    jsonSerde.initialize(null, props);

    in = new Text("{\n" + "  \"a\": [\n" + "    1,\n" + "    2,\n" + "    3\n" + "  ],\n" + "  \"b\": {\n"
        + "    \"a\": \"b\",\n" + "    \"c\": \"d\",\n" + "    \"e\": \"f\"\n" + "  },\n" + "  \"c\": {\n"
        + "    \"a\":1,\n" + "    \"b\":2\n" + "  }\n" + "}");

    row = (List<Object>) jsonSerde.deserialize(in);
    Object[] objs = (Object[]) row.get(0);
    Map<String, String> map = (Map<String, String>) row.get(1);
    Assert.assertEquals(objs.length, 3);
    Assert.assertEquals(objs[0], 1);
    Assert.assertEquals(objs[1], 2);
    Assert.assertEquals(objs[2], 3);

    Assert.assertEquals(map.size(), 3);
    Assert.assertEquals(map.get("a"), "b");
    Assert.assertEquals(map.get("c"), "d");
    Assert.assertEquals(map.get("e"), "f");
    Map<String, Object> map1 = (Map<String, Object>) row.get(2);
    Assert.assertEquals(map1.size(), 2);
    Assert.assertEquals(map1.get("a"), 1);
    Assert.assertEquals(map1.get("b"), 2);
  }

}
