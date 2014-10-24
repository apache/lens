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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.lens.lib.query.CSVSerde;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestCSVSerde.
 */
@SuppressWarnings("unchecked")
public class TestCSVSerde {

  /** The csv. */
  private final CSVSerde csv = new CSVSerde();

  /** The props. */
  final Properties props = new Properties();

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
   * Test deserialize.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testDeserialize() throws Exception {
    csv.initialize(null, props);
    Text in = new Text("hello,\"yes, okay\",1,char");

    List<Object> row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes, okay", row.get(1).toString());
    Assert.assertEquals(1, row.get(2));
    Assert.assertEquals("char      ", row.get(3).toString());

    in = new Text("hello,\"NULL\",1,char");

    row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals(null, row.get(1));
    Assert.assertEquals(1, row.get(2));
    Assert.assertEquals("char      ", row.get(3).toString());

    in = new Text("hello,\"yes, okay\",NULL,NULL");

    row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes, okay", row.get(1).toString());
    Assert.assertEquals(null, row.get(2));
    Assert.assertEquals(null, row.get(3));

    props.put(serdeConstants.LIST_COLUMNS, "a,b,c,d,e,f,g");
    props.put(serdeConstants.LIST_COLUMN_TYPES,
        "string,varchar(20),int,char(10),array<int>,map<int,string>,struct<a:int,b:map<int,string>>");
    csv.initialize(null, props);

    in = new Text("\"hello\",\"yes, okay\",\"1\","
        + "\"char\",\"1,NULL,3\",\"5=five,NULL=six,7=NULL\",\"8:5=five,NULL=six,7=NULL\"");
    row = (List<Object>) csv.deserialize(in);
    Assert.assertEquals(row.size(), 7);
    Assert.assertEquals(row.get(0), "hello");
    Assert.assertEquals(row.get(1).toString(), "yes, okay");
    Assert.assertEquals(row.get(2), 1);
    Assert.assertEquals(row.get(3).toString(), "char      ");
    Assert.assertTrue(row.get(4) instanceof List);
    List<Object> array = (List<Object>) row.get(4);
    Assert.assertEquals(array.size(), 3);
    Assert.assertEquals(array.get(0), 1);
    Assert.assertEquals(array.get(1), null);
    Assert.assertEquals(array.get(2), 3);
    Assert.assertTrue(row.get(5) instanceof Map);
    Map<Object, Object> map = (Map<Object, Object>) row.get(5);
    Assert.assertEquals(map.size(), 3);
    Assert.assertEquals(map.get(5), "five");
    Assert.assertEquals(map.get(null), "six");
    Assert.assertNull(map.get(7));
    List<Object> struct = (List<Object>) row.get(6);
    Assert.assertEquals(struct.size(), 2);
    Assert.assertEquals(struct.get(0), 8);
    Assert.assertEquals(struct.get(1), map);
  }

  /**
   * Test serialize.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testSerialize() throws Exception {
    props.put("separatorChar", ",");
    props.put("quoteChar", "\"");
    props.put(serdeConstants.LIST_COLUMNS, "a,b,c,d");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "string,varchar(20),int,char(10)");
    csv.initialize(null, props);

    List<? extends Object> row = Arrays.asList("hello", "yes, okay", 1, "char");

    Object ser = csv.serialize(row, csv.getObjectInspector());
    Assert.assertEquals(((Text) ser).toString(), "\"hello\",\"yes, okay\",\"1\",\"char\"");

    row = Arrays.asList("hello", null, 1, "char");

    ser = csv.serialize(row, csv.getObjectInspector());
    Assert.assertEquals(((Text) ser).toString(), "\"hello\",\"NULL\",\"1\",\"char\"");

    props.put(serdeConstants.LIST_COLUMNS, "a,b,c,d,e,f,g");
    props.put(serdeConstants.LIST_COLUMN_TYPES,
        "string,varchar(20),int,char(10),array<int>,map<int,string>,struct<a:int,b:map<int,string>>");
    csv.initialize(null, props);

    Map<Integer, String> map = new LinkedHashMap<Integer, String>();
    map.put(5, "five");
    map.put(null, "six");
    map.put(7, null);
    row = Arrays.asList("hello", "yes, okay", 1, "char", Arrays.asList(1, null, 3), map, Arrays.asList(8, map));

    ser = csv.serialize(row, csv.getObjectInspector());
    Assert.assertEquals(((Text) ser).toString(), "\"hello\",\"yes, okay\",\"1\","
        + "\"char\",\"1,NULL,3\",\"5=five,NULL=six,7=NULL\",\"8:5=five,NULL=six,7=NULL\"");
    props.put(serdeConstants.LIST_COLUMNS, "a,b,c,d");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "string,varchar(20),int,char(10)");
  }

  /**
   * Test deserialize custom separators.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testDeserializeCustomSeparators() throws Exception {
    props.put("separatorChar", "\t");
    props.put("quoteChar", "'");

    csv.initialize(null, props);

    final Text in = new Text("hello\t'yes\tokay'\t1\tchar");
    final List<Object> row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes\tokay", row.get(1).toString());
    Assert.assertEquals(1, row.get(2));
    Assert.assertEquals("char      ", row.get(3).toString());
  }

  /**
   * Test deserialize custom escape.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testDeserializeCustomEscape() throws Exception {
    props.put("quoteChar", "'");
    props.put("escapeChar", "\\");

    csv.initialize(null, props);

    final Text in = new Text("hello,'yes\\'okay',1,char");
    final List<Object> row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes'okay", row.get(1).toString());
    Assert.assertEquals(1, row.get(2));
    Assert.assertEquals("char      ", row.get(3).toString());
  }
}
