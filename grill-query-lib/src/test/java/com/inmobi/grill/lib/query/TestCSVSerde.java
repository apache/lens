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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestCSVSerde {

  private final CSVSerde csv = new CSVSerde();
  final Properties props = new Properties();
  
  @BeforeTest
  public void setup() throws Exception {
    props.put(serdeConstants.LIST_COLUMNS, "a,b,c");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "string,string,int");
  }
  
  @Test
  public void testDeserialize() throws Exception {
    csv.initialize(null, props);
    Text in = new Text("hello,\"yes, okay\",1");
    
    List<Object> row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes, okay", row.get(1));
    Assert.assertEquals(1, row.get(2));

    in = new Text("hello,\"NULL\",1");
    
    row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals(null, row.get(1));
    Assert.assertEquals(1, row.get(2));

    in = new Text("hello,\"yes, okay\",NULL");
    
    row = (List<Object>) csv.deserialize(in);

    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes, okay", row.get(1));
    Assert.assertEquals(null, row.get(2));

  }

  @Test
  public void testSerialize() throws Exception {
    props.put("separatorChar", ",");
    props.put("quoteChar", "\"");
    csv.initialize(null, props);
    
    List<? extends Object> row = Arrays.asList("hello", "yes, okay", 1);
    
    Object ser = csv.serialize(row, csv.getObjectInspector());
    Assert.assertEquals(((Text)ser).toString(), "\"hello\",\"yes, okay\",\"1\"");

    row = Arrays.asList("hello", null, 1);
    
    ser = csv.serialize(row, csv.getObjectInspector());
    Assert.assertEquals(((Text)ser).toString(), "\"hello\",\"NULL\",\"1\"");
  }

  
  @Test
  public void testDeserializeCustomSeparators() throws Exception {
    props.put("separatorChar", "\t");
    props.put("quoteChar", "'");
    
    csv.initialize(null, props);
    
    final Text in = new Text("hello\t'yes\tokay'\t1");
    final List<String> row = (List<String>) csv.deserialize(in);
        
    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes\tokay", row.get(1));
    Assert.assertEquals(1, row.get(2));
  }
  
  @Test
  public void testDeserializeCustomEscape() throws Exception {
    props.put("quoteChar", "'");
    props.put("escapeChar", "\\");
    
    csv.initialize(null, props);
    
    final Text in = new Text("hello,'yes\\'okay',1");
    final List<String> row = (List<String>) csv.deserialize(in);
        
    Assert.assertEquals("hello", row.get(0));
    Assert.assertEquals("yes'okay", row.get(1));
    Assert.assertEquals(1, row.get(2));
  } 
}
