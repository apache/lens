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

package org.apache.lens.cube.metadata;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMetastoreUtil {

  @Test
  public void testNamedStrings() {
    Set<CubeDimAttribute> attrs = new LinkedHashSet<CubeDimAttribute>();
    attrs.add(new BaseDimAttribute(new FieldSchema("first", "string", "")));
    attrs.add(new BaseDimAttribute(new FieldSchema("second", "string", "")));
    attrs.add(new BaseDimAttribute(new FieldSchema("biggggger", "string", "")));
    
    List<String> names = MetastoreUtil.getNamedStrs(attrs, 10);
    Assert.assertEquals(names.size(), 3);
    Assert.assertEquals(names.get(0), "first,");
    Assert.assertEquals(names.get(1), "second,");
    Assert.assertEquals(names.get(2), "biggggger");
    
    Map<String, String> props = new HashMap<String, String>();
    MetastoreUtil.addNameStrings(props, "test.key", attrs);
    String propValue = MetastoreUtil.getNamedStringValue(props, "test.key");
    Assert.assertEquals(props.size(), 2);
    Assert.assertEquals(props.get("test.key.size"), "1");
    Assert.assertEquals(propValue, "first,second,biggggger");

    props = new HashMap<String, String>();
    MetastoreUtil.addNameStrings(props, "test.key", attrs, 10);
    propValue = MetastoreUtil.getNamedStringValue(props, "test.key");
    Assert.assertEquals(props.size(), 4);
    Assert.assertEquals(props.get("test.key.size"), "3");
    Assert.assertEquals(propValue, "first,second,biggggger");
  }
}
