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

package org.apache.lens.regression.core.type;

import java.util.HashMap;

import org.apache.lens.server.api.util.LensUtil;


public class MapBuilder {
  private HashMap<String, String> map;

  public MapBuilder() {
    map = new HashMap<String, String>();
  }

  public MapBuilder(HashMap<String, String> h) {
    map = h;
  }

  public MapBuilder(String key, String value) {
    map = new HashMap<String, String>();
    map.put(key, value);
  }

  public MapBuilder(String[] keys, String[] values) {
    map = new HashMap<String, String>();
    for (int i = 0; i < keys.length; i++) {
      map.put(keys[i], values[i]);
    }
  }

  public MapBuilder(String... args) {
    map = LensUtil.getHashMap(args);
  }

  public void put(String key, String value) {
    map.put(key, value);
  }

  public String get(String key) {
    return map.get(key);
  }

  public void remove(String key) {
    map.remove(key);
  }

  public void clear() {
    map.clear();
  }

  public HashMap<String, String> getMap() {
    return map;
  }

}
