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

package org.apache.lens.server.api.priority;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.api.LensConfConstants;

import java.util.TreeMap;

public abstract class RangeConf<K extends Comparable<K>, V> {
  TreeMap<K, V> map = new TreeMap<K, V>();
  V floor;
  RangeConf(String confValue) {
    if(confValue == null || confValue.isEmpty()) {
      confValue = getDefaultConf();
    }
    String[] split = confValue.split("\\s*,\\s*");
    assert split.length % 2 == 1;
    floor = parseValue(split[0]);
    for(int i = 1; i < split.length; i += 2) {
      map.put(parseKey(split[i]), parseValue(split[i + 1]));
    }
  }

  protected abstract K parseKey(String s);
  protected abstract V parseValue(String s);
  protected abstract String getDefaultConf();
  public V get(K key) {
    return map.floorEntry(key) == null ? floor : map.floorEntry(key).getValue();
  }
  static String getFirstNonNullValueFromConf(Configuration conf, String... keys) {
    for (String key: keys){
      if(conf.get(key) != null) {
        return conf.get(key);
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "lower value: " + floor + ", map: " + map;
  }
}