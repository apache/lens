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

package org.apache.lens.server.api.query.priority;

import java.util.TreeMap;

/**
 * Class for storing range configurations. An Example would be grading system.
 * The value F,30,D,40,C,60,B,80,A corresponds to a system where
 * - inf &lt; marks &lt;= 30         : F
 * 30 &lt; marks &lt;= 40            : D
 * 40 &lt; marks &lt;= 60            : C
 * 60 &lt; marks &lt;= 80            : B
 * 80 &lt; marks &lt;= + Inf         : A
 * <p></p>
 * rangeConfInstance.get(marks) would give you the grade depending on the range.
 * <p></p>
 * The utility is for easily storing range configs in config xml files.
 * <p></p>
 * Implementation is done by storing the least value(floor) and keeping a treemap on rest of values
 *
 * @param <K> Key type. Integer in the grade range example
 * @param <V> Value Type. String(or a Grade class) in the grade range example
 */

public abstract class RangeConf<K extends Comparable<K>, V> {

  /**
   * The data structure is one map and one floor value
   *
   */

  /**
   * Treemap of lower boundary value and next grade
   */
  TreeMap<K, V> map = new TreeMap<K, V>();

  /**
   * Lowest boundary value.
   */
  V floor;

  /**
   * Constructor. parses the string to form the data structure.
   *
   * @param confValue
   */
  public RangeConf(String confValue) {
    if (confValue == null || confValue.isEmpty()) {
      confValue = getDefaultConf();
    }
    String[] split = confValue.split("\\s*,\\s*");
    assert split.length % 2 == 1;
    floor = parseValue(split[0]);
    for (int i = 1; i < split.length; i += 2) {
      map.put(parseKey(split[i]), parseValue(split[i + 1]));
    }
  }

  /**
   * parse key type from its string representation
   */
  protected abstract K parseKey(String s);

  /**
   * parse value type from its string representation
   */
  protected abstract V parseValue(String s);

  /**
   * When null/blank conf string passed, this would be the value from which data structure will be formed
   */
  protected abstract String getDefaultConf();

  /**
   * Get method.
   *
   * @param key
   * @return the value depending on which range the key lies in
   */
  public V get(K key) {
    return map.floorEntry(key) == null ? floor : map.floorEntry(key).getValue();
  }

  /**
   * toString representation
   *
   * @return string representation
   */
  @Override
  public String toString() {
    return "lower value: " + floor + ", map: " + map;
  }
}
