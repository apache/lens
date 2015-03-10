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
import java.util.Map;

/**
 * HashMap from String to type T where the key is case insensitive.
 *
 * @param <T>
 */
public class CaseInsensitiveStringHashMap<T> extends HashMap<String, T> {
  @Override
  public T get(Object key) {
    return super.get(caseInsensitiveKey(key));
  }

  @Override
  public T put(String key, T value) {
    return super.put(caseInsensitiveKey(key), value);
  }

  @Override
  public boolean containsKey(Object key) {
    return super.containsKey(caseInsensitiveKey(key));
  }

  private static String caseInsensitiveKey(Object key) {
    return key == null ? null : key.toString().toLowerCase();
  }

  @Override
  public void putAll(Map<? extends String, ? extends T> m) {
    for (Map.Entry<? extends String, ? extends T> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }
}
