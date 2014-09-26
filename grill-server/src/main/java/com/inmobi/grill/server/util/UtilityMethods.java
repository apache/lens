package com.inmobi.grill.server.util;

/*
 * #%L
 * Grill Server
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

import java.util.Map;

public class UtilityMethods {
  public static <K, V> void mergeMaps(Map<K, V> into, Map<K, V> from, boolean override) {
    for(K key: from.keySet()) {
      if(override || !into.containsKey(key)) {
        into.put(key, from.get(key));
      }
    }
  }
  public static String removeDomain(String username) {
    if(username.contains("@")) {
      username = username.substring(0, username.indexOf("@"));
    }
    return username;
  }
}
