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
package org.apache.lens.api.util;


import java.util.HashMap;
import java.util.Map;

public class CommonUtils {
  private CommonUtils() {

  }

  public interface EntryParser<K, V> {
    K parseKey(String str);

    V parseValue(String str);
  }

  private static EntryParser<String, String> defaultEntryParser = new EntryParser<String, String>() {
    @Override
    public String parseKey(String str) {
      return str;
    }

    @Override
    public String parseValue(String str) {
      return str;
    }
  };


  /**
   * Splits given String str around non-escaped commas. Then parses each of the split element
   * as map entries in the format `key=value`. Constructs a map of such entries.
   * e.g. "a=b, c=d" parses to map{a:b, c:d} where the symbols are self-explanatory.
   *
   * @param str The string to parse
   * @return parsed map
   */
  public static Map<String, String> parseMapFromString(String str) {
    return parseMapFromString(str, defaultEntryParser);
  }

  public static <K, V> Map<K, V> parseMapFromString(String str, EntryParser<K, V> parser) {
    Map<K, V> map = new HashMap<>();
    if (str != null) {
      for (String kv : str.split("(?<!\\\\),")) {
        if (!kv.isEmpty()) {
          String[] kvArray = kv.split("=");
          String key = "";
          String value = "";
          if (kvArray.length > 0) {
            key = kvArray[0].replaceAll("\\\\,", ",").trim();
          }
          if (kvArray.length > 1) {
            value = kvArray[1].replaceAll("\\\\,", ",").trim();
          }
          map.put(parser.parseKey(key), parser.parseValue(value));
        }
      }
    }
    return map;
  }
}
