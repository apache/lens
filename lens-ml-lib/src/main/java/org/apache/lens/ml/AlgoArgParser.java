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
package org.apache.lens.ml;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Class AlgoArgParser.
 */
public final class AlgoArgParser {
  private AlgoArgParser() {
  }

  /**
   * The Class CustomArgParser.
   *
   * @param <E> the element type
   */
  public abstract static class CustomArgParser<E> {

    /**
     * Parses the.
     *
     * @param value the value
     * @return the e
     */
    public abstract E parse(String value);
  }

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(AlgoArgParser.class);

  /**
   * Extracts feature names. If the algo has any parameters associated with @AlgoParam annotation, those are set
   * as well.
   *
   * @param algo the algo
   * @param args    the args
   * @return List of feature column names.
   */
  public static List<String> parseArgs(MLAlgo algo, String[] args) {
    List<String> featureColumns = new ArrayList<String>();
    Class<? extends MLAlgo> algoClass = algo.getClass();
    // Get param fields
    Map<String, Field> fieldMap = new HashMap<String, Field>();

    for (Field fld : algoClass.getDeclaredFields()) {
      fld.setAccessible(true);
      AlgoParam paramAnnotation = fld.getAnnotation(AlgoParam.class);
      if (paramAnnotation != null) {
        fieldMap.put(paramAnnotation.name(), fld);
      }
    }

    for (int i = 0; i < args.length; i += 2) {
      String key = args[i].trim();
      String value = args[i + 1].trim();

      try {
        if ("feature".equalsIgnoreCase(key)) {
          featureColumns.add(value);
        } else if (fieldMap.containsKey(key)) {
          Field f = fieldMap.get(key);
          if (String.class.equals(f.getType())) {
            f.set(algo, value);
          } else if (Integer.TYPE.equals(f.getType())) {
            f.setInt(algo, Integer.parseInt(value));
          } else if (Double.TYPE.equals(f.getType())) {
            f.setDouble(algo, Double.parseDouble(value));
          } else if (Long.TYPE.equals(f.getType())) {
            f.setLong(algo, Long.parseLong(value));
          } else {
            // check if the algo provides a deserializer for this param
            String customParserClass = algo.getConf().getProperties().get("lens.ml.args." + key);
            if (customParserClass != null) {
              Class<? extends CustomArgParser<?>> clz = (Class<? extends CustomArgParser<?>>) Class
                .forName(customParserClass);
              CustomArgParser<?> parser = clz.newInstance();
              f.set(algo, parser.parse(value));
            } else {
              LOG.warn("Ignored param " + key + "=" + value + " as no parser found");
            }
          }
        }
      } catch (Exception exc) {
        LOG.error("Error while setting param " + key + " to " + value + " for algo " + algo);
      }
    }
    return featureColumns;
  }
}
