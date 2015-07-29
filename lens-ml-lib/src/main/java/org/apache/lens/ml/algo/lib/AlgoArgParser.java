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
package org.apache.lens.ml.algo.lib;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.ml.algo.api.Algorithm;
import org.apache.lens.ml.api.AlgoParam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * AlgoArgParser class. Parses and sets algo params.
 */
@Slf4j
public final class AlgoArgParser {
  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(AlgoArgParser.class);

  private AlgoArgParser() {
  }

  /**
   * Extracts all the variables annotated with @AlgoParam checks if any input key matches the name
   * if so replaces the value.
   *
   * @param algo
   * @param algoParameters
   */
  public static void parseArgs(Algorithm algo, Map<String, String> algoParameters) {
    Class<? extends Algorithm> algoClass = algo.getClass();
    // Get param fields
    Map<String, Field> fieldMap = new HashMap<String, Field>();

    for (Field fld : algoClass.getDeclaredFields()) {
      fld.setAccessible(true);
      AlgoParam paramAnnotation = fld.getAnnotation(AlgoParam.class);
      if (paramAnnotation != null) {
        fieldMap.put(paramAnnotation.name(), fld);
      }
    }

    for (Map.Entry<String, String> entry : algoParameters.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      try {
        if (fieldMap.containsKey(key)) {
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
        LOG.error("Error while setting param " + key + " to " + value + " for algo " + algo, exc);
      }
    }
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
}
