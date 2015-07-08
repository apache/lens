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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.ml.algo.api.Algorithm;
import org.apache.lens.server.api.error.LensException;

/**
 * The Class Algorithms.
 */
public class Algorithms {

  /**
   * The algorithm classes.
   */

  private final Map<String, Class<? extends Algorithm>> algorithmClasses
    = new HashMap<String, Class<? extends Algorithm>>();

  /**
   * Registers algorithm
   *
   * @param name
   * @param algoClass
   */
  public void register(String name, Class<? extends Algorithm> algoClass) {
    if (algoClass != null) {
      algorithmClasses.put(name, algoClass);
    }
  }

  public Algorithm getAlgoForName(String name) throws LensException {
    Class<? extends Algorithm> algoClass = algorithmClasses.get(name);

    if (algoClass == null) {
      return null;
    }
    try {
      Constructor<? extends Algorithm> constructor = algoClass.getConstructor();
      return constructor.newInstance();
    } catch (Exception e) {
      throw new LensException("Unable to get Algorithm " + name, e);
    }
  }

  /**
   * Checks if algorithm is supported
   *
   * @param name
   * @return
   */
  public boolean isAlgoSupported(String name) {
    return algorithmClasses.containsKey(name);
  }

  public List<String> getAlgorithmNames() {
    return new ArrayList<String>(algorithmClasses.keySet());
  }
}
