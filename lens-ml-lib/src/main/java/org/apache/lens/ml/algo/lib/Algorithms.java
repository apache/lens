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
import org.apache.lens.ml.algo.api.MLAlgo;
import org.apache.lens.server.api.error.LensException;

/**
 * The Class Algorithms.
 */
public class Algorithms {

  /** The algorithm classes. */
  private final Map<String, Class<? extends MLAlgo>> algorithmClasses
    = new HashMap<String, Class<? extends MLAlgo>>();

  /**
   * Register.
   *
   * @param algoClass the algo class
   */
  public void register(Class<? extends MLAlgo> algoClass) {
    if (algoClass != null && algoClass.getAnnotation(Algorithm.class) != null) {
      algorithmClasses.put(algoClass.getAnnotation(Algorithm.class).name(), algoClass);
    } else {
      throw new IllegalArgumentException("Not a valid algorithm class: " + algoClass);
    }
  }

  /**
   * Gets the algo for name.
   *
   * @param name the name
   * @return the algo for name
   * @throws LensException the lens exception
   */
  public MLAlgo getAlgoForName(String name) throws LensException {
    Class<? extends MLAlgo> algoClass = algorithmClasses.get(name);
    if (algoClass == null) {
      return null;
    }
    Algorithm algoAnnotation = algoClass.getAnnotation(Algorithm.class);
    String description = algoAnnotation.description();
    try {
      Constructor<? extends MLAlgo> algoConstructor = algoClass.getConstructor(String.class, String.class);
      return algoConstructor.newInstance(name, description);
    } catch (Exception exc) {
      throw new LensException("Unable to get algo: " + name, exc);
    }
  }

  /**
   * Checks if is algo supported.
   *
   * @param name the name
   * @return true, if is algo supported
   */
  public boolean isAlgoSupported(String name) {
    return algorithmClasses.containsKey(name);
  }

  public List<String> getAlgorithmNames() {
    return new ArrayList<String>(algorithmClasses.keySet());
  }

}
