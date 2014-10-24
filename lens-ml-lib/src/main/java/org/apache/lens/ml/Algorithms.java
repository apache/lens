/*
 * #%L
 * Lens ML Lib
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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
package org.apache.lens.ml;

import org.apache.lens.api.LensException;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Algorithms {
  private final Map<String, Class<? extends MLTrainer>> algorithmClasses =
      new HashMap<String, Class<? extends MLTrainer>>();

  public void register(Class<? extends MLTrainer> trainerClass) {
    if (trainerClass != null && trainerClass.getAnnotation(Algorithm.class) != null) {
      algorithmClasses.put(trainerClass.getAnnotation(Algorithm.class).name(),
          trainerClass);
    } else {
      throw new IllegalArgumentException("Not a valid algorithm class: " + trainerClass);
    }
  }

  public MLTrainer getTrainerForName(String name) throws LensException {
    Class<? extends MLTrainer> trainerClass = algorithmClasses.get(name);
    if (trainerClass == null) {
      return null;
    }
    Algorithm algoAnnotation = trainerClass.getAnnotation(Algorithm.class);
    String description = algoAnnotation.description();
    try {
      Constructor<? extends MLTrainer> trainerConstructor =
          trainerClass.getConstructor(String.class, String.class);
      return trainerConstructor.newInstance(name, description);
    } catch (Exception exc) {
      throw new LensException("Unable to get trainer: " + name, exc);
    }
  }

  public boolean isAlgoSupported(String name) {
    return algorithmClasses.containsKey(name);
  }

  public List<String> getAlgorithmNames() {
    return new ArrayList<String>(algorithmClasses.keySet());
  }

}
