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
