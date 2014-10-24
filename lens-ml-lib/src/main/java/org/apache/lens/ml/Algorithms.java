package org.apache.lens.ml;

import org.apache.lens.api.LensException;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Class Algorithms.
 */
public class Algorithms {

  /** The algorithm classes. */
  private final Map<String, Class<? extends MLTrainer>> algorithmClasses = new HashMap<String, Class<? extends MLTrainer>>();

  /**
   * Register.
   *
   * @param trainerClass
   *          the trainer class
   */
  public void register(Class<? extends MLTrainer> trainerClass) {
    if (trainerClass != null && trainerClass.getAnnotation(Algorithm.class) != null) {
      algorithmClasses.put(trainerClass.getAnnotation(Algorithm.class).name(), trainerClass);
    } else {
      throw new IllegalArgumentException("Not a valid algorithm class: " + trainerClass);
    }
  }

  /**
   * Gets the trainer for name.
   *
   * @param name
   *          the name
   * @return the trainer for name
   * @throws LensException
   *           the lens exception
   */
  public MLTrainer getTrainerForName(String name) throws LensException {
    Class<? extends MLTrainer> trainerClass = algorithmClasses.get(name);
    if (trainerClass == null) {
      return null;
    }
    Algorithm algoAnnotation = trainerClass.getAnnotation(Algorithm.class);
    String description = algoAnnotation.description();
    try {
      Constructor<? extends MLTrainer> trainerConstructor = trainerClass.getConstructor(String.class, String.class);
      return trainerConstructor.newInstance(name, description);
    } catch (Exception exc) {
      throw new LensException("Unable to get trainer: " + name, exc);
    }
  }

  /**
   * Checks if is algo supported.
   *
   * @param name
   *          the name
   * @return true, if is algo supported
   */
  public boolean isAlgoSupported(String name) {
    return algorithmClasses.containsKey(name);
  }

  public List<String> getAlgorithmNames() {
    return new ArrayList<String>(algorithmClasses.keySet());
  }

}
