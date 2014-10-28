package org.apache.lens.ml;

public class MLUtils {
  public static String getTrainerName(Class<? extends MLTrainer> trainerClass) {
    Algorithm annotation = trainerClass.getAnnotation(Algorithm.class);
    if (annotation != null) {
      return annotation.name();
    }
    throw new IllegalArgumentException("Trainer should be decorated with annotation - " + Algorithm.class.getName());
  }
}
