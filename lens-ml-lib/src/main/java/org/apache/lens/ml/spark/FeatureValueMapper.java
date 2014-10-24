package org.apache.lens.ml.spark;

import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Map a feature value to a Double value usable by MLLib.
 */
public abstract class FeatureValueMapper implements Function<Object, Double>, Serializable {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
   */
  public abstract Double call(Object input);
}
