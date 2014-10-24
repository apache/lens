package org.apache.lens.ml;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The Interface Algorithm.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Algorithm {

  /**
   * Name.
   *
   * @return the string
   */
  String name();

  /**
   * Description.
   *
   * @return the string
   */
  String description();
}
