package org.apache.lens.ml;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The Interface TrainerParam.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TrainerParam {

  /**
   * Name.
   *
   * @return the string
   */
  String name();

  /**
   * Help.
   *
   * @return the string
   */
  String help();

  /**
   * Default value.
   *
   * @return the string
   */
  String defaultValue() default "None";
}
