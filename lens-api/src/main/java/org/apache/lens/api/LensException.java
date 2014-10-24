/*
 * 
 */
package org.apache.lens.api;

/**
 * The Class LensException.
 */
@SuppressWarnings("serial")
public class LensException extends Exception {

  /**
   * Instantiates a new lens exception.
   *
   * @param msg
   *          the msg
   */
  public LensException(String msg) {
    super(msg);
  }

  /**
   * Instantiates a new lens exception.
   *
   * @param msg
   *          the msg
   * @param th
   *          the th
   */
  public LensException(String msg, Throwable th) {
    super(msg, th);
  }

  /**
   * Instantiates a new lens exception.
   */
  public LensException() {
    super();
  }

  /**
   * Instantiates a new lens exception.
   *
   * @param th
   *          the th
   */
  public LensException(Throwable th) {
    super(th);
  }
}
