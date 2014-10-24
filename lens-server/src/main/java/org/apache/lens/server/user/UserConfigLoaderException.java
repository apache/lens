package org.apache.lens.server.user;

/**
 * The Class UserConfigLoaderException.
 */
public class UserConfigLoaderException extends RuntimeException {

  /**
   * Instantiates a new user config loader exception.
   */
  public UserConfigLoaderException() {
    super();
  }

  /**
   * Instantiates a new user config loader exception.
   *
   * @param s
   *          the s
   */
  public UserConfigLoaderException(String s) {
    super(s);
  }

  /**
   * Instantiates a new user config loader exception.
   *
   * @param e
   *          the e
   */
  public UserConfigLoaderException(Throwable e) {
    super(e);
  }

  /**
   * Instantiates a new user config loader exception.
   *
   * @param message
   *          the message
   * @param cause
   *          the cause
   */
  public UserConfigLoaderException(String message, Throwable cause) {
    super(message, cause);
  }
}
