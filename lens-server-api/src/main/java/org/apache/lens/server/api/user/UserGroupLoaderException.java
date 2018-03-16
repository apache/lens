package org.apache.lens.server.api.user;

public class UserGroupLoaderException extends RuntimeException {

  /**
   * Instantiates a new user group loader exception.
   */
  public UserGroupLoaderException() {
    super();
  }

  /**
   * Instantiates a new user group loader exception.
   *
   * @param s the s
   */
  public UserGroupLoaderException(String s) {
    super(s);
  }

  /**
   * Instantiates a new user group loader exception.
   *
   * @param e the e
   */
  public UserGroupLoaderException(Throwable e) {
    super(e);
  }

  /**
   * Instantiates a new user grouploader exception.
   *
   * @param message the message
   * @param cause   the cause
   */
  public UserGroupLoaderException(String message, Throwable cause) {
    super(message, cause);
  }
}
