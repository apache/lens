package org.apache.lens.client.exceptions;

/**
 * The Class LensClientException.
 */
public class LensClientException extends RuntimeException {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The message. */
  private final String message;

  /** The cause. */
  private Exception cause;

  /**
   * Instantiates a new lens client exception.
   *
   * @param message
   *          the message
   * @param cause
   *          the cause
   */
  public LensClientException(String message, Exception cause) {
    this.message = message;
    this.cause = cause;
  }

  /**
   * Instantiates a new lens client exception.
   *
   * @param message
   *          the message
   */
  public LensClientException(String message) {
    this.message = message;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public Exception getCause() {
    return cause;
  }
}
