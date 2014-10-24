package org.apache.lens.client.exceptions;

/**
 * The Class LensClientServerConnectionException.
 */
public class LensClientServerConnectionException extends LensClientException {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The error code. */
  private int errorCode;

  /**
   * Instantiates a new lens client server connection exception.
   *
   * @param errorCode
   *          the error code
   */
  public LensClientServerConnectionException(int errorCode) {
    super("Server Connection gave error code " + errorCode);
    this.errorCode = errorCode;
  }

  /**
   * Instantiates a new lens client server connection exception.
   *
   * @param message
   *          the message
   * @param e
   *          the e
   */
  public LensClientServerConnectionException(String message, Exception e) {
    super(message, e);
  }

  public int getErrorCode() {
    return errorCode;
  }
}
