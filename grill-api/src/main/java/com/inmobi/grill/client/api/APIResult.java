package com.inmobi.grill.client.api;

import org.apache.log4j.NDC;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringWriter;
import java.util.UUID;

/**
 * APIResult is the output returned by all the APIs; status-SUCCEEDED or FAILED
 * message- detailed message.
 */
@XmlRootElement(name = "result")
@XmlAccessorType(XmlAccessType.FIELD)
public class APIResult {
  private Status status;

  private String message;

  private String requestId;

  private static final JAXBContext JAXB_CONTEXT;

  static {
      try {
          JAXB_CONTEXT = JAXBContext.newInstance(APIResult.class);
      } catch (JAXBException e) {
          throw new RuntimeException(e);
      }
  }

  /**
   * API Result status.
   */
  public static enum Status {
      SUCCEEDED, PARTIAL, FAILED
  }

  public APIResult(Status status, String message) {
      super();
      this.status = status;
      this.message = message;
      requestId = NDC.peek();
      try {
          UUID.fromString(requestId);
      } catch (IllegalArgumentException e) {
          requestId = null;
      }
  }

  protected APIResult() {
      // private default constructor for JAXB
  }

  public Status getStatus() {
      return status;
  }

  public String getMessage() {
      return message;
  }

  public String getRequestId() {
      return requestId;
  }

  public void setRequestId(String reqId) {
      this.requestId = reqId;
  }

  @Override
  public String toString() {
      try {
          StringWriter stringWriter = new StringWriter();
          Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
          marshaller.marshal(this, stringWriter);
          return stringWriter.toString();
      } catch (JAXBException e) {
          return e.getMessage();
      }
  }

}
