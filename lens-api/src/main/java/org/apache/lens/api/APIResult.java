/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.api;

import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.*;

import org.apache.lens.api.jaxb.LensJAXBContext;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * APIResult is the output returned by all the APIs; status-SUCCEEDED or FAILED message- detailed message.
 */
@XmlRootElement(name = "result")
@XmlAccessorType(XmlAccessType.FIELD)
/**
 * Instantiates a new API result.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class APIResult {

  private static final APIResult SUCCESS = new APIResult(Status.SUCCEEDED, "");
  /**
   * The status.
   */
  @XmlElement
  @Getter
  private Status status;

  /**
   * The message.
   */
  @XmlElement
  @Getter
  private String message;

  /**
   * The Constant JAXB_CONTEXT.
   */
  private static final JAXBContext JAXB_CONTEXT;

  static {
    try {
      JAXB_CONTEXT = new LensJAXBContext(APIResult.class);
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * API Result status.
   */
  public enum Status {

    /**
     * The succeeded.
     */
    SUCCEEDED,
    /**
     * The partial.
     */
    PARTIAL,
    /**
     * The failed.
     */
    FAILED
  }

  /**
   * Instantiates a new API result.
   *
   * @param status  the status
   * @param message the message
   */
  public APIResult(Status status, String message) {
    super();
    this.status = status;
    this.message = message;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
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

  public static APIResult partial(int actual, int expected) {
    return new APIResult(Status.PARTIAL, actual + " out of " + expected);
  }

  public static APIResult successOrPartialOrFailure(int actual, int expected) {
    return successOrPartialOrFailure(actual, expected, null);
  }

  public static APIResult successOrPartialOrFailure(int actual, int expected, Exception e) {
    if (actual == 0 && expected != 0) {
      return failure(e);
    }
    if (actual < expected) {
      return partial(actual, expected);
    } else {
      return success();
    }
  }

  public static APIResult success() {
    return SUCCESS;
  }

  public static APIResult failure(Exception e) {
    String cause = extractCause(e);
    return new APIResult(Status.FAILED, cause);
  }

  public static APIResult partial(Exception e) {
    String cause = extractCause(e);
    return new APIResult(Status.PARTIAL, cause);
  }

  private static String extractCause(Throwable e) {
    StringBuilder cause = new StringBuilder();
    String sep = "";
    while (e != null) {
      cause.append(sep).append(e.getMessage());
      e = e.getCause();
      sep = ": ";
    }
    return cause.toString();
  }
}
