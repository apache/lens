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
package org.apache.lens.api.response;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

import org.apache.commons.lang.StringUtils;

import lombok.*;

/**
 *
 * Transport object for lens error information.
 *
 * @param <PAYLOAD>  represents type of error payload transferred in failure lens response
 */
@EqualsAndHashCode(exclude = { "stackTrace" })
@ToString
@NoArgsConstructor(access =  AccessLevel.PACKAGE)
@XmlAccessorType(XmlAccessType.FIELD)
public class LensErrorTO<PAYLOAD> {

  @XmlElement
  private int code;

  @XmlElement
  private String message;

  @XmlElement
  private String stackTrace;

  @XmlElement
  @Getter
  private PAYLOAD payload;

  @Getter
  @XmlElementWrapper(name = "childErrors")
  @XmlElement(name = "error")
  private List<LensErrorTO> childErrors;

  public static <PAYLOAD> LensErrorTO<PAYLOAD> composedOf(final int code, final String message,
      final String stackTrace, final PAYLOAD payload, final List<LensErrorTO> childErrors) {

    return new LensErrorTO<PAYLOAD>(code, message, stackTrace, payload, childErrors);
  }

  public static <PAYLOAD> LensErrorTO<PAYLOAD> composedOf(final int code, final String message,
      final String stackTrace, final PAYLOAD payload) {

    return new LensErrorTO<PAYLOAD>(code, message, stackTrace, payload, null);
  }

  public static LensErrorTO<NoErrorPayload> composedOf(final int code, final String message,
      final String stackTrace) {

    return new LensErrorTO<NoErrorPayload>(code, message, stackTrace, null, null);
  }

  public static LensErrorTO<NoErrorPayload> composedOf(final int code, final String message,
      final String stackTrace, final List<LensErrorTO> childErrors) {

    return new LensErrorTO<NoErrorPayload>(code, message, stackTrace, null, childErrors);
  }

  private LensErrorTO(final int code, final String message, final String stackTrace, final PAYLOAD errorPayload,
      final List<LensErrorTO> childErrors) {

    checkArgument(code > 0);
    checkArgument(StringUtils.isNotBlank(message));
    checkArgument(StringUtils.isNotBlank(stackTrace));

    this.code = code;
    this.message = message;
    this.stackTrace = stackTrace;
    this.payload = errorPayload;
    this.childErrors = childErrors;
  }

  public boolean areValidStackTracesPresent() {

    /* if stack trace of first level error is not valid, then return false */
    if (StringUtils.isBlank(stackTrace)) {
      return false;
    }

    /* validate stack traces of child Errors */
    if (childErrors != null) {
      for (LensErrorTO childError : childErrors) {
        if (!childError.areValidStackTracesPresent()) {
          return false;
        }
      }
    }
    return true;
  }
}
