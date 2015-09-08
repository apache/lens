/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es.exceptions;

import org.apache.lens.server.api.LensErrorInfo;
import org.apache.lens.server.api.error.LensException;

import lombok.NonNull;

public class ESClientException extends LensException {
  public ESClientException(String s, Exception e) {
    super(s, e);
  }

  public ESClientException(String errorMsg) {
    super(errorMsg);
  }

  public ESClientException(String errorMsg, Throwable cause) {
    super(errorMsg, cause);
  }

  public ESClientException() {
  }

  public ESClientException(Throwable cause) {
    super(cause);
  }

  public ESClientException(LensErrorInfo errorInfo) {
    super(errorInfo);
  }

  public ESClientException(String errorMsg, LensErrorInfo errorInfo) {
    super(errorMsg, errorInfo);
  }

  public ESClientException(LensErrorInfo errorInfo, Throwable cause,
                           @NonNull Object... errorMsgFormattingArgs) {
    super(errorInfo, cause, errorMsgFormattingArgs);
  }

  public ESClientException(String errorMsg, LensErrorInfo errorInfo, Throwable cause,
                           @NonNull Object... errorMsgFormattingArgs) {
    super(errorMsg, errorInfo, cause, errorMsgFormattingArgs);
  }
}
