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

public class InvalidQueryException extends LensException {
  static final String BASE_EXCEPTION_MESSAGE = "AST traversal failed!";

  public InvalidQueryException(String s) {
    super(BASE_EXCEPTION_MESSAGE + s);
  }

  public InvalidQueryException(String errorMsg, Throwable cause) {
    super(errorMsg, cause);
  }

  public InvalidQueryException() {
  }

  public InvalidQueryException(Throwable cause) {
    super(cause);
  }

  public InvalidQueryException(LensErrorInfo errorInfo) {
    super(errorInfo);
  }

  public InvalidQueryException(String errorMsg, LensErrorInfo errorInfo) {
    super(errorMsg, errorInfo);
  }

  public InvalidQueryException(LensErrorInfo errorInfo, Throwable cause,
                               @NonNull Object... errorMsgFormattingArgs) {
    super(errorInfo, cause, errorMsgFormattingArgs);
  }

  public InvalidQueryException(String errorMsg, LensErrorInfo errorInfo, Throwable cause,
                               @NonNull Object... errorMsgFormattingArgs) {
    super(errorMsg, errorInfo, cause, errorMsgFormattingArgs);
  }

  public InvalidQueryException(String s, Exception e) {
    super(BASE_EXCEPTION_MESSAGE + s, e);
  }

}
