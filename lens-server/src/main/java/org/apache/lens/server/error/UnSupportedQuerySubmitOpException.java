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
package org.apache.lens.server.error;

import static org.apache.lens.server.error.LensServerErrorCode.UNSUPPORTED_QUERY_SUBMIT_OPERATION;

import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.error.LensError;
import org.apache.lens.api.response.LensErrorTO;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.query.SupportedQuerySubmitOperations;

public class UnSupportedQuerySubmitOpException extends LensException {

  private final SupportedQuerySubmitOperations supportedOps = new SupportedQuerySubmitOperations();

  public UnSupportedQuerySubmitOpException() {
    super(UNSUPPORTED_QUERY_SUBMIT_OPERATION.getValue());
  }

  public UnSupportedQuerySubmitOpException(final Throwable cause) {
    super(UNSUPPORTED_QUERY_SUBMIT_OPERATION.getValue(), cause);
  }

  @Override
  protected String getFormattedErrorMsg(LensError lensError) {

    final String supportedOpsStr = supportedOps.getSupportedOperationsAsString();
    return lensError.getFormattedErrorMsg(supportedOpsStr);
  }

  @Override
  protected LensErrorTO buildLensErrorTO(final ErrorCollection errorCollection, final String errorMsg,
      final String stackTrace) {

    return LensErrorTO.composedOf(getErrorCode(), errorMsg, stackTrace, supportedOps, null);
  }
}
