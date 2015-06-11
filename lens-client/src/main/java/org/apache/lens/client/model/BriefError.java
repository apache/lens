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
package org.apache.lens.client.model;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.lens.api.result.PrettyPrintable;

import org.apache.commons.lang.StringUtils;

public class BriefError implements PrettyPrintable {

  private final int errorCode;
  private final String errorMsg;

  public BriefError(final int errorCode, final String errorMsg) {

    checkArgument(errorCode > 0);
    checkArgument(StringUtils.isNotBlank(errorMsg));
    this.errorCode = errorCode;
    this.errorMsg = errorMsg;
  }

  @Override
  public String toPrettyString() {

    StringBuilder sb = new StringBuilder("Error Code: ").append(this.errorCode).append("\n").append("Error Message: ")
        .append(this.errorMsg);
    return sb.toString();
  }
}
