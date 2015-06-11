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

import lombok.NonNull;

public class IdBriefErrorTemplate implements PrettyPrintable {

  private final IdBriefErrorTemplateKey idKey;
  private final String idValue;
  private final BriefError briefError;

  public IdBriefErrorTemplate(@NonNull final IdBriefErrorTemplateKey idKey, final String idValue,
      @NonNull BriefError briefError) {

    checkArgument(StringUtils.isNotBlank(idValue));
    this.idKey = idKey;
    this.idValue = idValue;
    this.briefError = briefError;
  }

  @Override
  public String toPrettyString() {

    StringBuilder sb = new StringBuilder(idKey.getConstant()).append(": ").append(this.idValue).append("\n")
        .append(this.briefError.toPrettyString());

    return sb.toString();
  }
}

