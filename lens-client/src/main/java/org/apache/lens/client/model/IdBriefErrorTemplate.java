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

import org.apache.lens.api.result.PrettyPrintable;

import org.apache.commons.lang.StringUtils;

import lombok.Data;
import lombok.NonNull;

@Data
public class IdBriefErrorTemplate implements PrettyPrintable {

  private final IdBriefErrorTemplateKey idKey;
  private final String idValue;
  @NonNull
  private final BriefError briefError;

  @Override
  public String toPrettyString() {

    StringBuilder sb = new StringBuilder();
    if (idKey != null && StringUtils.isNotBlank(idValue)) {
      sb.append(idKey.getConstant()).append(": ").append(this.idValue).append("\n");
    }
    sb.append(this.briefError.toPrettyString());
    return sb.toString();
  }
}

