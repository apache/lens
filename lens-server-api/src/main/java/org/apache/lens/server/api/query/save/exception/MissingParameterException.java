/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.query.save.exception;

import static org.apache.lens.api.error.LensCommonErrorCode.MISSING_PARAMETERS;

import java.util.Collection;

import org.apache.lens.server.api.LensErrorInfo;
import org.apache.lens.server.api.error.LensException;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * The class MissingParameterException. Thrown when a required parameter is not found.
 */
public class MissingParameterException extends LensException {

  @Getter
  private final ImmutableList<String> missingParameters;

  public MissingParameterException(Collection<String> missingParameters) {
    super(
      new LensErrorInfo(MISSING_PARAMETERS.getValue(), 0, MISSING_PARAMETERS.toString())
      , Joiner.on(",").join(missingParameters));
    this.missingParameters = ImmutableList.copyOf(missingParameters);
  }
}
