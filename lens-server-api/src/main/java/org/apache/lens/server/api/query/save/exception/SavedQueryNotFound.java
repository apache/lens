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
package org.apache.lens.server.api.query.save.exception;

import static org.apache.lens.api.error.LensCommonErrorCode.RESOURCE_NOT_FOUND;

import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.server.api.LensErrorInfo;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;

/**
 * The class SavedQueryNotFound.
 * Thrown when the requested saved query is not found.
 */
public class SavedQueryNotFound extends LensException {

  @Getter
  private long id;

  public SavedQueryNotFound(long id) {
    super(
      new LensErrorInfo(RESOURCE_NOT_FOUND.getValue(), 0, RESOURCE_NOT_FOUND.toString())
      , SavedQuery.class.getSimpleName()
      , String.valueOf(id));
    this.id = id;
  }
}
