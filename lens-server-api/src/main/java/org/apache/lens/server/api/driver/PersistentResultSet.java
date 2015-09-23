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
package org.apache.lens.server.api.driver;

import org.apache.lens.api.query.PersistentQueryResult;
import org.apache.lens.server.api.error.LensException;

/**
 * The Class PersistentResultSet.
 */
public abstract class PersistentResultSet extends LensResultSet {

  @Override
  public boolean canBePurged() {
    return true;
  }

  /**
   * Get the size of the result set file.
   *
   * @return The size if available, null if not available.
   * @throws LensException the lens exception
   */
  public abstract Long getFileSize() throws LensException;

  public String getHttpResultUrl() {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensResultSet#toQueryResult()
   */
  public PersistentQueryResult toQueryResult() throws LensException {
    return new PersistentQueryResult(getOutputPath(), size(), getFileSize(), getHttpResultUrl());
  }
  public boolean isHttpResultAvailable() throws LensException {
    return false;
  }
}
