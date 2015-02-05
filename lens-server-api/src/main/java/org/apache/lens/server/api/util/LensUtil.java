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
package org.apache.lens.server.api.util;

import org.apache.commons.lang3.StringUtils;

/**
 * Utility methods for Lens
 */
public final class LensUtil {

  private LensUtil() {

  }

  /**
   * Get the message corresponding to base cause. If no cause is available or no message is available
   *  parent's message is returned.
   *
   * @param e
   * @return message
   */
  public static String getCauseMessage(Throwable e) {
    String expMsg = null;
    if (e.getCause() != null) {
      expMsg = getCauseMessage(e.getCause());
    }
    if (StringUtils.isBlank(expMsg)) {
      expMsg = e.getLocalizedMessage();
    }
    return expMsg;
  }

}
