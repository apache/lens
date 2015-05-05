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
package org.apache.lens.api.error;

import com.google.common.collect.ImmutableSet;

/**
 * Interface used to interact with error collection created from error configuration file.
 */
public interface ErrorCollection {

  /**
   *
   * @param errorCode the errorCode for which LensError instance has to be retrieved.
   * @return the LensError instance for the given errorCode. If errorCode is not found in ErrorCollection, then a
   *         default LensError instance of error collection implementation will be returned.
   */
  LensError getLensError(final int errorCode);

  /**
   *
   * @return the Immutable set of all error payload classes stored in ErrorCollection. If there are no error payload
   *         classes, then an empty set will be returned.
   */
  ImmutableSet<Class> getErrorPayloadClasses();

}
