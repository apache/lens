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

import java.util.List;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.save.param.ParameterCollectionTypeEncoder;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * The class ParameterCollectionException. Thrown when the collection type of the parameter
 * and the encoded values are different.
 */
public class ParameterCollectionException extends LensException {

  @Getter
  private final ParameterCollectionTypeEncoder collectionType;
  @Getter
  private final ImmutableList<String> values;

  public ParameterCollectionException(
    ParameterCollectionTypeEncoder collectionType, List<String> values, String message) {
    super(values + " cannot be encoded as " + collectionType.name() + ", Reason : " + message);
    this.collectionType = collectionType;
    this.values = ImmutableList.copyOf(values);
  }

  public ParameterCollectionException(
    ParameterCollectionTypeEncoder collectionType, List<String> values, Throwable cause) {
    super(values + " cannot be encoded as " + collectionType.name(), cause);
    this.collectionType = collectionType;
    this.values = ImmutableList.copyOf(values);
  }
}
