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


import org.apache.lens.api.query.save.ParameterDataType;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;

/**
 * The class ValueEncodeException.
 * Thrown when the value cannot be encoded according to the data type specified in the definition.
 */
public class ValueEncodeException extends LensException {
  @Getter
  private final ParameterDataType dataType;
  @Getter
  private final Object valueSupplied;

  public ValueEncodeException(ParameterDataType dataType, Object valueSupplied, Throwable cause) {
    super(valueSupplied + " cannot be encoded as " + dataType.name(), cause);
    this.dataType = dataType;
    this.valueSupplied = valueSupplied;
  }

  public ValueEncodeException(ParameterDataType dataType, Object valueSupplied) {
    super(valueSupplied + " cannot be encoded as " + dataType.name());
    this.dataType = dataType;
    this.valueSupplied = valueSupplied;
  }

}
