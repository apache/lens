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
package org.apache.lens.api;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.*;

import org.apache.commons.lang.StringUtils;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@XmlRootElement
@EqualsAndHashCode
@ToString
@XmlAccessorType(XmlAccessType.FIELD)
@NoArgsConstructor
public class SupportedOperations<T> implements Serializable {

  private static final String SEP = ", ";

  @XmlElementWrapper(name = "supportedOperations")
  @XmlElement(name = "operation")
  private List<String> supportedOps = new LinkedList<String>();

  public SupportedOperations(T... supportedOps) {
    for (T supportedOp : supportedOps) {
      this.supportedOps.add(supportedOp.toString().toLowerCase());
    }
  }

  public String getSupportedOperationsAsString() {
    return StringUtils.join(supportedOps, SEP);
  }
}
