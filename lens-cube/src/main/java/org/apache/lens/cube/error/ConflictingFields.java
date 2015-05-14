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
package org.apache.lens.cube.error;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.*;

import javax.xml.bind.annotation.*;

import org.apache.commons.lang.StringUtils;

import lombok.*;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@EqualsAndHashCode
@ToString
public class ConflictingFields {

  private static final String SEP = ", ";

  @XmlElementWrapper(name = "conflictingFields")
  @XmlElement(name = "field")
  private SortedSet<String> fields;

  public ConflictingFields(@NonNull final SortedSet<String> fields) {

    checkArgument(!fields.isEmpty(), "We should atleast have one conflicting field to create a valid instance.");
    for (String field : fields) {
      checkState(field != null, "Conflicting fields must not contain null. Conflicting fields: %s", fields);
    }
    this.fields = fields;
  }

  public String getConflictingFieldsString() {
    String conflictingFieldsStrJoined = StringUtils.join(fields, SEP);

    if (fields.size() == 1) {
      return conflictingFieldsStrJoined + " cannot be queried";
    } else {
      return conflictingFieldsStrJoined + " cannot be queried together";
    }
  }
}
