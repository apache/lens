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
package org.apache.lens.server.api.query.save.param;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.query.save.ParameterDataType;
import org.apache.lens.server.api.query.save.exception.ParameterCollectionException;
import org.apache.lens.server.api.query.save.exception.ValueEncodeException;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * The enum ParameterCollectionTypeEncoder.
 * Encoder for encoding the value according to the collection type.
 */
@XmlRootElement
public enum ParameterCollectionTypeEncoder {
  SINGLE {
    @Override
    public String encode(ParameterDataType dataType, List<String> values)
      throws ValueEncodeException, ParameterCollectionException {
      if (values.size() != 1) {
        throw new ParameterCollectionException(this, values, "Has to be exactly one");
      }
      return ParameterDataTypeEncoder.encodeFor(dataType, values.get(0));
    }
  },
  MULTIPLE {
    @Override
    public String encode(final ParameterDataType dataType, List<String> values)
      throws ValueEncodeException, ParameterCollectionException {
      if (values.size() <= 0) {
        throw new ParameterCollectionException(this, values, "Need atleast one value");
      }
      final StringBuilder builder = new StringBuilder("(");
      List<String> transformedValues = Lists.newArrayList();
      for (String rawValue : values) {
        transformedValues.add(ParameterDataTypeEncoder.encodeFor(dataType, rawValue));
      }
      Joiner.on(',').skipNulls().appendTo(
        builder,
        transformedValues
      );
      return builder.append(")").toString();
    }
  };

  public abstract String encode(ParameterDataType dataType, List<String> value) throws
    ValueEncodeException, ParameterCollectionException;
}
