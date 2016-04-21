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

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.query.save.ParameterDataType;
import org.apache.lens.server.api.query.save.exception.ValueEncodeException;

import org.apache.commons.lang3.StringEscapeUtils;

/**
 * The enum ParameterDataTypeEncoder.
 * Encodes the value according to the data type.
 *
 */
@XmlRootElement
public enum ParameterDataTypeEncoder {
  STRING {
    public String encode(String rawValue) {
      return "'" + StringEscapeUtils.escapeEcmaScript(rawValue) + "'";
    }

    @Override
    public String getSampleValue() {
      return "sample_string_val";
    }
  },
  NUMBER {
    public String encode(String rawValue) {
      return String.valueOf(Long.parseLong(rawValue));
    }

    @Override
    public String getSampleValue() {
      return String.valueOf(Long.MAX_VALUE);
    }
  },
  DECIMAL {
    public String encode(String rawValue) {
      return String.valueOf(Double.parseDouble(rawValue));
    }

    @Override
    public String getSampleValue() {
      return String.valueOf(Double.MAX_VALUE);
    }
  },
  BOOLEAN {
    @Override
    public String encode(String rawValue) {
      if (rawValue.equalsIgnoreCase("true") || rawValue.equalsIgnoreCase("false")) {
        return rawValue;
      }
      throw new RuntimeException("boolean has to be strictly true or false");
    }

    @Override
    public String getSampleValue() {
      return Boolean.TRUE.toString();
    }
  };

  public abstract String encode(String rawValue);
  public abstract String getSampleValue();

  public static String encodeFor(ParameterDataType dataType, String rawValue) throws ValueEncodeException {
    try {
      return ParameterDataTypeEncoder.valueOf(dataType.toString()).encode(rawValue);
    } catch (Throwable e) {
      throw new ValueEncodeException(dataType, rawValue, e);
    }
  }

}
