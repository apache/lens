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
package org.apache.lens.server.api.query.save;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.lens.api.query.save.Parameter;
import org.apache.lens.api.query.save.ParameterCollectionType;
import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.save.param.ParameterDataTypeEncoder;
import org.apache.lens.server.api.query.save.param.ParameterResolver;

import com.google.common.collect.Lists;

public final class SavedQueryHelper {

  private SavedQueryHelper() {

  }

  /**
   * Extracts the default parameter values from the saved query
   *
   * @param query
   * @return multivalued map of parameter values
   */
  public static MultivaluedMap<String, String> getDefaultParameterValues(SavedQuery query) {
    final MultivaluedMap<String, String> defaults = new MultivaluedHashMap<>();
    for(Parameter parameter: query.getParameters()) {
      String[] defaultValues = parameter.getDefaultValue();
      if (defaultValues != null && defaultValues.length !=0) {
        defaults.addAll(parameter.getName(), defaultValues);
      }
    }
    return defaults;
  }

  /**
   * Gets a sample query for saved query by auto resolving the parameters
   *
   * @param savedQuery
   * @return hql query
   * @throws LensException
   */
  public static String getSampleResolvedQuery(SavedQuery savedQuery) throws LensException {
    return ParameterResolver.resolve(savedQuery, extrapolateSampleParamValues(savedQuery));
  }

  /**
   * Given a saved query, this method extrapolates the parameter values (using the parameter definition)
   * and returns it as a multivalued map
   *
   * @param savedQuery
   * @return multivalued map containing parameter values
   */
  private static MultivaluedMap<String, String> extrapolateSampleParamValues(SavedQuery savedQuery) {
    final MultivaluedHashMap<String, String> paramValues = new MultivaluedHashMap<>();
    for(Parameter parameter : savedQuery.getParameters()) {
      final String sampleValue = ParameterDataTypeEncoder.valueOf(parameter.getDataType().toString()).getSampleValue();
      if (parameter.getCollectionType() == ParameterCollectionType.SINGLE) {
        paramValues.putSingle(
          parameter.getName(),
          sampleValue
        );
      } else if (parameter.getCollectionType() == ParameterCollectionType.MULTIPLE) {
        paramValues.put(
          parameter.getName(),
          Lists.newArrayList(sampleValue, sampleValue)
        );
      }
    }
    return paramValues;
  }

}
