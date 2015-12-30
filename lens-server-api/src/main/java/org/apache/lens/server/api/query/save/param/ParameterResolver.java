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
package org.apache.lens.server.api.query.save.param;

import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.lens.api.query.save.Parameter;
import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.server.api.query.save.SavedQueryHelper;
import org.apache.lens.server.api.query.save.exception.MissingParameterException;
import org.apache.lens.server.api.query.save.exception.ParameterCollectionException;
import org.apache.lens.server.api.query.save.exception.ParameterValueException;
import org.apache.lens.server.api.query.save.exception.ValueEncodeException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * The class ParameterResolver.
 * Takes care of resolving the parameter with values specified for the given query
 *
 */
public class ParameterResolver {

  private final ImmutableMap<String, Parameter> parameterMap;
  private final SavedQuery savedQuery;
  private final MultivaluedMap<String, String> queryParameters;

  private ParameterResolver(SavedQuery savedQuery, MultivaluedMap<String, String> queryParameters) {
    final ImmutableMap.Builder<String, Parameter> builder = ImmutableMap.builder();
    for (Parameter parameter : savedQuery.getParameters()) {
      builder.put(parameter.getName(), parameter);
    }
    parameterMap = builder.build();
    this.savedQuery = savedQuery;
    this.queryParameters = SavedQueryHelper.getDefaultParameterValues(savedQuery);
    this.queryParameters.putAll(queryParameters);
  }

  /**
   *
   * @return resolved query
   * @throws ParameterValueException, IllegalArgumentException
   *
   * The function resolves the provided values for the bind params.
   * A MissingParameterException is thrown if a value is not provided for one of the params
   *
   */
  private String resolve() throws ParameterValueException, MissingParameterException {
    final Sets.SetView<String> unProvidedParameters
      = Sets.difference(parameterMap.keySet(), queryParameters.keySet());
    if (unProvidedParameters.size() > 0) {
      throw new MissingParameterException(unProvidedParameters);
    }
    final StringBuilder query = new StringBuilder(savedQuery.getQuery());
    for (Map.Entry<String, List<String>> parameter : queryParameters.entrySet()) {
      final String parameterName = parameter.getKey();
      final List<String> values = parameter.getValue();
      final Parameter parameterDetail = parameterMap.get(parameterName);
      final String encodedValue;
      try {
        encodedValue = ParameterCollectionTypeEncoder
          .valueOf(parameterDetail.getCollectionType().toString())
          .encode(
            parameterDetail.getDataType(),
            values
          );
      } catch (ValueEncodeException | ParameterCollectionException e) {
        throw new ParameterValueException(parameterName, values, e);
      }
      resolveParameter(query, parameterName, encodedValue);
    }
    return query.toString();
  }

  /**
   * The function replaces all the occurrences of the specified bind parameter with the resolved value.
   * There could be more than one occurrence of a particular bind param
   *
   * For eg. select * from table where col1 = :p1 and col2 = :p1. In this case all the valid occurrences
   * of the bind param p1 will be replaced by the resolved value.
   *
   * @param query (gets mutated)
   * @param parameter, the bind param
   * @param resolvedValue, the value to be resolved
   */
  private static void resolveParameter(StringBuilder query, String parameter, String resolvedValue) {
    final Pattern pattern = Pattern.compile(ParameterParser.getPatternStringFor(parameter));
    Matcher matcher = pattern.matcher(query);
    while (matcher.find()) {
      if (matcher.group(1) != null) {
        final MatchResult result = matcher.toMatchResult();
        query
          .replace(result.start(1) - ParameterParser.getParameterPrefixLength(), result.end(1), resolvedValue);
        matcher = pattern.matcher(query);
      }
    }
  }

  public static String resolve(SavedQuery savedQuery, MultivaluedMap<String, String> queryParameters)
    throws ParameterValueException, MissingParameterException {
    return new ParameterResolver(savedQuery, queryParameters).resolve();
  }
}
