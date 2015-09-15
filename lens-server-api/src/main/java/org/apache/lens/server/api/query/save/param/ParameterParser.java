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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.api.query.save.Parameter;
import org.apache.lens.api.query.save.ParameterParserResponse;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Responsible for parsing the query (any grammar) and extracting parameters out of it
 */
public class ParameterParser {

  private static final String PARAMETER_INDICATOR_PREFIX = ":";
  private static final String SINGLE_QUOTED_LITERAL_PATTERN = "'[^']*'";
  private static final String DOUBLE_QUOTED_LITERAL_PATTERN = "\"[^\"]*\"";
  private static final Pattern PARAM_PATTERN = Pattern.compile(getPatternStringFor("[a-zA-Z][a-zA-Z1-9_]*"));

  /**
   *
   * @param baseParameterPattern, regex denoting the parameters to match
   * @return a regex pattern that captures the valid occurrences in the 1st group while
   * ignoring occurrences inside literals (single/double quoted)
   *
   * To search for a parameter named p1, we will have to search for ':p1' that
   * occurs natively in the query ignoring the ones that are single/double quoted.
   * For eg. In the string - "Hai this is valid param :p1 and 'this is invalid param :p1'", the
   * first occurrence is valid and the second is invalid as it falls under matching single quotes
   *
   * invalid_pattern_1|invalid_pattern_2|PARAMETER_INDICATOR_PREFIX(baseParameterPattern)
   *
   * For a baseParameterPattern - param1, the resulting pattern would be
   * '[^']*'|"[^"]*"|:(param1)
   */
  public static String getPatternStringFor(String baseParameterPattern) {
    final StringBuilder patternBuilder = new StringBuilder();
    Joiner.on("|").appendTo(patternBuilder, getInvalidPatterns());
    patternBuilder.append("|");
    patternBuilder.append(PARAMETER_INDICATOR_PREFIX);
    patternBuilder.append("(");
    patternBuilder.append(baseParameterPattern);
    patternBuilder.append(")");
    return patternBuilder.toString();
  }

  /**
   *
   * @return a list of invalid regexp patterns that could possibly contain the parameter.
   */
  private static List<String> getInvalidPatterns() {
    final List<String> invalidPatternList = Lists.newArrayList();
    invalidPatternList.add(SINGLE_QUOTED_LITERAL_PATTERN);
    invalidPatternList.add(DOUBLE_QUOTED_LITERAL_PATTERN);
    return invalidPatternList;
  }

  /**
   * Returns the length of the parameter prefix configured.
   *
   * @return length of the parameter prefix
   */
  public static int getParameterPrefixLength() {
    return PARAMETER_INDICATOR_PREFIX.length();
  }

  private final String query;

  public ParameterParser(String query) {
    this.query = query;
  }

  /**
   * Returns set of parameter names found in the query
   *
   * @return set of parameter names
   */
  public Set<String> extractParameterNames() {
    final ParameterParserResponse parameterParserResponse = extractParameters();
    return Sets.newHashSet(
      Collections2.transform(parameterParserResponse.getParameters(), new Function<Parameter, String>() {
        @Override
        public String apply(Parameter parameter) {
          return parameter.getName();
        }
      }));
  }

  /**
   * Returns parameter parser response for the query
   *
   * @return ParameterParserResponse object
   */
  public ParameterParserResponse extractParameters() {
    final Matcher m = PARAM_PATTERN.matcher(query);
    final Set<Parameter> allParams = Sets.newHashSet();
    while (m.find()) {
      String validMatch = m.group(1);
      if (validMatch != null) {
        allParams.add(new Parameter(validMatch));
      }
    }
    return new ParameterParserResponse(
      true,
      null,
      ImmutableSet.copyOf(allParams)
    );
  }
}
