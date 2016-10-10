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
package org.apache.lens.cube.parse;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.lens.server.api.error.LensException;

/**
 * DataCompletenessChecker is for identifying the completeness of data in a fact for the given set of measures, start
 * and end date. A fact will have a dataCompletenessTag, multiple facts can have the same dataCompletenessTag.
 * Similarly, measures will have a dataCompletenessTag, multiple measures can have the same dataCompletenessTag.
 * The api will take the dataCompletenessTag for the facts and measures and compute the completeness based on these
 * tags. The utility of having tags is that the similar kind of measures or facts which will have the same level of
 * completeness can use the same tag, thus we avoid the redundant completeness computation for similar measures
 * and facts.
 * The implementations of the interface can truncate the start and end date.
 */
public interface DataCompletenessChecker {

  /**
   * Get completeness of the set of measures in a fact based on the dataCompletenessTag for the given starttime and
   * endtime.
   *
   * @param factTag This is the dataCompletenessTag for a fact. The tag can be specified by setting the property
   *                named dataCompletenessTag for the fact. Mutltiple facts can have the same dataCompletenessTag.
   * @param start Start time of the query (Inclusive).
   * @param end End time of the query (Exclusive).
   * @param measureTag List of distinct tag of the measures in the query. Multiple measures can have the same
   *                   dataCompletenessTag.
   * @return map; key is the name of the dataCompletenessTag which refers to one or more measures. Value is the map
   * of date and %completeness.
   */
  Map<String, Map<Date, Float>> getCompleteness(String factTag, Date start, Date end, Set<String> measureTag)
    throws LensException;

}
