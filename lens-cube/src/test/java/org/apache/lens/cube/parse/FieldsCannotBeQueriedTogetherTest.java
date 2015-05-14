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

import static org.apache.lens.cube.parse.CubeTestSetup.TWO_DAYS_RANGE;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lens.cube.error.ConflictingFields;
import org.apache.lens.cube.error.FieldsCannotBeQueriedTogetherException;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FieldsCannotBeQueriedTogetherTest extends TestQueryRewrite {

  private Configuration conf = new Configuration();

  @BeforeClass
  public void beforeClassFieldsCannotBeQueriedTogetherTest() {
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
  }

  @Test
  public void testQueryWithDimensionAndMeasure() throws SemanticException, ParseException, LensException {

    /* If all the queried dimensions are present in a derived cube, and one of the queried measure is not present in
    the same derived cube, then query shall be disallowed.

    dim2 and msr1 are not present in the same derived cube, hence query shall be disallowed with appropriate
    exception. */

    testFieldsCannotBeQueriedTogetherError("select dim2, SUM(msr1) from basecube where " + TWO_DAYS_RANGE,
        Arrays.asList("dim2", "msr1"));
  }

  @Test
  public void testQueryWithChainReferencedDimensionAttributeAndMeasure() throws SemanticException, ParseException,
      LensException {

    /* In this query a dimension attribute referenced through join chain name is used in select. If the
    source column for such a dimension attribute and the  queried measure are not present in the same derived cube,
    then query shall be disallowed.

    cityState.name is a dimension attribute used in select statement and referenced through join chain name citystate.
    It is queryable through chain source column cityid. cityid and msr1 are not present in the same derived cube, hence
    query shall be disallowed with appropriate exception. */

    testFieldsCannotBeQueriedTogetherError("select citystate.name, SUM(msr1) from basecube where " + TWO_DAYS_RANGE,
        Arrays.asList("citystate.name", "msr1"));
  }

  @Test
  public void testQueryWithMeasureAndChainReferencedDimAttributeInFilter() throws SemanticException, ParseException,
      LensException {

    /* In this query a dimension attribute referenced through join chain name is used in filter. If the
    source column for such a dimension attribute and the  queried measure are not present in the same derived cube,
    then query shall be disallowed.

    cityState.name is a dimension attribute used in where clause(filter) and referenced through join chain name. It is
    queryable through chain source column cityid. cityid and msr1 are not present in the same derived cube, hence query
    shall be disallowed with appropriate exception. */

    testFieldsCannotBeQueriedTogetherError("select SUM(msr1) from basecube where cityState.name = 'foo' and "
            + TWO_DAYS_RANGE, Arrays.asList("citystate.name", "msr1"));
  }

  @Test
  public void testQueryWithOnlyMeasure() throws ParseException, SemanticException, LensException {

    /* A query which contains only measure should pass, if the measure is present in some derived cube.
    msr1 is present in one of the derived cubes, hence query shall pass without any exception. */

    rewrite("select SUM(msr1) from basecube where " + TWO_DAYS_RANGE, conf);
  }

  @Test
  public void testQueryWithMeasureAndChainReferencedDimAttributeInCaseStatement() throws ParseException,
      SemanticException, LensException {

    /* In this query a dimension attribute referenced through join chain name is used in case statement.
    A query which contains such a dim attribute and a measure is allowed even if the source column of the used dim
    attribute and the queried measure are not present in the same derived cube.

    cityState.name is a dimension attribute used in where clause(filter) and referenced through join chain name
    cityState. It is queryable through source column basecube.cityid. basecube.cityid and msr1 are not present in the
    same derived cube. However since cityState.name is only present in the case statement, the query is allowed. */

    rewrite("select SUM(CASE WHEN cityState.name ='foo' THEN msr1 END) from basecube where " + TWO_DAYS_RANGE, conf);
  }

  @Test
  public void testQueryWithDimAttributesNotInSameDerviedCube() throws ParseException, SemanticException, LensException {

    /* dim2 and countryid are not present in the same derived cube, hence query should be disallowed */

    testFieldsCannotBeQueriedTogetherError("select dim2, countryid, SUM(msr2) from basecube where " + TWO_DAYS_RANGE,
        Arrays.asList("countryid", "dim2"));
  }

  @Test
  public void testQueryWithMeasureNotInAnyDerviedCube() throws ParseException, SemanticException, LensException {

    /* newmeasure is not present in any derived cube, hence the query should be disallowed. */

    testFieldsCannotBeQueriedTogetherError("select newmeasure from basecube where "
        + TWO_DAYS_RANGE, Arrays.asList("newmeasure"));
  }

  @Test
  public void testQueryWithReferencedDimAttributeAndMeasure() throws SemanticException, ParseException,
      LensException {

    /* In this query a referenced dimension attribute is used in select statement. If the source column for such a
    dimension attribute and the  queried measure are not present in the same derived cube, then query shall be
    disallowed.

    cityStateCapital is a referenced dimension attribute used in select statement. It is queryable through chain source
    column cityid. cityid and msr1 are not present in the same derived cube, hence query shall be disallowed with
    appropriate exception. */

    testFieldsCannotBeQueriedTogetherError(
        "select citystatecapital, SUM(msr1) from basecube where " + TWO_DAYS_RANGE,
        Arrays.asList("citystatecapital", "msr1"));
  }

  private void testFieldsCannotBeQueriedTogetherError(final String testQuery, final List<String> conflictingFields)
    throws ParseException, SemanticException, LensException {

    try {

      rewrite(testQuery, conf);
      fail("Expected FieldsCannotBeQueriedTogetherException but it didn't come");
    } catch(FieldsCannotBeQueriedTogetherException actualException) {

      SortedSet<String> expectedFields = new TreeSet<String>(conflictingFields);

      FieldsCannotBeQueriedTogetherException expectedException =
          new FieldsCannotBeQueriedTogetherException(new ConflictingFields(expectedFields));
      assertEquals(actualException, expectedException);
    }
  }
}
