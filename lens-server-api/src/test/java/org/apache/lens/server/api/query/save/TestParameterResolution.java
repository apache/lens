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

import static org.apache.lens.api.query.save.ParameterCollectionType.MULTIPLE;
import static org.apache.lens.api.query.save.ParameterCollectionType.SINGLE;
import static org.apache.lens.api.query.save.ParameterDataType.*;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.lens.api.query.save.Parameter;
import org.apache.lens.api.query.save.SavedQuery;
import org.apache.lens.server.api.query.save.exception.MissingParameterException;
import org.apache.lens.server.api.query.save.exception.ParameterValueException;
import org.apache.lens.server.api.query.save.param.ParameterResolver;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class TestParameterResolution {

  private static final String QUERY_STRING = "select * from table where "
    + "col = :param1 and "
    + "col = :param1 and "
    + "col = 'a :param1 inside single quotes' and "
    + "col = \"a :param1 inside double quotes\" and "
    + "col in :param2 and "
    + "col = :param3 and "
    + "col in :param4 and "
    + "col = :param5 and "
    + "col = :param7 and "
    + "col in :param8 and "
    + "col in :param6";

  private static final SavedQuery QUERY = new SavedQuery(
    1,
    "query_name",
    "description",
    QUERY_STRING
    ,
    Lists.newArrayList(
      new Parameter(
        "param1", "Param1", new String[]{"val"}, STRING, SINGLE
      ),
      new Parameter(
        "param2", "Param2", new String[]{"val1", "val2"}, STRING, MULTIPLE
      ),
      new Parameter(
        "param3", "Param3", new String[]{"1"}, NUMBER, SINGLE
      ),
      new Parameter(
        "param4", "Param4", new String[]{"1", "2"}, NUMBER, MULTIPLE
      ),
      new Parameter(
        "param7", "Param7", new String[]{"1"}, DECIMAL, SINGLE
      ),
      new Parameter(
        "param8", "Param8", new String[]{"1.2", "2.1"}, DECIMAL, MULTIPLE
      ),
      new Parameter(
        "param5", "Param5", new String[]{"true"}, BOOLEAN, SINGLE
      ),
      new Parameter(
        "param6", "Param6", null, BOOLEAN, MULTIPLE
      )
    )
  );


  @Test
  public void testWithProperValues() throws ParameterValueException, MissingParameterException {
    MultivaluedMap<String, String> parameterValues = new MultivaluedHashMap<>();
    parameterValues.put("param6", Lists.newArrayList("true", "false"));
    String resolvedQuery = ParameterResolver.resolve(QUERY, parameterValues);
    Assert.assertEquals(
      "select * from table where col = 'val' and col = 'val' and col = 'a :param1 inside single quotes' "
        + "and col = \"a :param1 inside double quotes\" and col in ('val1','val2') and col = 1 and col in (1,2) "
        + "and col = true and col = 1.0 and col in (1.2,2.1) and col in (true,false)",
      resolvedQuery,
      "Query resolution did not happen correctly"
    );
  }

  @Test
  public void testWithInvalidValueForNumber() throws MissingParameterException {
    MultivaluedMap<String, String> parameterValues = new MultivaluedHashMap<>();
    parameterValues.put("param3", Lists.newArrayList("number"));
    parameterValues.put("param6", Lists.newArrayList("true", "false"));
    try {
      final String resolvedQuery = ParameterResolver.resolve(QUERY, parameterValues);
      Assert.fail("The query seem to have resolved with invalid datatype : " + resolvedQuery);
    } catch (ParameterValueException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testWithInvalidValueForBoolean() throws MissingParameterException {
    MultivaluedMap<String, String> parameterValues = new MultivaluedHashMap<>();
    parameterValues.put("param5", Lists.newArrayList("boolean"));
    parameterValues.put("param6", Lists.newArrayList("true", "false"));
    try {
      final String resolvedQuery = ParameterResolver.resolve(QUERY, parameterValues);
      Assert.fail("The query seem to have resolved with invalid datatype : " + resolvedQuery);
    } catch (ParameterValueException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testWithInvalidValueForDecimal() throws MissingParameterException {
    MultivaluedMap<String, String> parameterValues = new MultivaluedHashMap<>();
    parameterValues.put("param7", Lists.newArrayList("decimal"));
    parameterValues.put("param6", Lists.newArrayList("true", "false"));
    try {
      final String resolvedQuery = ParameterResolver.resolve(QUERY, parameterValues);
      Assert.fail("The query seem to have resolved with invalid datatype : " + resolvedQuery);
    } catch (ParameterValueException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testWithIncorrectCollectionTypeForSingle() throws MissingParameterException {
    MultivaluedMap<String, String> parameterValues = new MultivaluedHashMap<>();
    parameterValues.put("param5", Lists.newArrayList("true", "false"));
    parameterValues.put("param6", Lists.newArrayList("true"));
    try {
      final String resolvedQuery = ParameterResolver.resolve(QUERY, parameterValues);
      Assert.fail("The query seem to have resolved with invalid collection type : " + resolvedQuery);
    } catch (ParameterValueException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testWithIncorrectCollectionTypeForMultiple() throws MissingParameterException {
    MultivaluedMap<String, String> parameterValues = new MultivaluedHashMap<>();
    parameterValues.put("param6", Lists.<String>newArrayList());
    try {
      final String resolvedQuery = ParameterResolver.resolve(QUERY, parameterValues);
      Assert.fail("The query seem to have resolved with invalid collection type : " + resolvedQuery);
    } catch (ParameterValueException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testWithMissingParameters() throws ParameterValueException{
    try {
      String resolvedQuery = ParameterResolver.resolve(QUERY, new MultivaluedHashMap<String, String>());
      Assert.fail("The query seem to have resolved with missing parameters : " + resolvedQuery);
    } catch (MissingParameterException e) {
      Assert.assertTrue(true);
    }
  }
}
