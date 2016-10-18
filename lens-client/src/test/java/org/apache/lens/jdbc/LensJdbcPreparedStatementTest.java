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
package org.apache.lens.jdbc;

import static org.apache.lens.client.jdbc.LensJdbcPreparedStatement.tokenize;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;


public class LensJdbcPreparedStatementTest {

  @Test
  public void testTokenize() {
    List<String> out = tokenize("select * from tab limit ?");
    Assert.assertArrayEquals(new String[]{"select * from tab limit ", ""}, out.toArray());

    out = tokenize("select * from tab where a = ? and b=c");
    Assert.assertArrayEquals(new String[]{"select * from tab where a = ", " and b=c"}, out.toArray());

    out = tokenize("select \"? something \" from tab");
    Assert.assertArrayEquals(new String[]{"select \"? something \" from tab"}, out.toArray());

    out = tokenize("select \'?\' from tab where a=?");
    Assert.assertArrayEquals(new String[]{"select \'?\' from tab where a=", ""}, out.toArray());

    out = tokenize("select \'indian\\\'s\' from tab where a=b");
    Assert.assertArrayEquals(new String[]{"select 'indian\\'s' from tab where a=b"}, out.toArray());


  }

}
