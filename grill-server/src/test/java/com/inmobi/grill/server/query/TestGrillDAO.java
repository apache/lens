package com.inmobi.grill.server.query;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.inmobi.grill.server.api.query.FinishedGrillQuery;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGrillDAO {

  @Test
  public void testDAO() throws Exception {
    Configuration conf = new Configuration();
    GrillServerDAO dao = new GrillServerDAO();
    dao.init(conf);
    dao.createFinishedQueriesTable();
    FinishedGrillQuery query = new FinishedGrillQuery();
    query.setHandle("adas");
    query.setSubmitter("adasdas");
    query.setUserQuery("asdsadasdasdsa");
    dao.insertFinishedQuery(query);
    Assert.assertEquals(query,
        dao.getQuery(query.getHandle()));
    dao.dropFinishedQueriesTable();
  }
}
