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
package org.apache.lens.server.metastore;

import static org.apache.lens.api.APIResult.*;

import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.*;
import org.apache.lens.api.DateTime;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.metastore.*;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metastore.CubeMetastoreService;
import org.apache.lens.server.api.metastore.DatabaseResourcesService;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

/**
 * Database resource api
 * <p> </p>
 * This provides api for all things database.
 */
@Path("database")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
@Slf4j
public class DatabaseResource {

  public static DatabaseResourcesService getSvc() {
    return LensServices.get().getService(DatabaseResourcesService.NAME);
  }

  private static LensException processLensException(LensException exc) {
    if (exc != null) {
      exc.buildLensErrorTO(LensServices.get().getErrorCollection());
    }
    return exc;
  }

  /**
   * API to know if database service is up and running
   *
   * @return Simple text saying it up
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Database is up";
  }


  /**
   * Create a new database
   *
   * @param dbName           The db name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if create has failed
   */
  @POST
  @Path("databases")
  public APIResult refreshDatabaseResources(String dbName) {
    log.info("Refresh database : ", dbName);
    try {
      getSvc().refreshDatabaseResources(dbName);
      return success();
    } catch (LensException e) {
      log.error("Error creating database {}", dbName, e);
      return failure(processLensException(e));
    }
  }

}