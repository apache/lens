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

package org.apache.lens.server.query;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.query.*;
import org.apache.lens.server.api.error.LensException;


import org.glassfish.jersey.media.multipart.FormDataParam;
import org.testng.Assert;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Path("/queryapi/notifictaion")
public class TestQueryNotifictaionResource {

  @Getter
  private static int finishedCount = 0;
  @Getter
  private static int successfulCount = 0;
  @Getter
  private static int failedCount = 0;
  @Getter
  private static int cancelledCount = 0;
  @Getter
  private static int accessTokenCount = 0;
  @Getter
  private static int dataCount = 0;

  @POST
  @Path("finished")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public void prepareQuery(
    @FormDataParam("eventtype") String eventtype,
    @FormDataParam("eventtime") String eventtime,
    @FormDataParam("query") LensQuery query,
    @QueryParam("access_token") String accessToken,
    @QueryParam("data") String data) throws LensException {

    System.out.println("@@@@ Received Finished Event for queryid: " + query.getQueryHandleString()
      + " queryname:" + query.getQueryName() + " user:" + query.getSubmittedUser()
      + " status:" + query.getStatus() + " eventtype:" + eventtype + " access_token:" + accessToken
      + " data:" + data);

    finishedCount++;

    if (accessToken != null && accessToken.equals("ABC123")) {
      accessTokenCount++;
    }

    if (data != null && data.equals("x<>yz,\"abc")) {
      dataCount++;
    }

    Assert.assertTrue(query.getQueryName().toUpperCase().contains(query.getStatus().getStatus().name()),
      "query " + query.getQueryName() + " " + query.getStatus());

    if (query.getStatus().successful()) {
      successfulCount++;
    } else if (query.getStatus().failed()) {
      failedCount++;
    } else if (query.getStatus().cancelled()) {
      cancelledCount++;
    }
  }

  public static void clearState() {
    finishedCount = 0;
    successfulCount = 0;
    cancelledCount = 0;
    failedCount = 0;
    accessTokenCount = 0;
    dataCount = 0;
  }


}
