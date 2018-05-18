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
package org.apache.lens.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.lens.server.auth.Authenticate;
import org.apache.lens.server.util.UtilityMethods;

/**
 * The logs resource
 */
@Authenticate
@Path("/logs")
public class LogResource {

  private static final String LOG_FILE_EXTN = ".log";

  /**
   * Tells whether log resource if up or not
   *
   * @return message
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Logs resource is up!";
  }

  /**
   * Get logs corresponding to logSegregrationStr
   *
   * @param logSegregrationStr log segregation string - can be request id for all requests; query handle for queries
   *
   * @return streaming log
   */
  @GET
  @Path("/{logSegregrationStr}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM})
  public Response getLogs(@PathParam("logSegregrationStr") String logSegregrationStr) {
    String logFileName = System.getProperty("lens.log.dir") + "/" + logSegregrationStr + LOG_FILE_EXTN;
    final File logFile = new File(logFileName);
    if (!logFile.exists()) {
      return Response.status(Status.NOT_FOUND).entity("Log file for '" + logSegregrationStr + "' not found").build();
    }

    StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(OutputStream os) throws IOException {
        FileInputStream fin = new FileInputStream(logFile);
        try {
          UtilityMethods.pipe(fin, os);
        } finally {
          fin.close();
        }
      }
    };
    return Response.ok(stream).type(MediaType.APPLICATION_OCTET_STREAM).build();
  }

}
