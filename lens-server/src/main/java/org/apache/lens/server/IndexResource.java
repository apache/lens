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

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;

/**
 * The Class IndexResource.
 */
@Path("/")
public class IndexResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Lens server is up!";
  }

  @GET
  @Path("/index")
  @Produces(MediaType.TEXT_PLAIN)
  public String getIndexMessage() {
    return "Hello World! from lens";
  }

  @GET
  @Path("/admin/stack")
  @Produces(MediaType.TEXT_PLAIN)
  public String getThreadDump() {
    ThreadGroup topThreadGroup = Thread.currentThread().getThreadGroup();

    while (topThreadGroup.getParent() != null) {
      topThreadGroup = topThreadGroup.getParent();
    }
    Thread[] threads = new Thread[topThreadGroup.activeCount()];

    int nr = topThreadGroup.enumerate(threads);
    StringBuilder builder = new StringBuilder();
    builder.append("Total number of threads:").append(nr).append("\n");
    for (int i = 0; i < nr; i++) {
      builder.append(threads[i].getName()).append("\n\tState: ").append(threads[i].getState()).append("\n");
      String stackTrace = StringUtils.join(threads[i].getStackTrace(), "\n");
      builder.append(stackTrace);
      builder.append("\n----------------------\n\n");
    }
    return builder.toString();
  }

  @GET
  @Path("/admin/status")
  @Produces(MediaType.TEXT_PLAIN)
  public String getStatus() {
    return LensServices.get().getServiceState().toString();
  }

}
