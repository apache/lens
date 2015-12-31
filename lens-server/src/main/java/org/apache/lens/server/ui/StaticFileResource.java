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
package org.apache.lens.server.ui;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;

/**
 * The Class StaticFileResource.
 */
@Path("/")
public class StaticFileResource {

  // Cache for file content, bound by both size and time
  /** The Constant CONTENT_CACHE. */
  private static final LoadingCache<String, String> CONTENT_CACHE = CacheBuilder.newBuilder().maximumSize(100)
    .expireAfterAccess(10, TimeUnit.MINUTES).build(new CacheLoader<String, String>() {
      String baseDir = null;

      @Override
      public String load(String filePath) throws Exception {
        if (baseDir == null) {
          baseDir = LensServices.get().getHiveConf()
            .get(LensConfConstants.SERVER_UI_STATIC_DIR, LensConfConstants.DEFAULT_SERVER_UI_STATIC_DIR);
        }
        return loadFile(baseDir, filePath);
      }
    });

  /**
   * Load file.
   *
   * @param baseDir  the base dir
   * @param filePath the file path
   * @return the string
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static String loadFile(String baseDir, String filePath) throws IOException {
    return Files.toString(new File(baseDir, filePath), Charset.forName("UTF-8"));
  }

  /**
   * Gets the static resource.
   *
   * @param filePath the file path
   * @return the static resource
   */
  @GET
  @Path("/{filePath:.*}")
  public Response getStaticResource(@PathParam("filePath") String filePath) {
    try {
      HiveConf conf = LensServices.get().getHiveConf();
      if (conf.getBoolean(LensConfConstants.SERVER_UI_ENABLE_CACHING,
        LensConfConstants.DEFAULT_SERVER_UI_ENABLE_CACHING)) {
        return Response.ok(CONTENT_CACHE.get(filePath), getMediaType(filePath)).build();
      } else {
        // This is for dev mode
        String baseDir = conf.get(LensConfConstants.SERVER_UI_STATIC_DIR,
          LensConfConstants.DEFAULT_SERVER_UI_STATIC_DIR);
        return Response.ok(loadFile(baseDir, filePath), getMediaType(filePath)).build();
      }
    } catch (Exception e) {
      if (e.getCause() instanceof FileNotFoundException || e instanceof FileNotFoundException) {
        throw new NotFoundException("Not Found: " + filePath);
      }
      throw new WebApplicationException("Server error: " + e.getCause(), e);
    }
  }

  /**
   * Gets the media type.
   *
   * @param filePath the file path
   * @return the media type
   */
  private String getMediaType(String filePath) {
    if (filePath == null) {
      return null;
    }

    if (filePath.endsWith(".html")) {
      return MediaType.TEXT_HTML;
    } else if (filePath.endsWith(".js")) {
      return "application/javascript";
    } else if (filePath.endsWith(".css")) {
      return "text/css";
    }
    return null;
  }
}
