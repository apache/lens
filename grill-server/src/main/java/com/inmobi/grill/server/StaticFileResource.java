package com.inmobi.grill.server;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

@Path("/")
public class StaticFileResource {
  public static final Log LOG = LogFactory.getLog(StaticFileResource.class);

  // Cache for file content, bound by both size and time
  private static final LoadingCache<String, String> contentCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build(new CacheLoader<String, String>() {
      String baseDir = null;
      @Override
      public String load(String filePath) throws Exception {
        if (baseDir == null) {
          baseDir = GrillServices.get().getHiveConf().get(GrillConfConstants.GRILL_SERVER_UI_STATIC_DIR,
            GrillConfConstants.DEFAULT_GRILL_SERVER_UI_STATIC_DIR);
        }
        return loadFile(baseDir, filePath);
      }
    });

  private static String loadFile(String baseDir, String filePath) throws IOException {
    return Files.toString(new File(baseDir, filePath), Charset.forName("UTF-8"));
  }

  @GET
  @Path("/{filePath:.*}")
  public Response getStaticResource(@PathParam("filePath") String filePath) {
    try {
      HiveConf conf = GrillServices.get().getHiveConf();
      if (conf.getBoolean(GrillConfConstants.GRILL_SERVER_UI_ENABLE_CACHING,
        GrillConfConstants.DEFAULT_GRILL_SERVER_UI_ENABLE_CACHING)) {
        return Response.ok(contentCache.get(filePath), getMediaType(filePath)).build();
      } else {
        // This is for dev mode
        String baseDir = conf.get(GrillConfConstants.GRILL_SERVER_UI_STATIC_DIR,
          GrillConfConstants.DEFAULT_GRILL_SERVER_UI_STATIC_DIR);
        return Response.ok(loadFile(baseDir, filePath), getMediaType(filePath)).build();
      }
    } catch (Exception e) {
      if (e.getCause() instanceof FileNotFoundException
        || e instanceof FileNotFoundException
        ) {
        throw new NotFoundException("Not Found: " + filePath);
      }
      throw new WebApplicationException("Server error: " + e.getCause(), e);
    }
  }

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
