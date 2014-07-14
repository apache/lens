package com.inmobi.grill.server;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Path("/static")
public class StaticFileResource {
  public static final Log LOG = LogFactory.getLog(StaticFileResource.class);
  public static final String STATIC_BASE_DIR =
    StringUtils.join(new String[]{"webapp", "grill-server", "static"}, File.separator);

  // Cache for file content, bound by both size and time
  private LoadingCache<String, String> contentCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build(new CacheLoader<String, String>() {
      @Override
      public String load(String filePath) throws Exception {
        return Files.toString(new File(STATIC_BASE_DIR, filePath), Charset.forName("UTF-8"));
      }
    });

  @GET
  @Path("/{filePath:.*}")
  public Response getStaticResource(@PathParam("filePath") String filePath) {
    try {
      return Response.ok(contentCache.get(filePath), getMediaType(filePath)).build();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof FileNotFoundException) {
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
