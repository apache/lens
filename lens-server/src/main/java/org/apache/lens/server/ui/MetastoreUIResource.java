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

import java.util.List;
import java.util.UUID;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.*;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metastore.CubeMetastoreService;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import lombok.extern.slf4j.Slf4j;

/**
 * metastore UI resource api
 * <p></p>
 * This provides api for all things metastore UI.
 */

@Path("metastoreapi")
@Slf4j
public class MetastoreUIResource {

  public CubeMetastoreService getSvc() {
    return LensServices.get().getService(CubeMetastoreService.NAME);
  }

  private void checkSessionHandle(LensSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  /**
   * API to know if metastore service is up and running
   *
   * @return Simple text saying it up
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Metastore is up";
  }

  /**
   * Get all Cube names, Dimension Table names and Storage names
   *
   * @param publicId The publicId for the session in which user is working
   * @return JSON string consisting of different table names and types
   * @throws WebApplicationException  wraps LensException
   */
  @GET
  @Path("tables")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public String getAllTables(@QueryParam("publicId") UUID publicId) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    JSONArray tableList = new JSONArray();

    List<String> cubes;
    try {
      cubes = getSvc().getAllCubeNames(sessionHandle);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }

    if (cubes != null) {
      for (String cube : cubes) {
        try {
          tableList.put(new JSONObject().put("name", cube).put("type", "cube"));
        } catch (JSONException j) {
          log.error("Error while parsing json", j);
        }
      }
    }

    List<String> dimTables;
    try {
      dimTables = getSvc().getAllDimensionNames(sessionHandle);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }

    if (dimTables != null) {
      for (String dimTable : dimTables) {
        try {
          tableList.put(new JSONObject().put("name", dimTable).put("type", "dimtable"));
        } catch (JSONException j) {
          log.error("Error while parsing json", j);
        }
      }
    }

    try {
      List<String> nativeTables = getSvc().getAllNativeTableNames(sessionHandle, "all", null);
      for (String nativeTable : nativeTables) {
        tableList.put(new JSONObject().put("name", nativeTable).put("type", "native"));
      }
    } catch (Exception e) {
      log.error("Error while getting all native tables", e);
    }

    return tableList.toString();
  }


  /**
   * Gets metadata of a cube or dimension
   *
   * @param publicId The publicId for the session in which user is working
   * @param name     name of cube or dimension to be described
   * @return JSON string consisting of different dimension and measure names and types
   * @throws WebApplicationException wraps LensException
   */
  @GET
  @Path("tables/{name}")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public String getDescription(@QueryParam("publicId") UUID publicId, @QueryParam("type") String type,
    @PathParam("name") String name) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    JSONArray attribList = new JSONArray();
    if (type.equals("cube")) {
      XBaseCube cube;
      try {
        cube = (XBaseCube) getSvc().getCube(sessionHandle, name);
      } catch (LensException e) {
        throw new WebApplicationException(e);
      }

      if (cube.getMeasures() != null) {
        for (XMeasure measure : cube.getMeasures().getMeasure()) {
          try {
            attribList.put(new JSONObject().put("name", measure.getName()).put("type", measure.getType()));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        }
      }
      if (cube.getDimAttributes() != null) {
        for (XDimAttribute dim : cube.getDimAttributes().getDimAttribute()) {
          try {
            attribList.put(new JSONObject().put("name", dim.getName()).put("type", dim.getType()));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        }
      }
    } else if (type.equals("dimtable")) {
      XDimension table;
      try {
        table = getSvc().getDimension(sessionHandle, name);
      } catch (LensException e) {
        throw new WebApplicationException(e);
      }
      if (table.getAttributes() != null) {
        for (XDimAttribute col : table.getAttributes().getDimAttribute()) {
          try {
            attribList.put(new JSONObject().put("name", col.getName()).put("type", col.getType()));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        }
      }

      if (table.getExpressions() != null) {
        for (XExprColumn expr : table.getExpressions().getExpression()) {
          try {
            attribList.put(new JSONObject().put("name", expr.getName()).put("type", "expression")
              .put("expression", expr.getExprSpec()));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        }
      }
    }
    return attribList.toString();
  }

  /**
   * Get all Table and column names and types which contain the search word
   *
   * @param publicId The publicId for the session in which user is working
   * @param keyword  keyword to be searched
   * @return JSON string consisting of different table and column names and types
   */
  @GET
  @Path("searchablefields")
  @Produces({MediaType.TEXT_HTML, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public String getFilterResults(@QueryParam("publicId") UUID publicId, @QueryParam("keyword") String keyword) {
    LensSessionHandle sessionHandle = SessionUIResource.getOpenSession(publicId);
    checkSessionHandle(sessionHandle);
    JSONArray tableList = null;
    JSONArray searchResultList = new JSONArray();
    try {

      tableList = new JSONArray(getAllTables(publicId));
    } catch (JSONException j) {
      log.error("Error while parsing json", j);
    }
    for (int item = 0; item < tableList.length(); item++) {
      String name = null, type = null;
      try {
        name = tableList.getJSONObject(item).getString("name");
        type = tableList.getJSONObject(item).getString("type");
      } catch (JSONException j) {
        log.error("Error while parsing json", j);
      }
      if (type.equals("cube")) {
        JSONArray cubeAttribList = null;
        JSONArray cubeSearchResultList = new JSONArray();
        try {

          cubeAttribList = new JSONArray(getDescription(publicId, "cube", name));
        } catch (JSONException j) {
          log.error("Error while parsing json", j);
        }
        for (int col = 0; col < cubeAttribList.length(); col++) {
          String colname = null, coltype = null;
          try {
            colname = cubeAttribList.getJSONObject(col).getString("name");
            coltype = cubeAttribList.getJSONObject(col).getString("type");

          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
          if (colname.contains(keyword)) {
            try {
              cubeSearchResultList.put(new JSONObject().put("name", colname).put("type", coltype));
            } catch (JSONException j) {
              log.error("Error while parsing json", j);
            }
          }
        }
        if (cubeSearchResultList.length() > 0) {
          try {
            searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns",
              cubeSearchResultList));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        } else if (name.contains(keyword)) {
          try {
            searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns",
              cubeSearchResultList));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        }
      } else if (type.equals("dimtable")) {
        JSONArray dimAttribList = null;
        JSONArray dimSearchResultList = new JSONArray();
        try {

          dimAttribList = new JSONArray(getDescription(publicId, "dimtable", name));
        } catch (JSONException j) {
          log.error("Error while parsing json", j);
        }
        for (int col = 0; col < dimAttribList.length(); col++) {
          String colname = null, coltype = null;
          try {
            colname = dimAttribList.getJSONObject(col).getString("name");
            coltype = dimAttribList.getJSONObject(col).getString("type");

          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
          if (colname.contains(keyword)) {
            try {
              dimSearchResultList.put(new JSONObject().put("name", colname).put("type", coltype));
            } catch (JSONException j) {
              log.error("Error while parsing json", j);
            }
          }
        }
        if (dimSearchResultList.length() > 0) {
          try {
            searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns",
              dimSearchResultList));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        } else if (name.contains(keyword)) {
          try {
            searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns",
              dimSearchResultList));
          } catch (JSONException j) {
            log.error("Error while parsing json", j);
          }
        }
      }
    }
    return searchResultList.toString();
  }
}

