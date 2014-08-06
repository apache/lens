package com.inmobi.grill.server.metastore;

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

/**
 * Created by vikash pandey on 25/7/14.
 */

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.metastore.CubeMetastoreService;
import com.inmobi.grill.server.session.SessionUIResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;
import java.util.*;

/**
 * metastore UI resource api
 *
 * This provides api for all things metastore UI.
 */

@Path("metastoreapi")
public class MetastoreUIResource {

    public static final Log LOG = LogFactory.getLog(MetastoreUIResource.class);
    public CubeMetastoreService getSvc() { return (CubeMetastoreService)GrillServices.get().getService("metastore");}

    private void checkSessionHandle(GrillSessionHandle sessionHandle) {
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

    /*
     * Checks if any Measure Name in a cube contains a keyword
     *
    private boolean measureNameMatched(XCube cube, String keyword)
    {
        if(cube.getMeasures()!=null) {
            for (XMeasure measure : cube.getMeasures().getMeasures()) {
                if (measure.getName().contains(keyword)) return true;
            }
        }
        return false;
    }


    /*
    * Checks if any Dimension Attribute Name in a cube contains a keyword
    *
    private boolean dimAttrNameMatched(XCube cube, String keyword)
    {
        if(cube.getDimAttributes()!=null) {
            for (XDimAttribute dim : cube.getDimAttributes().getDimAttributes()) {
                if (dim.getName().contains(keyword)) return true;
            }
        }
        return false;
    }*/

    /**
     * Get all Cube names, Dimension Table names and Storage names
     *
     * @param publicId The publicId for the session in which user is working
     *
     * @return JSON string consisting of different table names and types
     *
     * @throws GrillException, JSONException
     */
    @GET @Path("tables")
    @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public String getAllTables(@QueryParam("publicId") UUID publicId)
    {
        GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        JSONArray tableList = new JSONArray();

        List<String> cubes;
        try{
            cubes = getSvc().getAllCubeNames(sessionHandle);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }

        if(cubes != null)
            for(String cube : cubes)
            {
                try {
                    tableList.put(new JSONObject().put("name", cube).put("type", "cube"));
                }
                catch(JSONException j){
                    LOG.error(j);
                }
            }

        List<String> dimTables;
        try{
            dimTables = getSvc().getAllDimTableNames(sessionHandle);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }

        if(dimTables != null)
            for(String dimTable : dimTables)
            {
                try {
                    tableList.put(new JSONObject().put("name", dimTable).put("type", "dimtable"));
                }
                catch(JSONException j){
                    LOG.error(j);
                }
            }
        /*List<String> storageTables;
        try{
            storageTables = getSvc().getAllStorageNames(sessionHandle);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }
        for(String storageTable : storageTables)
        {
            try {
                tableList.put(new JSONObject().put("name", storageTable).put("type", "storage"));
            }
            catch(JSONException j){
                LOG.error(j);
            }
        }*/
        return tableList.toString();
    }


    /**
     * Get all dimension and measure names and types of a cube
     *
     * @param publicId The publicId for the session in which user is working
     *
     * @param cubeName name of cube to be described
     *
     * @return JSON string consisting of different dimension and measure names and types
     *
     * @throws GrillException, JSONException
     */
    @GET @Path("tables/{cubeName}/cube")
    @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public String getCubeDescription(@QueryParam("publicId") UUID publicId, @PathParam("cubeName") String cubeName) {
        GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        JSONArray cubeAttribList = new JSONArray();
        XCube cube;
        try {
            cube = getSvc().getCube(sessionHandle, cubeName);
        } catch (GrillException e) {
            throw new WebApplicationException(e);
        }
        if (cube.getMeasures()!= null) {
            for (XMeasure measure : cube.getMeasures().getMeasures()) {
                try {
                    cubeAttribList.put(new JSONObject().put("name", measure.getName()).put("type", measure.getType()));
                } catch (JSONException j) {
                    LOG.error(j);
                }
            }
        }
        if(cube.getDimAttributes()!=null) {
            for (XDimAttribute dim : cube.getDimAttributes().getDimAttributes()) {
                try {
                    cubeAttribList.put(new JSONObject().put("name", dim.getName()).put("type", dim.getType()));
                } catch (JSONException j) {
                    LOG.error(j);
                }
            }
        }
        return cubeAttribList.toString();
    }

    /**
     * Get all column names and types of a dimension table
     *
     * @param publicId The publicId for the session in which user is working
     *
     * @param dimtableName name of dimension table to be described
     *
     * @return JSON string consisting of different column names and types
     *
     * @throws GrillException, JSONException
     */
    @GET @Path("tables/{dimtableName}/dimtable")
    @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public String getDimDescription(@QueryParam("publicId") UUID publicId, @PathParam("dimtableName") String dimtableName) {
        GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        JSONArray dimAttribList = new JSONArray();
        DimensionTable table;
        try {
            table = getSvc().getDimensionTable(sessionHandle, dimtableName);
        } catch (GrillException e) {
            throw new WebApplicationException(e);
        }
        if (table.getColumns() != null) {
            for (Column col : table.getColumns().getColumns()) {
                try {
                    dimAttribList.put(new JSONObject().put("name", col.getName()).put("type", col.getType()));
                } catch (JSONException j) {
                    LOG.error(j);
                }
            }
        }
        return dimAttribList.toString();
    }


    /**
     * Get all Table and column names and types which contain the search word
     *
     * @param publicId The publicId for the session in which user is working
     *
     * @param keyword keyword to be searched
     *
     * @return JSON string consisting of different table and column names and types
     *
     * @throws GrillException, JSONException
     */
    @GET @Path("tables/{keyword}")
    @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public String getFilterResults(@QueryParam("publicId") UUID publicId, @PathParam("keyword") String keyword)
    {
        GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        JSONArray tableList= null;
        JSONArray searchResultList = new JSONArray();
        try {

            tableList = new JSONArray(getAllTables(publicId));
        }catch(JSONException j)
        {
            LOG.error(j);
        }
        for(int item = 0; item < tableList.length(); item++)
        {
            String name =null, type=null;
            try {
                name = tableList.getJSONObject(item).getString("name");
                type = tableList.getJSONObject(item).getString("type");
            }catch(JSONException j)
            {
                LOG.error(j);
            }
            if(type.equals("cube")) {
                JSONArray cubeAttribList = null;
                JSONArray cubeSearchResultList = new JSONArray();
                try {

                    cubeAttribList = new JSONArray(getCubeDescription(publicId, name));
                }catch(JSONException j)
                {
                    LOG.error(j);
                }
                for(int col = 0; col < cubeAttribList.length(); col++) {
                    String colname =null, coltype =null;
                    try {
                        colname = cubeAttribList.getJSONObject(col).getString("name");
                        coltype = cubeAttribList.getJSONObject(col).getString("type");

                    } catch(JSONException j) {
                        LOG.error(j);
                    }
                    if(colname.contains(keyword)) {
                        try{
                            cubeSearchResultList.put(new JSONObject().put("name",colname).put("type",coltype));
                        }catch(JSONException j)
                        {
                            LOG.error(j);
                        }
                    }
                }
                if(cubeSearchResultList.length()>0) {
                    try {
                        searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns", cubeSearchResultList));
                    } catch(JSONException j) {
                        LOG.error(j);
                    }
                }
                else if(name.contains(keyword)) {
                    try {
                        searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns", cubeSearchResultList));
                    } catch(JSONException j) {
                        LOG.error(j);
                    }
                }
            }
            else if(type.equals("dimtable")) {
                JSONArray dimAttribList = null;
                JSONArray dimSearchResultList = new JSONArray();
                try {

                    dimAttribList = new JSONArray(getDimDescription(publicId, name));
                }catch(JSONException j)
                {
                    LOG.error(j);
                }
                for(int col = 0; col < dimAttribList.length(); col++) {
                    String colname =null, coltype =null;
                    try {
                        colname = dimAttribList.getJSONObject(col).getString("name");
                        coltype = dimAttribList.getJSONObject(col).getString("type");

                    } catch(JSONException j) {
                        LOG.error(j);
                    }
                    if(colname.contains(keyword)) {
                        try{
                            dimSearchResultList.put(new JSONObject().put("name",colname).put("type",coltype));
                        }catch(JSONException j)
                        {
                            LOG.error(j);
                        }
                    }
                }
                if(dimSearchResultList.length()>0) {
                    try {
                        searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns", dimSearchResultList));
                    } catch(JSONException j) {
                        LOG.error(j);
                    }
                }
                else if(name.contains(keyword)) {
                    try {
                        searchResultList.put(new JSONObject().put("name", name).put("type", type).put("columns", dimSearchResultList));
                    } catch(JSONException j) {
                        LOG.error(j);
                    }
                }
            }
        }
        return searchResultList.toString();
    }



    /**
     * Get all Table names and types which contain the search word
     *
     * @param publicId The publicId for the session in which user is working
     *
     * @param keyword keyword to be searched
     *
     * @return JSON string consisting of different table names and types
     *
     * @throws GrillException, JSONException
     *
     @GET @Path("tables/{keyword}")
     @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
     public String showFilterResults(@QueryParam("publicId") UUID publicId, @PathParam("keyword") String keyword)
     {
     GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
     checkSessionHandle(sessionHandle);
     JSONArray tableList= null;
     JSONArray searchResults = new JSONArray();
     try {

     tableList = new JSONArray(showAllTables(publicId));
     }catch(JSONException j)
     {
     LOG.error(j);
     }
     for(int item = 0; item < tableList.length(); item++)
     {
     String name =null, type=null;
     try {
     name = tableList.getJSONObject(item).getString("name");
     type = tableList.getJSONObject(item).getString("type");
     }catch(JSONException j)
     {
     LOG.error(j);
     }
     if(name.contains(keyword)) {
     try{
     searchResults.put(new JSONObject().put("name", name).put("type", type));
     }catch(JSONException j)
     {
     LOG.error(j);
     }
     }
     else if (type.equals("Cube")) {
     XCube cube;
     try {
     cube = getSvc().getCube(sessionHandle, name);
     } catch (GrillException e) {
     throw new WebApplicationException(e);
     }
     if(measureNameMatched(cube, keyword))
     {
     try{
     searchResults.put(new JSONObject().put("name", name).put("type", type));
     }catch(JSONException j)
     {
     LOG.error(j);
     }
     }
     else if(dimAttrNameMatched(cube, keyword))
     {
     try{
     searchResults.put(new JSONObject().put("name", name).put("type", type));
     }catch(JSONException j)
     {
     LOG.error(j);
     }
     }
     }
     }
     return searchResults.toString();
     }*/
}

