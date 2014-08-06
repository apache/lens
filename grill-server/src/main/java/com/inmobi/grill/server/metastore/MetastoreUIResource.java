package com.inmobi.grill.server.metastore;

/**
 * Created by inmobi on 25/07/14.
 */
//-----

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.metastore.XCube;
import com.inmobi.grill.api.metastore.XDimAttrNames;
import com.inmobi.grill.api.metastore.XDimension;
import com.inmobi.grill.api.metastore.XStorage;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.metastore.CubeMetastoreService;
import org.apache.commons.collections.MultiHashMap;
import org.glassfish.jersey.media.multipart.FormDataParam;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;

/**
 * metastore UI resource api
 *
 * This provides api for all things metastore UI.
 */

@Path("metaquery")
public class MetastoreUIResource {

    //private GrillClient client;

    /*public void setClient(GrillClient client) {
        this.client = client;
    }*/

    public CubeMetastoreService getSvc() { return (CubeMetastoreService)GrillServices.get().getService("metastore");}

    private void checkSessionId(GrillSessionHandle sessionHandle) {
        if (sessionHandle == null) {
            throw new BadRequestException("Invalid session handle");
        }
    }

    private boolean checkAttributeMatching(List<String> attribList, String search)
    {
        Iterator<String> it = attribList.iterator();
        while(it.hasNext())
        {
            if(it.next().contains(search)) return true;
        }
        return false;
    }

    @GET @Path("tables")
    @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public MultiHashMap showAllTables(@QueryParam("sessionid") GrillSessionHandle sessionid)
    //public String showAllTables()
    {
        checkSessionId(sessionid);
        MultiHashMap tableList = new MultiHashMap();
        List<String> cubes;
        try{
            cubes = getSvc().getAllCubeNames(sessionid);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }
        for(String cube : cubes)
        {
            tableList.put("Cube",cube);
        }
        List<String> dimTables;
        try{
            dimTables = getSvc().getAllDimTableNames(sessionid);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }
        for(String dimTable : dimTables)
        {
            tableList.put("DimensionTable",dimTable);
        }
        List<String> storageTables;
        try{
            storageTables = getSvc().getAllStorageNames(sessionid);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }
        for(String storageTable : storageTables)
        {
            tableList.put("StorageTable",storageTable);
        }
        return tableList;
        //return "Reached";
    }

   /* @GET @Path("tables/{searchNameOnly}")
    public MultiHashMap showFilterResultsNameOnly(MultiHashMap tableList, @QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("searchNameOnly") String search)
    {
        checkSessionId(sessionid);
        MultiHashMap searchResults = new MultiHashMap();
        Set set = tableList.entrySet();
        Iterator iterate = set.iterator();
        Map.Entry<String, List<String>> me;
        while(iterate.hasNext())
        {
            me = (Map.Entry) iterate.next();
            for(int item =0; item < me.getValue().size(); item++)
            {
                if(me.getValue().get(item).contains(search))
                {
                    searchResults.put(me.getKey(),me.getValue().get(item));
                }
            }
        }
        return searchResults;
    }*/

    @GET @Path("tables/{search}")
    public MultiHashMap showFilterResults(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("search") String search)
    {
        checkSessionId(sessionid);
        MultiHashMap tableList = showAllTables(sessionid);
        MultiHashMap searchResults = new MultiHashMap();
        Set set = tableList.entrySet();
        Iterator iterate = set.iterator();
        Map.Entry<String, List<String>> me;
        while(iterate.hasNext())
        {
            me = (Map.Entry) iterate.next();
            for(int item =0; item < me.getValue().size(); item++)
            {
                String itemName = me.getValue().get(item);
                if(me.getKey().equals("Cube"))
                {
                    if(itemName.contains(search))
                        searchResults.put("Cube", itemName);
                    else
                    {
                        XCube cube;
                        try {
                            cube = getSvc().getCube(sessionid, itemName);
                        } catch (GrillException e) {
                            throw new WebApplicationException(e);
                        }
                        if (checkAttributeMatching(cube.getDimAttrNames().getDimAttrNames(), search))
                            searchResults.put("Cube", itemName);
                        else if (checkAttributeMatching(cube.getMeasureNames().getMeasures(), search))
                            searchResults.put("Cube", itemName);
                    }
                }
                else if(me.getKey().equals("DimensionTable"))
                {
                    if(itemName.contains(search))
                        searchResults.put("DimensionTable", itemName);
                    /*else {
                        XDimension dimension;
                        try {
                            dimension = getSvc().getDimension(sessionid, itemName);
                        } catch (GrillException e) {
                            throw new WebApplicationException(e);
                        }
                    }*/
                }
                else if(me.getKey().equals("StorageTable"))
                {
                    if(itemName.contains(search))
                        searchResults.put("StorageTable", itemName);
                    /*else {
                        XStorage storage;
                        try {
                            storage = getSvc().getStorage(sessionid, itemName);
                        } catch (GrillException e) {
                            throw new WebApplicationException(e);
                        }
                    }*/
                }
            }
        }
        return searchResults;
    }

}