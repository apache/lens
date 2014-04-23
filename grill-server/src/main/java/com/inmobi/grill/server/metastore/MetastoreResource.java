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

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.APIResult.Status;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.metastore.CubeMetastoreService;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import java.util.List;

/**
 * metastore resource api
 *
 * This provides api for all things metastore.
 */
@Path("metastore")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class MetastoreResource {
  public static final Logger LOG = LogManager.getLogger(MetastoreResource.class);
  public static final APIResult SUCCESS = new APIResult(Status.SUCCEEDED, "");
  public static final ObjectFactory xCubeObjectFactory = new ObjectFactory();

  public CubeMetastoreService getSvc() {
    return (CubeMetastoreService)GrillServices.get().getService("metastore");
  }

  private void checkSessionId(GrillSessionHandle sessionHandle) {
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
   * Get all databases in the metastore
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return StringList consisting of all database names.
   * 
   * @throws GrillException
   */
  @GET @Path("databases")
  public StringList getAllDatabases(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    checkSessionId(sessionid);
    List<String> allNames;
    try {
      allNames = getSvc().getAllDatabases(sessionid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
    return new StringList(allNames);
  }

  /**
   * Get the current database
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return The current db name
   */
  @GET @Path("databases/current")
  public String getDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    checkSessionId(sessionid);
    LOG.info("Get database");
    try {
      return getSvc().getCurrentDatabase(sessionid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Set the current db 
   * 
   * @param sessionid The sessionid in which user is working
   * @param dbName The db name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if set was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if set has failed
   */
  @PUT @Path("databases/current")
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public APIResult setDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid, String dbName) {
    checkSessionId(sessionid);
    LOG.info("Set database:" + dbName);
    try {
      getSvc().setCurrentDatabase(sessionid, dbName);
    } catch (GrillException e) {
      LOG.error("Error changing current database", e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Delete the db specified by name. Deleting underlying tables is optional.
   * If db does not exist, delete is ignored.
   * 
   * @param sessionid The sessionid in which user is working
   * @param dbName The db name
   * @param cascade if true, all the tables inside the db will also be dropped.
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if delete was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if delete has failed
   */
  @DELETE @Path("databases/{dbName}")
  public APIResult dropDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dbName") String dbName,
      @QueryParam("cascade") boolean cascade) {
    checkSessionId(sessionid);
    LOG.info("Drop database " + dbName+ " cascade?" + cascade);
    try {
      getSvc().dropDatabase(sessionid, dbName, cascade);
    } catch (GrillException e) {
      LOG.error("Error dropping " + dbName, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Create a new database
   * 
   * @param sessionid The sessionid in which user is working
   * @param ignoreIfExisting If true, create will be ignored if db already exists,
   *  otherwise it fails.
   * @param dbName The db name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if create has failed
   */
  @POST @Path("databases")
  public APIResult createDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @QueryParam("ignoreIfExisting") @DefaultValue("true") boolean ignoreIfExisting,
      String dbName ) {
    checkSessionId(sessionid);
    LOG.info("Create database " + dbName + " Ignore Existing? " + ignoreIfExisting);

    try {
      getSvc().createDatabase(sessionid, dbName, ignoreIfExisting);
    } catch (GrillException e) {
      LOG.error("Error creating database " + dbName, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all cubes in the metastores
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return StringList consisting of all cubes names
   * 
   */
  @GET @Path("cubes")
  public StringList getAllCubes(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    checkSessionId(sessionid);
    try {
      return new StringList(getSvc().getAllCubeNames(sessionid));
    } catch (GrillException e) {
      LOG.error("Error getting cube names", e);
      throw new WebApplicationException(e);
    }
  }

  /**
   * Delete all cubes
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return
   * APIResult with state {@link Status#SUCCEEDED} in case of successful delete.
   * APIResult with state {@link Status#FAILED} in case of delete failure.
   * APIResult with state {@link Status#PARTIAL} in case of partial delete.
   */
  @DELETE @Path("cubes")
  public APIResult deleteAllCubes(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    checkSessionId(sessionid);
    boolean failed = false;
    List<String> cubeNames = null;
    int numDeleted = 0;
    try {
      cubeNames = getSvc().getAllCubeNames(sessionid);
      for (String cubeName : cubeNames) {
        getSvc().dropCube(sessionid, cubeName);
        numDeleted++;
      }
    } catch (GrillException e) {
      LOG.error("Error deleting cubes:", e);
      failed = true;
    }
    if (cubeNames != null && numDeleted == cubeNames.size()) {
      return new APIResult(Status.SUCCEEDED, "Delete of all "
          + "cubes is successful");
    } else {
      assert (failed);
      if (numDeleted == 0) {
        return new APIResult(Status.FAILED, "Delete of all "
            + "cubes has failed");        
      } else {
        return new APIResult(Status.PARTIAL, "Delete of all "
            + "cubes is partial");        
      }
    }
  }

  /**
   * Create a new cube
   * 
   * @param sessionid The sessionid in which user is working
   * @param cube The {@link XCube} representation of the cube definition
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if create has failed
   */
  @POST @Path("cubes")
  public APIResult createNewCube(@QueryParam("sessionid") GrillSessionHandle sessionid, XCube cube) {
    checkSessionId(sessionid);
    try {
      getSvc().createCube(sessionid, cube);
    } catch (GrillException e) {
      LOG.error("Error creating cube " + cube.getName(), e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  private void checkTableNotFound(GrillException e, String table) {
    if (e.getCause() instanceof HiveException) {
      HiveException hiveErr = (HiveException) e.getCause();
      if (hiveErr.getMessage().startsWith("Could not get table")) {
        throw new NotFoundException("Table not found " + table, e);
      }
    }
  }

  /**
   * Update cube definition
   * 
   * @param sessionid The sessionid in which user is working
   * @param cubeName The cube name
   * @param cube The {@link XCube} representation of the updated cube definition
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if udpate has failed
   */
  @PUT @Path("/cubes/{cubeName}")
  public APIResult updateCube(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("cubeName") String cubeName, XCube cube) {
    checkSessionId(sessionid);
    try {
      getSvc().updateCube(sessionid, cube);
    } catch (GrillException e) {
      checkTableNotFound(e, cube.getName());
      LOG.error("Error updating cube " + cube.getName(), e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get the cube specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param cubeName The cube name
   * 
   * @return JAXB representation of {@link XCube} 
   */
  @GET @Path("/cubes/{cubeName}")
  public JAXBElement<XCube> getCube(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("cubeName") String cubeName)  {
    checkSessionId(sessionid);
    try {
      return xCubeObjectFactory.createXCube(getSvc().getCube(sessionid, cubeName));
    } catch (GrillException e) {
      checkTableNotFound(e, cubeName);
      LOG.error("Error getting cube", e);
      throw new WebApplicationException(e);
    }
  }

  /**
   * Drop the cube, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param cubeName The cube name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/cubes/{cubeName}")
  public APIResult dropCube(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("cubeName") String cubeName) {
    checkSessionId(sessionid);
    try {
      getSvc().dropCube(sessionid, cubeName);
    } catch (GrillException e) {
      checkTableNotFound(e, cubeName);
      LOG.error("Error droping cube " + cubeName, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all storages in the metastore
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return StringList consisting of all the storage names
   * 
   * @throws GrillException
   */
  @GET @Path("storages")
  public StringList getAllStorages(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    checkSessionId(sessionid);
    try {
      return new StringList(getSvc().getAllStorageNames(sessionid));
    } catch (GrillException e) {
      LOG.error("Error getting storages", e);
      throw new WebApplicationException(e);
    }
  }

  /**
   * Create new storage
   * 
   * @param sessionid The sessionid in which user is working
   * @param storage The XStorage representation of storage
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if create has failed
   */
  @POST @Path("storages")
  public APIResult createNewStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, XStorage storage) {
    checkSessionId(sessionid);
    try {
      getSvc().createStorage(sessionid, storage);
    } catch (GrillException e) {
      LOG.error("Error creating storage " + storage.getName(), e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Delete all storages in metastore
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return
   * APIResult with state {@link Status#SUCCEEDED} in case of successful delete.
   * APIResult with state {@link Status#FAILED} in case of delete failure.
   * APIResult with state {@link Status#PARTIAL} in case of partial delete.
   */
  @DELETE @Path("storages")
  public APIResult deleteAllStoragess(@QueryParam("sessionid") GrillSessionHandle sessionid) {
    checkSessionId(sessionid);
    boolean failed = false;
    List<String> storageNames = null;
    int numDeleted = 0;
    try {
      storageNames = getSvc().getAllStorageNames(sessionid);
      for (String storageName : storageNames) {
        getSvc().dropStorage(sessionid, storageName);
        numDeleted++;
      }
    } catch (GrillException e) {
      LOG.error("Error deleting storages:", e);
      failed = true;
    }
    if (storageNames != null && numDeleted == storageNames.size()) {
      return new APIResult(Status.SUCCEEDED, "Delete of all "
          + "cubes is successful");
    } else {
      assert (failed);
      if (numDeleted == 0) {
        return new APIResult(Status.FAILED, "Delete of all "
            + "storages has failed");        
      } else {
        return new APIResult(Status.PARTIAL, "Delete of all "
            + "storages is partial");        
      }
    }
  }

  /**
   * Update storage definition
   * 
   * @param sessionid The sessionid in which user is working
   * @param storageName The storage name
   * @param storage The {@link XStorage} representation of the updated storage definition
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if update has failed
   */
  @PUT @Path("/storages/{storageName}")
  public APIResult updateStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("storageName") String storageName, XStorage storage) {
    checkSessionId(sessionid);
    try {
      getSvc().alterStorage(sessionid, storageName, storage);
    } catch (GrillException e) {
      checkTableNotFound(e, storageName);
      LOG.error("Error updating storage" + storageName, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get the storage specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param storageName The storage name
   * 
   * @return JAXB representation of {@link XStorage} 
   */
  @GET @Path("/storages/{storage}")
  public JAXBElement<XStorage> getStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("storage") String storageName) throws Exception {
    checkSessionId(sessionid);
    try {
      return xCubeObjectFactory.createXStorage(getSvc().getStorage(sessionid, storageName));
    } catch (GrillException e) {
      checkTableNotFound(e, storageName);
      throw e;
    }
  }

  /**
   * Drop the storage, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param storageName The storage name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/storages/{storage}")
  public APIResult dropStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("storage") String storageName) {
    checkSessionId(sessionid);
    try {
      getSvc().dropStorage(sessionid, storageName);
    } catch (GrillException e) {
      checkTableNotFound(e, storageName);
      LOG.error("Error dropping storage" + storageName, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all facts that belong to a cube in the metastore
   * 
   * @param sessionid The sessionid in which user is working
   * @param cubeName name of the cube
   * 
   * @return List of {@link FactTable} objects 
   * 
   */
  @GET @Path("/cubes/{cubeName}/facts")
  public List<FactTable> getAllFactsOfCube(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("cubeName") String cubeName)
      throws GrillException {
    checkSessionId(sessionid);
    try {
      return getSvc().getAllFactsOfCube(sessionid, cubeName);
    } catch (GrillException exc) {
      checkTableNotFound(exc, cubeName);
      throw exc;
    }
  }

  /**
   * Get all fact tables in the metastore in the current database
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return StringList consisting of all fact table names
   * 
   */
  @GET @Path("/facts")
  public StringList getAllFacts(@QueryParam("sessionid") GrillSessionHandle sessionid) throws GrillException {
    checkSessionId(sessionid);
    return new StringList(getSvc().getAllFactNames(sessionid));
  }

  /**
   * Delete all fact tables
   * 
   * @param sessionid The sessionid in which user is working
   * @param cascade if set to true, all the underlying tables will be dropped, if set to false, only the fact table will be dropped
   * 
   * @return
   * APIResult with state {@link Status#SUCCEEDED} in case of successful delete.
   * APIResult with state {@link Status#FAILED} in case of delete failure.
   * APIResult with state {@link Status#PARTIAL} in case of partial delete.
   */
  @DELETE @Path("facts")
  public APIResult deleteAllFacts(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade) {
    checkSessionId(sessionid);
    boolean failed = false;
    List<String> factNames = null;
    int numDeleted = 0;
    try {
      factNames = getSvc().getAllCubeNames(sessionid);
      for (String factName : factNames) {
        getSvc().dropFactTable(sessionid, factName, cascade);
        numDeleted++;
      }
    } catch (GrillException e) {
      LOG.error("Error deleting cubes:", e);
      failed = true;
    }
    if (factNames != null && numDeleted == factNames.size()) {
      return new APIResult(Status.SUCCEEDED, "Delete of all "
          + "fact tables is successful");
    } else {
      assert (failed);
      if (numDeleted == 0) {
        return new APIResult(Status.FAILED, "Delete of all "
            + "fact tables has failed");        
      } else {
        return new APIResult(Status.PARTIAL, "Delete of all "
            + "fact tables is partial");        
      }
    }
  }

  /**
   * Get the fact table specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * 
   * @return JAXB representation of {@link FactTable} 
   */
  @GET @Path("/facts/{factName}")
  public JAXBElement<FactTable> getFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factName") String factName)
      throws GrillException {
    checkSessionId(sessionid);
    try {
      return xCubeObjectFactory.createFactTable(getSvc().getFactTable(sessionid, factName));
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      throw exc;
    }
  }

  /**
   * Create a new fact tabble
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The {@link FactTable} representation of the fact table definition
   * @param storageTables The Storage table description of fact in each storage
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if create has failed
   */
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @POST @Path("/facts")
  public APIResult createFactTable(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("fact") FactTable fact,
      @FormDataParam("storageTables") XStorageTables storageTables)
          throws GrillException {
    checkSessionId(sessionid);
    try {
      LOG.info("Create fact table");
      getSvc().createFactTable(sessionid, fact, storageTables);
    } catch (GrillException exc) {
      LOG.error("Exception creating fact:" , exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Update fact table definition
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName name of the fact table
   * @param fact The {@link FactTable} representation of the updated fact table definition
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if udpate has failed
   */
  @PUT @Path("/facts/{factName}")
  public APIResult updateFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factName") String  factName, FactTable fact)
          throws GrillException {
    checkSessionId(sessionid);
    try {
      getSvc().updateFactTable(sessionid, fact);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      LOG.error("Error updating fact" + factName, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the fact table, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * @param cascade If true, all the storage tables of the fact will also be dropped
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factName}")
  public APIResult dropFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factName") String  factName,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade)  
          throws GrillException {
    checkSessionId(sessionid);
    try {
      getSvc().dropFactTable(sessionid, factName, cascade);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      LOG.error("Error dropping fact" + factName, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all storages of the fact table in the metastore
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * 
   * @return {@link StringList} consisting of all the storage names
   * 
   * @throws GrillException
   */
  @GET @Path("/facts/{factName}/storages")
  public StringList getStoragesOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factName") String factName) throws GrillException {
    checkSessionId(sessionid);
    try {
    return new StringList(getSvc().getStoragesOfFact(sessionid, factName));
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      throw new WebApplicationException(exc);
    }
  }

  /**
   * Drop all the storage tables of a fact table
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factName}/storages")
  public APIResult dropAllStoragesOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factName") String factName) {
    checkSessionId(sessionid);
    try {
      getSvc().dropAllStoragesOfFact(sessionid, factName);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      LOG.error("Error dropping storages of fact" + factName, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Add storage to fact table
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * @param storageTable The storage table description
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if add has failed
   */
  @POST @Path("/facts/{factName}/storages")
  public APIResult addStorageToFact(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factName") String factName, XStorageTableElement storageTable) {
    checkSessionId(sessionid);
    try {
      getSvc().addStorageToFact(sessionid, factName, storageTable);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      LOG.error("Error adding storage to fact" + factName, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the storage of a fact, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * @param storage The storage name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factName}/storages/{storage}")
  public APIResult dropStorageFromFact(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factName") String factName,
      @PathParam("storage") String storage) {
    checkSessionId(sessionid);
    try {
      getSvc().dropStorageOfFact(sessionid, factName, storage);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      LOG.error("Error dropping storage of fact" + factName, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return  SUCCESS;
  }

  /**
   * Get the fact storage table 
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * @param storage The storage name
   * 
   * @return JAXB representation of {@link XStorageTableElement} 
   */
  @GET @Path("/facts/{factName}/storages/{storage}")
  public JAXBElement<XStorageTableElement> getStorageOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factName") String factName,
      @PathParam("storage") String storage) throws  GrillException {
    return xCubeObjectFactory.createXStorageTableElement(getSvc().getStorageOfFact(sessionid, factName, storage));
  }

  /**
   * Get all partitions of the fact table in the specified storage;
   *  can be filtered as well.
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * @param storage The storage name
   * @param filter The filter for partitions, string representation of the filter
   * for ex: x &lt "XXX" and y &gt "YYY"
   * 
   * @return JAXB representation of {@link PartitionList} containing {@link XPartition} objects
   */
  @GET @Path("/facts/{factName}/storages/{storage}/partitions")
  public JAXBElement<PartitionList> getAllPartitionsOfFactStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factName") String factName,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter) throws GrillException {
    checkSessionId(sessionid);
    try {
      List<XPartition> partitions = getSvc().getAllPartitionsOfFactStorage(sessionid, factName, storage, filter);
      PartitionList partList = xCubeObjectFactory.createPartitionList();
      partList.getXPartition().addAll(partitions);
      return xCubeObjectFactory.createPartitionList(partList);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      throw exc;
    }
  }

  /**
   * Drop the partitions in the storage of a fact; can specified filter as well
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * @param storage The storage name
   * @param filter The filter for partitions, string representation of the filter
   * for ex: x &lt "XXX" and y &gt "YYY"
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factName}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfFactStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factName") String factName,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByFilter(sessionid, factName, storage, filter);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      return new APIResult(Status.PARTIAL, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Add a new partition for a storage of fact
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName fact table name
   * @param storage storage name
   * @param partition {@link XPartition} representation of partition
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if add has failed
   */
  @POST @Path("/facts/{factName}/storages/{storage}/partitions")
  public APIResult addPartitionToFactStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factName") String factName,
      @PathParam("storage") String storage,
      XPartition partition) {
    checkSessionId(sessionid);
    try {
      getSvc().addPartitionToFactStorage(sessionid, factName, storage, partition);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      LOG.error("Error adding partition to storage of fact" + factName + ":" + storage, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the partitions in the storage of a fact table, specified by exact values
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * @param storage The storage name
   * @param values Comma separated values
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factName}/storages/{storage}/partition")
  public APIResult dropPartitionOfFactStorageByValues(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factName") String factName,
      @PathParam("storage") String storage,
      @QueryParam("values") String values) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByValues(sessionid, factName, storage,
          values);

    } catch (GrillException e) {
      checkTableNotFound(e, factName);
      LOG.error("Error dropping partition to storage of fact" + factName + ":" + storage, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all dimension tables in the metastore
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return StringList consisting of all dimension table names
   * 
   */
  @GET @Path("/dimensions")
  public StringList getAllDims(@QueryParam("sessionid") GrillSessionHandle sessionid) throws GrillException {
    return new StringList(getSvc().getAllDimNames(sessionid));
  }

  /**
   * Create a new dimension table
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimensionTable The {@link DimensionTable} representation of the dimension table definition
   * @param storageTables The Storage table description of dimension table in each storage
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if create has failed
   */
  @POST @Path("/dimensions")
  public APIResult createCubeDimension(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("dimensionTable") DimensionTable dimensionTable,
      @FormDataParam("storageTables") XStorageTables storageTables) {
    checkSessionId(sessionid);
    try {
      getSvc().createCubeDimensionTable(sessionid, dimensionTable, storageTables);
    } catch (GrillException exc) {
      LOG.error("Error creating cube dimension table " + dimensionTable.getName(), exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Update dimension table definition
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimensionTable The {@link DimensionTable} representation of the updated dim table definition
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if udpate has failed
   */
  @PUT @Path("/dimensions/{dimName}")
  public APIResult updateCubeDimension(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimName") String dimName,
      DimensionTable dimensionTable) {
    checkSessionId(sessionid);
    try {
      getSvc().updateDimensionTable(sessionid, dimensionTable);
    } catch (GrillException exc) {
      checkTableNotFound(exc, dimName);
      LOG.error("Error updating cube dimension table " + dimName, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the dimension table, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * @param cascade if true, all the storage tables of dimension table will also be dropped
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimName}")
  public APIResult dropDimension(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimName") String dimension,
      @QueryParam("cascade") boolean cascade) {
    checkSessionId(sessionid);
    try {
      getSvc().dropDimensionTable(sessionid, dimension, cascade);
    } catch (GrillException e) {
      checkTableNotFound(e, dimension);
      LOG.error("Error dropping cube dimension table " + dimension, e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get the dimension table specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The cube name
   * 
   * @return JAXB representation of {@link DimensionTable} 
   */
  @GET @Path("/dimensions/{dimName}")
  public JAXBElement<DimensionTable> getDimension(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimName") String dimName)
      throws GrillException {
    checkSessionId(sessionid);
    try {
      return xCubeObjectFactory.createDimensionTable(getSvc().getDimensionTable(sessionid, dimName));
    } catch (GrillException exc) {
      checkTableNotFound(exc, dimName);
      throw exc;
    }
  }

  /**
   * Get all storages of the dimension table in the metastore
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * 
   * @return StringList consisting of all the storage names
   * 
   * @throws GrillException
   */
  @GET @Path("/dimensions/{dimName}/storages")
  public StringList getDimensionStorages(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimName") String dimension)
      throws GrillException {
    checkSessionId(sessionid);
    return new StringList(getSvc().getDimensionStorages(sessionid, dimension));
  }

  /**
   * Add storage to dimension table
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The dimension table name
   * @param storageTbl The Storage table description
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if add has failed
   */
  @POST @Path("/dimensions/{dimName}/storages")
  public APIResult createDimensionStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimName") String dimName,
      XStorageTableElement storageTbl) {
    checkSessionId(sessionid);
    try {
      getSvc().createDimensionStorage(sessionid, dimName, storageTbl);
    } catch (GrillException e) {
      checkTableNotFound(e, dimName);
      LOG.error("Error creating dimension table storage " + dimName + ":" + storageTbl.getStorageName(), e);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get the dim storage table 
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The fact table name
   * @param storage The storage name
   * 
   * @return JAXB representation of {@link XStorageTableElement} 
   */
  @GET @Path("/dimensions/{dimName}/storages/{storage}")
  public JAXBElement<XStorageTableElement> getStorageOfDim(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimName") String dimName,
      @PathParam("storage") String storage) throws  GrillException {
    checkSessionId(sessionid);
    return xCubeObjectFactory.createXStorageTableElement(getSvc().getStorageOfDim(sessionid, dimName, storage));
  }

  /**
   * Drop all the storage tables of a dimension table
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The dimension table name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimName}/storages")
  public APIResult dropAllStoragesOfDim(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimName") String dimName) {
    checkSessionId(sessionid);
    try {
      getSvc().dropAllStoragesOfDim(sessionid, dimName);
    } catch (GrillException exc) {
      checkTableNotFound(exc, dimName);
      LOG.error("Error dropping storages of dimension table " + dimName, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the storage of a dimension table, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The dimension table name
   * @param storage The storage name
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimName}/storages/{storage}")
  public APIResult dropStorageOfDim(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimName") String dimName,
      @PathParam("storage") String storage) {
    checkSessionId(sessionid);
    try {
      getSvc().dropStorageOfDim(sessionid, dimName, storage);
    } catch (GrillException exc) {
      checkTableNotFound(exc, dimName);
      LOG.error("Error dropping storage of dimension table " + dimName + ":" + storage, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all partition of the dimension table in the specified storage;
   *  can be filtered
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * @param storage The storage name
   * @param filter The filter for partitions, string representation of the filter
   * for ex: x &lt "XXX" and y &gt "YYY"
   * 
   * @return JAXB representation of {@link PartitionList} containing {@link XPartition} objects
   */
  @GET @Path("/dimensions/{dimName}/storages/{storage}/partitions")
  public JAXBElement<PartitionList> getAllPartitionsOfDimStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimName") String dimension,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter)
          throws GrillException {
    checkSessionId(sessionid);
    List<XPartition> partitions = getSvc().getAllPartitionsOfDimStorage(sessionid, dimension, storage, filter);
    PartitionList partList = xCubeObjectFactory.createPartitionList();
    partList.getXPartition().addAll(partitions);
    return xCubeObjectFactory.createPartitionList(partList);
  }

  /**
   * Drop the partitions in the storage of a dimension table; can specified filter as well
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The dimension table name
   * @param storage The storage name
   * @param filter The filter for partitions, string representation of the filter
   * for ex: x &lt "XXX" and y &gt "YYY"
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimName}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfDimStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimName") String dimName,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByFilter(sessionid, dimName, storage, filter);
    } catch (GrillException exc) {
      LOG.error("Error dropping partition on storage of dimension table " + dimName + ":" + storage, exc);
      return new APIResult(Status.PARTIAL, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the partitions in the storage of a dimension table, specified by exact values
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The dimension table name
   * @param storage The storage name
   * @param values Comma separated values
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimName}/storages/{storage}/partition")
  public APIResult dropPartitionsOfDimStorageByValue(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimName") String dimName,
      @PathParam("storage") String storage,
      @QueryParam("values") String values) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByValues(sessionid, dimName, storage,
          values);
    } catch (GrillException exc) {
      LOG.error("Error dropping partitions on storage of dimension table " + dimName + ":" + storage, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Add a new partition for a storage of dimension
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName dimension table name
   * @param storage storage name
   * @param partition {@link XPartition} representation of partition
   * 
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link Status#FAILED}, if add has failed
   */
  @POST @Path("/dimensions/{dimName}/storages/{storage}/partitions")
  public APIResult addPartitionToDimStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimName") String dimName,
      @PathParam("storage") String storage,
      XPartition partition) {
    checkSessionId(sessionid);
    try {
      getSvc().addPartitionToDimStorage(sessionid, dimName, storage, partition);
    } catch (GrillException exc) {
      LOG.error("Error adding partition to storage of dimension table " + dimName + ":" + storage, exc);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }
}
