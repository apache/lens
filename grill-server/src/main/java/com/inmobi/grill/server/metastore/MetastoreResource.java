package com.inmobi.grill.server.metastore;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.api.APIResult.Status;
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
  public static final APIResult SUCCESS = new APIResult(APIResult.Status.SUCCEEDED, "");
  public static final ObjectFactory xCubeObjectFactory = new ObjectFactory();

  public CubeMetastoreService getSvc() {
    return (CubeMetastoreService)GrillServices.get().getService("metastore");
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if set was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if set has failed
   */
  @PUT @Path("databases/current")
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public APIResult setDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid, String dbName) {
    LOG.info("Set database");
    try {
      getSvc().setCurrentDatabase(sessionid, dbName);
    } catch (GrillException e) {
      LOG.error("Error changing current database", e);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if delete was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if delete has failed
   */
  @DELETE @Path("databases/{dbname}")
  public APIResult dropDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dbname") String dbName, 
      @QueryParam("cascade") boolean cascade) {
    LOG.info("Drop database " + dbName+ " cascade?" + cascade);
    try {
      getSvc().dropDatabase(sessionid, dbName, cascade);
    } catch (GrillException e) {
      LOG.error("Error dropping " + dbName, e);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if create has failed
   */
  @POST @Path("databases")
  public APIResult createDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @QueryParam("ignoreifexist") @DefaultValue("true") boolean ignoreIfExisting,
      String dbName ) {
    LOG.info("Create database " + dbName + " Ignore Existing? " + ignoreIfExisting);

    try {
      getSvc().createDatabase(sessionid, dbName, ignoreIfExisting);
    } catch (GrillException e) {
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
   * APIResult with state {@value APIResult.Status#SUCCEEDED} in case of successful delete.
   * APIResult with state {@value APIResult.Status#FAILED} in case of delete failure.
   * APIResult with state {@value APIResult.Status#PARTIAL} in case of partial delete.
   */
  @DELETE @Path("cubes")
  public APIResult deleteAllCubes(@QueryParam("sessionid") GrillSessionHandle sessionid) {
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
      return new APIResult(APIResult.Status.SUCCEEDED, "Delete of all "
          + "cubes is successful");
    } else {
      assert (failed);
      if (numDeleted == 0) {
        return new APIResult(APIResult.Status.FAILED, "Delete of all "
            + "cubes has failed");        
      } else {
        return new APIResult(APIResult.Status.PARTIAL, "Delete of all "
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if create has failed
   */
  @POST @Path("cubes")
  public APIResult createNewCube(@QueryParam("sessionid") GrillSessionHandle sessionid, XCube cube) {
    try {
      getSvc().createCube(sessionid, cube);
    } catch (GrillException e) {
      LOG.error("Error creating cube " + cube.getName(), e);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
   * @param cubename The cube name
   * @param cube The {@link XCube} representation of the updated cube definition
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if udpate has failed
   */
  @PUT @Path("/cubes/{cubename}")
  public APIResult updateCube(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("cubename") String cubename, XCube cube) {
    try {
      getSvc().updateCube(sessionid, cube);
    } catch (GrillException e) {
      checkTableNotFound(e, cube.getName());
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
  @GET @Path("/cubes/{cubename}")
  public JAXBElement<XCube> getCube(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("cubename") String cubeName)  {
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/cubes/{cubename}")
  public APIResult dropCube(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("cubename") String cubeName) {
    try {
      getSvc().dropCube(sessionid, cubeName);
    } catch (GrillException e) {
      checkTableNotFound(e, cubeName);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if create has failed
   */
  @POST @Path("storages")
  public APIResult createNewStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, XStorage storage) {
    try {
      getSvc().createStorage(sessionid, storage);
    } catch (GrillException e) {
      LOG.error("Error creating storage " + storage.getName(), e);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Delete all storages in metastore
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return
   * APIResult with state {@value APIResult.Status#SUCCEEDED} in case of successful delete.
   * APIResult with state {@value APIResult.Status#FAILED} in case of delete failure.
   * APIResult with state {@value APIResult.Status#PARTIAL} in case of partial delete.
   */
  @DELETE @Path("storages")
  public APIResult deleteAllStoragess(@QueryParam("sessionid") GrillSessionHandle sessionid) {
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
      return new APIResult(APIResult.Status.SUCCEEDED, "Delete of all "
          + "cubes is successful");
    } else {
      assert (failed);
      if (numDeleted == 0) {
        return new APIResult(APIResult.Status.FAILED, "Delete of all "
            + "storages has failed");        
      } else {
        return new APIResult(APIResult.Status.PARTIAL, "Delete of all "
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if udpate has failed
   */
  @PUT @Path("/storages/{storage}")
  public APIResult updateStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("storage") String storageName, XStorage storage) {
    try {
      getSvc().alterStorage(sessionid, storageName, storage);
    } catch (GrillException e) {
      checkTableNotFound(e, storageName);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get the storage specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param storageName The stroage name
   * 
   * @return JAXB representation of {@link XStorage} 
   */
  @GET @Path("/storages/{storage}")
  public JAXBElement<XStorage> getStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("storage") String storageName) throws Exception{
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
   * @param stoageName The storage name
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/storages/{storage}")
  public APIResult dropStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("storage") String storageName) {
    try {
      getSvc().dropStorage(sessionid, storageName);
    } catch (GrillException e) {
      checkTableNotFound(e, storageName);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all facts that belong to a cube in the metastores
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return List of {@link FactTable} objects 
   * 
   */
  @GET @Path("/cubes/{cubename}/facts")
  public List<FactTable> getAllFactsOfCube(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("cubename") String cubeName) 
      throws GrillException {
    try {
      return getSvc().getAllFactsOfCube(sessionid, cubeName);
    } catch (GrillException exc) {
      checkTableNotFound(exc, cubeName);
      throw exc;
    }
  }

  /**
   * Get all fact tables in the metastores
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return StringList consisting of all fact table names
   * 
   */
  @GET @Path("/facts")
  public StringList getAllFacts(@QueryParam("sessionid") GrillSessionHandle sessionid) throws GrillException {
    return new StringList(getSvc().getAllFactNames(sessionid));
  }

  /**
   * Delete all fact tables
   * 
   * @param sessionid The sessionid in which user is working
   * 
   * @return
   * APIResult with state {@value APIResult.Status#SUCCEEDED} in case of successful delete.
   * APIResult with state {@value APIResult.Status#FAILED} in case of delete failure.
   * APIResult with state {@value APIResult.Status#PARTIAL} in case of partial delete.
   */
  @DELETE @Path("facts")
  public APIResult deleteAllFacts(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade) {
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
      return new APIResult(APIResult.Status.SUCCEEDED, "Delete of all "
          + "fact tables is successful");
    } else {
      assert (failed);
      if (numDeleted == 0) {
        return new APIResult(APIResult.Status.FAILED, "Delete of all "
            + "fact tables has failed");        
      } else {
        return new APIResult(APIResult.Status.PARTIAL, "Delete of all "
            + "fact tables is partial");        
      }
    }
  }

  /**
   * Get the fact table specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * 
   * @return JAXB representation of {@link FactTable} 
   */
  @GET @Path("/facts/{factname}")
  public JAXBElement<FactTable> getFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact) 
      throws GrillException {
    try {
      return xCubeObjectFactory.createFactTable(getSvc().getFactTable(sessionid, fact));
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if create has failed
   */
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @POST @Path("/facts")
  public APIResult createFactTable(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("fact") FactTable fact,
      @FormDataParam("storagetables") XStorageTables storageTables) 
          throws GrillException {
    try {
      getSvc().createFactTable(sessionid, fact, storageTables);
    } catch (GrillException exc) {
      return new APIResult(APIResult.Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Update fact table definition
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The {@link FactTable} representation of the updated fact table definition
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if udpate has failed
   */
  @PUT @Path("/facts/{factname}")
  public APIResult updateFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String  factName, FactTable fact) 
          throws GrillException {
    try {
      getSvc().updateFactTable(sessionid, fact);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      return new APIResult(APIResult.Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the fact table, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * @param cascase If true, all the storage tables of the fact will also be dropped
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factname}")
  public APIResult dropFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String  fact, 
      @DefaultValue("false") @QueryParam("cascade") boolean cascade)  
          throws GrillException {
    try {
      getSvc().dropFactTable(sessionid, fact, cascade);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(APIResult.Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Get all storages of the fact table in the metastore
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * 
   * @return StringList consisting of all the storage names
   * 
   * @throws GrillException
   */
  @GET @Path("/facts/{factname}/storages")
  public StringList getStoragesOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact) throws GrillException {
    try {
    return new StringList(getSvc().getStoragesOfFact(sessionid, fact));
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      throw new WebApplicationException(exc);
    }
  }

  /**
   * Drop all the storage tables of a fact table
   * 
   * @param sessionid The sessionid in which user is working
   * @param factName The fact table name
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factname}/storages")
  public APIResult dropAllStoragesOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String factName) {
    try {
      getSvc().dropAllStoragesOfFact(sessionid, factName);
    } catch (GrillException exc) {
      checkTableNotFound(exc, factName);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Add storage to fact table
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * @param storageTable The Storage table description
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if add has failed
   */
  @POST @Path("/facts/{factname}/storages")
  public APIResult addStorageToFact(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact, XStorageTableElement storageTable) {
    try {
      getSvc().addStorageToFact(sessionid, fact, storageTable);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the storage of a fact, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * @param storage The storage name
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factname}/storages/{storage}")
  public APIResult dropStorageFromFact(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact,
      @PathParam("storage") String storage) {
    try {
      getSvc().dropStorageOfFact(sessionid, fact, storage);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return  SUCCESS;
  }

  /**
   * Get the fact storage table 
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * @param storage The storage name
   * 
   * @return JAXB representation of {@link XStorageTableElement} 
   */
  @GET @Path("/facts/{factname}/storages/{storage}")
  public JAXBElement<XStorageTableElement> getStorageOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact,
      @PathParam("storage") String storage) throws  GrillException {
    return xCubeObjectFactory.createXStorageTableElement(getSvc().getStorageOfFact(sessionid, fact, storage));
  }

  /*
  @PUT @Path("/facts/{factname}/storages/{storage}")
  public APIResult alterFactStorageUpdatePeriod(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact,
                                                 @PathParam("storage") String storage,
                                                 StorageUpdatePeriodList periods) {
    try {
      getSvc().alterFactStorageUpdatePeriod(sessionid, fact, storage, periods);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

   */
  /**
   * Get all partitions of the fact table in the specified storage;
   *  can be filtered as well.
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * @param storage The storage name
   * @param filter The filter for partitions, string representation of the filter
   * for ex: x &lt "XXX" and y &gt "YYY"
   * 
   * @return JAXB representation of {@link PartitionList} containing {@link XPartition} objects
   */
  @GET @Path("/facts/{factname}/storages/{storage}/partitions")
  public JAXBElement<PartitionList> getAllPartitionsOfFactStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter) throws GrillException {
    try {
      List<XPartition> partitions = getSvc().getAllPartitionsOfFactStorage(sessionid, fact, storage, filter);
      PartitionList partList = xCubeObjectFactory.createPartitionList();
      partList.getXPartition().addAll(partitions);
      return xCubeObjectFactory.createPartitionList(partList);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      throw exc;
    }
  }

  /**
   * Drop the partitions in the storage of a fact; can specified filter as well
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * @param storage The storage name
   * @param filter The filter for partitions, string representation of the filter
   * for ex: x &lt "XXX" and y &gt "YYY"
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factname}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfFactStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter) {
    try {
      getSvc().dropPartitionFromStorageByFilter(sessionid, fact, storage, filter);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.PARTIAL, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Add a new partition for a storage of fact
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact fact table name
   * @param storage storage name
   * @param partition {@link XPartition} representation of partition
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if add has failed
   */
  @POST @Path("/facts/{factname}/storages/{storage}/partitions")
  public APIResult addPartitionToFactStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact,
      @PathParam("storage") String storage,
      XPartition partition) {
    try {
      getSvc().addPartitionToFactStorage(sessionid, fact, storage, partition);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the partitions in the storage of a fact table, specified by exact values
   * 
   * @param sessionid The sessionid in which user is working
   * @param fact The fact table name
   * @param storage The storage name
   * @param values Comma separated values
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/facts/{factname}/storages/{storage}/partition")
  public APIResult dropPartitionOfFactStorageByValues(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact,
      @PathParam("storage") String storage,
      @QueryParam("values") String values) {
    try {
      getSvc().dropPartitionFromStorageByValues(sessionid, fact, storage,
          values);

    } catch (GrillException e) {
      checkTableNotFound(e, fact);
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
    return new StringList(getSvc().getAllDatabases(sessionid));
  }

  /**
   * Create a new dimension tabble
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimensionTable The {@link DimensionTable} representation of the dimension table definition
   * @param storageTables The Storage table description of dimension table in each storage
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if create was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if create has failed
   */
  @POST @Path("/dimensions")
  public APIResult createCubeDimension(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("dimtable") DimensionTable dimensionTable,
      @FormDataParam("storagetables") XStorageTables storageTables) {
    try {
      getSvc().createCubeDimensionTable(sessionid, dimensionTable, storageTables);
    } catch (GrillException exc) {
      LOG.error("Error creating cube dimension table " + dimensionTable.getName(), exc);
      return new APIResult(APIResult.Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Update dimension table definition
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimensionTable The {@link DimensionTable} representation of the updated dim table definition
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if udpate has failed
   */
  @PUT @Path("/dimensions/{dimname}")
  public APIResult updateCubeDimension(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimName, 
      DimensionTable dimensionTable) {
    try {
      getSvc().updateDimensionTable(sessionid, dimensionTable);
    } catch (GrillException exc) {
      checkTableNotFound(exc, dimName);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the dimension table, specified by name
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimneison table name
   * @param cascade if true, all the storage tables of dimension table will also be dropped
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimname}")
  public APIResult dropDimension(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimension, 
      @QueryParam("cascade") boolean cascade) {
    try {
      getSvc().dropDimensionTable(sessionid, dimension, cascade);
    } catch (GrillException e) {
      checkTableNotFound(e, dimension);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
  @GET @Path("/dimensions/{dimname}")
  public JAXBElement<DimensionTable> getDimension(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimName) 
      throws GrillException {
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
  @GET @Path("/dimensions/{dimname}/storages")
  public StringList getDimensionStorages(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimension) 
      throws GrillException {
    return new StringList(getSvc().getDimensionStorages(sessionid, dimension));
  }

  /**
   * Add storage to dimension table
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimName The dimension table name
   * @param storageTable The Storage table description
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if add has failed
   */
  @POST @Path("/dimensions/{dimname}/storages")
  public APIResult createDimensionStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimName, 
      XStorageTableElement storageTbl) {
    try {
      getSvc().createDimensionStorage(sessionid, dimName, storageTbl);
    } catch (GrillException e) {
      checkTableNotFound(e, dimName);
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
  @GET @Path("/dimensions/{dimname}/storages/{storage}")
  public JAXBElement<XStorageTableElement> getStorageOfDim(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimName,
      @PathParam("storage") String storage) throws  GrillException {
    return xCubeObjectFactory.createXStorageTableElement(getSvc().getStorageOfDim(sessionid, dimName, storage));
  }

  /**
   * Drop all the storage tables of a dimension table
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimname}/storages")
  public APIResult dropAllStoragesOfDim(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimName) {
    try {
      getSvc().dropAllStoragesOfDim(sessionid, dimName);
    } catch (GrillException exc) {
      checkTableNotFound(exc, dimName);
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
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimname}/storages/{storage}")
  public APIResult dropStorageOfDim(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimName,
      @PathParam("storage") String storage) {
    try {
      getSvc().dropStorageOfDim(sessionid, dimName, storage);
    } catch (GrillException exc) {
      checkTableNotFound(exc, dimName);
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
  @GET @Path("/dimensions/{dimname}/storages/{storage}/partitions")
  public JAXBElement<PartitionList> getAllPartitionsOfDimStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimension,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter)
          throws GrillException {
    List<XPartition> partitions = getSvc().getAllPartitionsOfDimStorage(sessionid, dimension, storage, filter);
    PartitionList partList = xCubeObjectFactory.createPartitionList();
    partList.getXPartition().addAll(partitions);
    return xCubeObjectFactory.createPartitionList(partList);
  }

  /**
   * Drop the partitions in the storage of a dimension table; can specified filter as well
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * @param storage The storage name
   * @param filter The filter for partitions, string representation of the filter
   * for ex: x &lt "XXX" and y &gt "YYY"
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimname}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfDimStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimension,
      @PathParam("storage") String storage,
      @QueryParam("filter") String filter) {
    try {
      getSvc().dropPartitionFromStorageByFilter(sessionid, dimension, storage, filter);
    } catch (GrillException exc) {
      return new APIResult(Status.PARTIAL, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Drop the partitions in the storage of a dimension table, specified by exact values
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * @param storage The storage name
   * @param values Comma separated values
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if drop was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if drop has failed
   */
  @DELETE @Path("/dimensions/{dimname}/storages/{storage}/partition")
  public APIResult dropPartitionsOfDimStorageByValue(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimension,
      @PathParam("storage") String storage,
      @QueryParam("values") String values) {
    try {
      getSvc().dropPartitionFromStorageByValues(sessionid, dimension, storage,
          values);
    } catch (GrillException exc) {
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  /**
   * Add a new partition for a storage of dimension
   * 
   * @param sessionid The sessionid in which user is working
   * @param dimension dimension table name
   * @param storage storage name
   * @param partition {@link XPartition} representation of partition
   * 
   * @return {@link APIResult} with state {@link APIResult.Status#SUCCEEDED}, if add was successful.
   * {@link APIResult} with state {@link APIResult.Status#FAILED}, if add has failed
   */
  @POST @Path("/dimensions/{dimname}/storages/{storage}/partitions")
  public APIResult addPartitionToDimStorage(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimension,
      @PathParam("storage") String storage,
      XPartition partition) {
    try {
      getSvc().addPartitionToDimStorage(sessionid, dimension, storage, partition);
    } catch (GrillException exc) {
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }
}
