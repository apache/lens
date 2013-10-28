package com.inmobi.grill.metastore.service;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.APIResult.Status;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.Database;
import com.inmobi.grill.metastore.model.DimensionTable;
import com.inmobi.grill.metastore.model.ObjectFactory;
import com.inmobi.grill.metastore.model.XCube;
import com.inmobi.grill.server.api.CubeMetastoreService;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import java.util.ArrayList;
import java.util.List;

@Path("metastore")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class MetastoreResource {
  public static final Logger LOG = LogManager.getLogger(MetastoreResource.class);
  public static final APIResult SUCCESS = new APIResult(APIResult.Status.SUCCEEDED, "");
  public static final ObjectFactory xCubeObjectFactory = new ObjectFactory();

  private String getCurrentUser() {
    return "";
  }

  public CubeMetastoreService getSvc() {
    return CubeMetastoreServiceImpl.getInstance(getCurrentUser());
  }

  @GET @Path("databases")
  public List<Database> getAllDatabases() throws GrillException {
    List<String> allNames = getSvc().getAllDatabases();
    if (allNames != null && !allNames.isEmpty()) {
      List<Database> dblist = new ArrayList<Database>();
      for (String dbName : allNames) {
        Database db = new Database();
        db.setName(dbName);
        dblist.add(db);
      }
      return dblist;
    }
    return null;
  }

  @GET @Path("database")
  public Database getDatabase() throws GrillException {
    LOG.info("Get database");
    Database db = new Database();
    db.setName(getSvc().getCurrentDatabase());
    return db;
  }

  @PUT @Path("database")
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public APIResult setDatabase(Database db) {
    LOG.info("Set database");
    try {
      getSvc().setCurrentDatabase(db.getName());
    } catch (GrillException e) {
      LOG.error("Error changing current database", e);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  @DELETE @Path("database/{dbname}")
  public APIResult dropDatabase(@PathParam("dbname") String dbName, 
  		@QueryParam("cascade") boolean cascade) {
    LOG.info("Drop database " + dbName+ " cascade?" + cascade);
    try {
      getSvc().dropDatabase(dbName, cascade);
    } catch (GrillException e) {
      LOG.error("Error dropping " + dbName, e);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  @PUT @Path("database/{dbname}")
  public APIResult createDatabase(Database db) {
    LOG.info("Create database " + db.getName() + " Ignore Existing? " + db.getIgnoreIfExisting());

    try {
      getSvc().createDatabase(db.getName(), db.getIgnoreIfExisting());
    } catch (GrillException e) {
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  @GET @Path("cubes")
  public List<String> getAllCubes() throws GrillException {
    try {
      return getSvc().getAllCubeNames();
    } catch (GrillException e) {
      LOG.error("Error getting cube names", e);
      throw e;
    }
  }

  @DELETE @Path("cubes")
  public String deleteAllCubes() {
    return "delete all cubes";
  }

  @POST @Path("cubes")
  public APIResult createNewCube(XCube cube) {
    try {
      getSvc().createCube(cube);
    } catch (GrillException e) {
      LOG.error("Error creating cube " + cube.getName());
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  private void checkTableNotFound(GrillException e, String cubeName) {
    if (e.getCause() instanceof HiveException) {
      HiveException hiveErr = (HiveException) e.getCause();
      if (hiveErr.getMessage().startsWith("Could not get table")) {
        throw new NotFoundException("Table not found " + cubeName, e);
      }
    }
  }

  @PUT @Path("/cubes/{cubename}")
  public APIResult updateCube(@PathParam("cubename") String cubename, XCube cube) {
    try {
      getSvc().updateCube(cube);
    } catch (GrillException e) {
      checkTableNotFound(e, cube.getName());
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  @GET @Path("/cubes/{cubename}")
  public JAXBElement<XCube> getCube(@PathParam("cubename") String cubeName) throws Exception{
    try {
      return xCubeObjectFactory.createXCube(getSvc().getCube(cubeName));
    } catch (GrillException e) {
      checkTableNotFound(e, cubeName);
      throw e;
    }
  }

  @DELETE @Path("/cubes/{cubename}")
  public APIResult dropCube(@PathParam("cubename") String cubeName, 
  		@QueryParam("cascade") boolean cascade) {
    try {
      getSvc().dropCube(cubeName, cascade);
    } catch (GrillException e) {
      checkTableNotFound(e, cubeName);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }




  /*<grill-url>/metastore/cubes/cubename/facts
  - GET - Get all the cube facts
  - PUT  - Not used
  - DELETE - Drop all the facts
  */


  /*
  <grill-url>/metastore/cubes/cubename/facts/factname
  - GET - Get the cube fact
  - PUT - Update the cube fact
  - DELETE - Drop the cube fact
  -POST - ?

  <grill-url>/metastore/cubes/cubename/facts/factname/storages
  - GET - get all the storages
  - POST - Add a storage
  - PUT  - Not used
  - DELETE - Drop all the storages
  <grill-url>/metastore/cubes/cubename/facts/factname/storages/storage
  - GET - Get the fact storage
  - PUT - Update the fact storage (add/remove update periods with storage)
  - DELETE - Drop the fact stoarge
  - POST - ?

  <grill-url>/metastore/cubes/cubename/facts/factname/storages/storage/partitions
  - GET - get all the partitions in storage
  - POST - Add a partition
  - PUT  - Not used
  - DELETE - Drop all the partitions

  <grill-url>/metastore/cubes/cubename/facts/factname/storages/storage/partitions?partfilter
  - GET - get all the partitions in storage with part filter
  - POST - Not Used
  - PUT  - Not used
  - DELETE - Drop all the partitions with the part filter

  <grill-url>/metastore/cubes/cubename/facts/factname/storages/storage/partitions/<partspec>
  - GET - Get the partition
  - PUT - Update the storage partition
  - DELETE - Drop the stoarge partition
  - POST - ?

  <grill-url>/metastore/dimensions/
    - GET - get all the dimensions
  - POST - Add a dimension
  - PUT  - Not used
  - DELETE - Drop all the dimensions
 */

  @POST @Path("/dimensions")
  public APIResult createCubeDimension(DimensionTable dimensionTable) {
    try {
      getSvc().createCubeDimensionTable(dimensionTable);
    } catch (GrillException exc) {
      LOG.error("Error creating cube dimension table " + dimensionTable.getName(), exc);
      return new APIResult(APIResult.Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }
  
  @PUT @Path("/dimensions/{dimname}")
  public APIResult updateCubdeDimension(@PathParam("dimname") String dimName, 
  		DimensionTable dimensionTable) {
  	try {
  		getSvc().updateDimensionTable(dimensionTable);
  	} catch (GrillException exc) {
  		checkTableNotFound(exc, dimensionTable.getName());
  		return new APIResult(Status.FAILED, exc.getMessage());
  	}
  	return SUCCESS;
  }

  @DELETE @Path("/dimensions/{dimname}")
  public APIResult dropDimension(@PathParam("dimname") String dimension, @QueryParam("cascade") boolean cascade) {
    try {
      getSvc().dropDimensionTable(dimension, cascade);
    } catch (GrillException e) {
      checkTableNotFound(e, dimension);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }
  
  @GET @Path("/dimensions/{dimname}")
  public JAXBElement<DimensionTable> getDimension(@PathParam("dimname") String dimName) throws GrillException {
  	try {
  		return xCubeObjectFactory.createDimensionTable(getSvc().getDimensionTable(dimName));
  	} catch (GrillException exc) {
  		checkTableNotFound(exc, dimName);
  		throw exc;
  	}
  }

  /*
  <grill-url>/metastore/dimensions/dimname
  - GET - Get the dimension
  - PUT - Update the dimension
  - DELETE - Drop the dimension
  - POST - ?

  <grill-url>/metastore/dimensions/dimname/storages
  - GET - get all the storages of the dimension dimname
  - POST - Add a storage to dimname
  - PUT  - Not used
  - DELETE - Drop all the strorages

  <grill-url>/metastore/dimensions/dimname/storages/storage
  - GET - Get the dimension storage
  - PUT - Update the dimension storage
  - DELETE - Drop the dimension storage
  - POST - ?

  <grill-url>/metastore/dimensions/dimname/storages/storage/partitions
  - GET - get all the partitions in storage
  - POST - Add a partition
  - PUT  - Not used
  - DELETE - Drop all the partitions

  <grill-url>/metastore/dimensions/dimname/storages/storage/partitions?<partfilter>
  - GET - get all the partitions in storage with the filter
  - POST - NOT used
  - PUT  - Not used
  - DELETE - Drop all the partitions with the filter

    <grill-url>/metastore/dimensions/dimname/storages/storage/<partspec>
  - GET - Get the partition
  - PUT - Update the storage partition
  - DELETE - Drop the stoarge partition
  - POST - ?
  */

  @GET @Path("/hello")
  public String getMessage() {
      return "Hello World! from metastore";
  }

  @GET
  public String index() {
    return "index";
  }
}
