package com.inmobi.grill.metastore.service;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.StringList;
import com.inmobi.grill.client.api.APIResult.Status;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.*;
import com.inmobi.grill.server.api.CubeMetastoreService;
import com.inmobi.grill.server.api.QueryExecutionService;
import com.inmobi.grill.service.GrillServices;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import java.util.ArrayList;
import java.util.Collection;
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
    return (CubeMetastoreService)GrillServices.get().getService("metastore");
  }

  @GET @Path("databases")
  public List<Database> getAllDatabases(@QueryParam("sessionid") GrillSessionHandle sessionid) throws GrillException {
    List<String> allNames = getSvc().getAllDatabases(sessionid);
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
  public Database getDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid) throws GrillException {
    LOG.info("Get database");
    Database db = new Database();
    db.setName(getSvc().getCurrentDatabase(sessionid));
    return db;
  }

  @PUT @Path("database")
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public APIResult setDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid, Database db) {
    LOG.info("Set database");
    try {
      getSvc().setCurrentDatabase(sessionid, db.getName());
    } catch (GrillException e) {
      LOG.error("Error changing current database", e);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  @DELETE @Path("database/{dbname}")
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

  @PUT @Path("database/{dbname}")
  public APIResult createDatabase(@QueryParam("sessionid") GrillSessionHandle sessionid, Database db) {
    LOG.info("Create database " + db.getName() + " Ignore Existing? " + db.getIgnoreIfExisting());

    try {
      getSvc().createDatabase(sessionid, db.getName(), db.getIgnoreIfExisting());
    } catch (GrillException e) {
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  @GET @Path("cubes")
  public StringList getAllCubes(@QueryParam("sessionid") GrillSessionHandle sessionid) throws GrillException {
    try {
      return new StringList(getSvc().getAllCubeNames(sessionid));
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
  public APIResult createNewCube(@QueryParam("sessionid") GrillSessionHandle sessionid, XCube cube) {
    try {
      getSvc().createCube(sessionid, cube);
    } catch (GrillException e) {
      LOG.error("Error creating cube " + cube.getName());
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

  @PUT @Path("/cubes/{cubename}")
  public APIResult updateCube(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("cubename") String cubename, XCube cube) {
    try {
      getSvc().updateCube(sessionid, cube);
    } catch (GrillException e) {
      checkTableNotFound(e, cube.getName());
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

  @GET @Path("/cubes/{cubename}")
  public JAXBElement<XCube> getCube(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("cubename") String cubeName) throws Exception{
    try {
      return xCubeObjectFactory.createXCube(getSvc().getCube(sessionid, cubeName));
    } catch (GrillException e) {
      checkTableNotFound(e, cubeName);
      throw e;
    }
  }

  @DELETE @Path("/cubes/{cubename}")
  public APIResult dropCube(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("cubename") String cubeName, 
  		@QueryParam("cascade") boolean cascade) {
    try {
      getSvc().dropCube(sessionid, cubeName, cascade);
    } catch (GrillException e) {
      checkTableNotFound(e, cubeName);
      return new APIResult(APIResult.Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

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

  @GET @Path("/facts")
  public StringList getAllFacts(@QueryParam("sessionid") GrillSessionHandle sessionid) throws GrillException {
    return new StringList(getSvc().getAllFactNames(sessionid));
  }

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
  
  @POST @Path("/facts/{factname}")
  public APIResult createFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid, FactTable fact) 
  		throws GrillException {
  	try {
  		getSvc().createFactTable(sessionid, fact);
  	} catch (GrillException exc) {
  		return new APIResult(APIResult.Status.FAILED, exc.getMessage());
  	}
  	return SUCCESS;
  }
  
  @PUT @Path("/facts/{factname}")
  public APIResult updateFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid, FactTable fact) 
  		throws GrillException {
  	try {
  		getSvc().updateFactTable(sessionid, fact);
  	} catch (GrillException exc) {
  		return new APIResult(APIResult.Status.FAILED, exc.getMessage());
  	}
  	return SUCCESS;
  }
  
  @DELETE @Path("/facts/{factname}")
  public APIResult dropFactTable(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String  fact, 
  		@QueryParam("cascade") boolean cascade)  
  		throws GrillException {
  	try {
  		getSvc().dropFactTable(sessionid, fact, cascade);
  	} catch (GrillException exc) {
      checkTableNotFound(exc, fact);
  		return new APIResult(APIResult.Status.FAILED, exc.getMessage());
  	}
  	return SUCCESS;
  }

  @GET @Path("/facts/{factname}/storages")
  public StringList getStoragesOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact) throws GrillException {
    return new StringList(getSvc().getStoragesOfFact(sessionid, fact));
  }

  @POST @Path("/facts/{factname}/storages")
  public APIResult addStorageToFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact, FactStorage storage) {
    try {
      getSvc().addStorageToFact(sessionid, fact, storage);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  @DELETE @Path("/facts/{factname}/storages/{storage}")
  public APIResult dropStorageFromFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact, @PathParam("storage") String storage) {
    try {
      getSvc().dropStorageOfFact(sessionid, fact, storage);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return  SUCCESS;
  }

  @GET @Path("/facts/{factname}/storages/{storage}")
  public JAXBElement<FactStorage> getStorageOfFact(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("factname") String fact,
                                      @PathParam("storage") String storage) throws  GrillException {
    return xCubeObjectFactory.createFactStorage(getSvc().getStorageOfFact(sessionid, fact, storage));
  }


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

  @DELETE @Path("/facts/{factname}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfFactStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact,
                                                         @PathParam("storage") String storage,
                                                         @QueryParam("filter") String filter) {
    try {
      getSvc().dropPartitionsOfFactStorageByFilter(sessionid, fact, storage, filter);
    } catch (GrillException exc) {
      checkTableNotFound(exc, fact);
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

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

  @DELETE @Path("/facts/{factname}/storages/{storage}/partition/{values}")
  public APIResult dropPartitionOfFactStorageByValues(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("factname") String fact,
                                                      @PathParam("storage") String storage,
                                                      @PathParam("values") String values) {
    try {
      getSvc().dropPartitionOfFactStorageByValue(sessionid, fact, storage, values);
    } catch (GrillException e) {
      checkTableNotFound(e, fact);
      return new APIResult(Status.FAILED, e.getMessage());
    }
    return SUCCESS;
  }

 /*
  <grill-url>/metastore/cubes/cubename/facts/factname/storages/storage/partitions/<partspec>
  - GET - Get the partition
  - PUT - Update the storage partition
  - DELETE - Drop the stoarge partition
  - POST - ?*/

  @POST @Path("/dimensions")
  public APIResult createCubeDimension(@QueryParam("sessionid") GrillSessionHandle sessionid, DimensionTable dimensionTable) {
    try {
      getSvc().createCubeDimensionTable(sessionid, dimensionTable);
    } catch (GrillException exc) {
      LOG.error("Error creating cube dimension table " + dimensionTable.getName(), exc);
      return new APIResult(APIResult.Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }
  
  @PUT @Path("/dimensions/{dimname}")
  public APIResult updateCubeDimension(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimName, 
  		DimensionTable dimensionTable) {
  	try {
  		getSvc().updateDimensionTable(sessionid, dimensionTable);
  	} catch (GrillException exc) {
  		checkTableNotFound(exc, dimensionTable.getName());
  		return new APIResult(Status.FAILED, exc.getMessage());
  	}
  	return SUCCESS;
  }

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

  @GET @Path("/dimensions/{dimname}/storages")
  public List<JAXBElement<XStorage>> getDimensionStorages(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimension) 
  		throws GrillException {
  	Collection<String> storages = getSvc().getDimensionStorages(sessionid, dimension);
  	List<JAXBElement<XStorage>> xStorages = new ArrayList<JAXBElement<XStorage>>(storages.size());
  	
  	for (String s : storages) {
  		XStorage xs = xCubeObjectFactory.createXStorage();
  		xs.setName(s);
  		xStorages.add(xCubeObjectFactory.createXStorage(xs));
  	}
  	
  	return xStorages;
  }
  
  @POST @Path("/dimensions/{dimname}/storages")
  public APIResult createDimensionStorage(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimName, 
  		UpdatePeriodElement updatePeriodElement) {
  	try {
			getSvc().createDimensionStorage(sessionid, dimName, 
					updatePeriodElement.getUpdatePeriod(),
					updatePeriodElement.getStorageAttr());
		} catch (GrillException e) {
			checkTableNotFound(e, dimName);
			return new APIResult(Status.FAILED, e.getMessage());
		}
  	return SUCCESS;
  }
  
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
  
  // Get storage sets only the name of the XStorage object.
  @GET @Path("/dimensions/{dimname}/storages/{storage}")
  public JAXBElement<XStorage> getStorageOfDimension(@QueryParam("sessionid") GrillSessionHandle sessionid, @PathParam("dimname") String dimname, 
  		@PathParam("storage") String storage) throws GrillException {
  	try {
  		return xCubeObjectFactory.createXStorage(getSvc().getStorageOfDimension(sessionid, dimname, storage));
  	} catch (GrillException exc) {
  		checkTableNotFound(exc, dimname);
  		throw exc;
  	}
  }

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

  @DELETE @Path("/dimensions/{dimname}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfDimStorageByFilter(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimension,
                                                      @PathParam("storage") String storage,
                                                      @QueryParam("filter") String filter) {
    try {
      getSvc().dropPartitionOfDimStorageByFilter(sessionid, dimension, storage, filter);
    } catch (GrillException exc) {
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

  @DELETE @Path("/dimensions/{dimname}/storages/{storage}/partition")
  public APIResult dropPartitionsOfDimStorageByValue(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("dimname") String dimension,
                                                     @PathParam("storage") String storage,
                                                     @QueryParam("values") String values) {
    try {
      getSvc().dropPartitionOfDimStorageByValue(sessionid, dimension, storage, values);
    } catch (GrillException exc) {
      return new APIResult(Status.FAILED, exc.getMessage());
    }
    return SUCCESS;
  }

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
