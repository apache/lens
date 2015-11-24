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
package org.apache.lens.server.metastore;

import static org.apache.lens.api.APIResult.*;

import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.APIResult.*;
import org.apache.lens.api.DateTime;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.StringList;
import org.apache.lens.api.metastore.*;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metastore.CubeMetastoreService;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import org.glassfish.jersey.media.multipart.FormDataParam;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

/**
 * metastore resource api
 * <p> </p>
 * This provides api for all things metastore.
 */
@Path("metastore")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
@Slf4j
public class MetastoreResource {
  public static final ObjectFactory X_CUBE_OBJECT_FACTORY = new ObjectFactory();

  public static CubeMetastoreService getSvc() {
    return LensServices.get().getService(CubeMetastoreService.NAME);
  }

  private static void checkSessionId(LensSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  private void checkNonNullArgs(String message, Object... args) {
    for (Object arg : args) {
      if (arg == null) {
        throw new BadRequestException(message);
      }
    }
  }

  private void checkNonNullPartitionList(XPartitionList partitions) {
    checkNonNullArgs("Partition List is null", partitions);
    checkNonNullArgs("One partition is null", partitions.getPartition().toArray());
  }

  private static LensException processLensException(LensException exc) {
    if (exc != null) {
      exc.buildLensErrorTO(LensServices.get().getErrorCollection());
    }
    return exc;
  }

  public enum Entity {
    DATABASE {
      @Override
      public List<String> doGetAll(LensSessionHandle sessionHandle) throws LensException {
        return getSvc().getAllDatabases(sessionHandle);
      }

      @Override
      public void doDelete(LensSessionHandle sessionid, String entityName, Boolean cascade) throws LensException {
        if (cascade == null) {
          throw new NotImplementedException();
        } else {
          getSvc().dropDatabase(sessionid, entityName, cascade);
        }
      }

    }, STORAGE {
      @Override
      public List<String> doGetAll(LensSessionHandle sessionid) throws LensException {
        return getSvc().getAllStorageNames(sessionid);
      }

      @Override
      public void doDelete(LensSessionHandle sessionid, String entityName, Boolean cascade) throws LensException {
        if (cascade == null) {
          getSvc().dropStorage(sessionid, entityName);
        } else {
          throw new NotImplementedException();
        }
      }
    }, CUBE {
      @Override
      public List<String> doGetAll(LensSessionHandle sessionHandle) throws LensException {
        return getSvc().getAllCubeNames(sessionHandle);
      }

      @Override
      public void doDelete(LensSessionHandle sessionid, String entityName, Boolean cascade) throws LensException {
        if (cascade == null) {
          getSvc().dropCube(sessionid, entityName);
        } else {
          throw new NotImplementedException();
        }
      }
    }, FACT {
      @Override
      public List<String> doGetAll(LensSessionHandle sessionid) throws LensException {
        return getSvc().getAllFactNames(sessionid, null);
      }

      @Override
      public void doDelete(LensSessionHandle sessionid, String entityName, Boolean cascade) throws LensException {
        if (cascade == null) {
          throw new NotImplementedException();
        } else {
          getSvc().dropFactTable(sessionid, entityName, cascade);
        }
      }
    }, DIMENSION {
      @Override
      public List<String> doGetAll(LensSessionHandle sessionid) throws LensException {
        return getSvc().getAllDimensionNames(sessionid);
      }

      @Override
      public void doDelete(LensSessionHandle sessionid, String entityName, Boolean cascade) throws LensException {
        if (cascade == null) {
          getSvc().dropDimension(sessionid, entityName);
        } else {
          throw new NotImplementedException();
        }
      }
    }, DIMTABLE {
      @Override
      public List<String> doGetAll(LensSessionHandle sessionid) throws LensException {
        return getSvc().getAllDimTableNames(sessionid, null);
      }

      @Override
      public void doDelete(LensSessionHandle sessionid, String entityName, Boolean cascade) throws LensException {
        if (cascade == null) {
          throw new NotImplementedException();
        } else {
          getSvc().dropDimensionTable(sessionid, entityName, cascade);
        }
      }
    };

    public abstract List<String> doGetAll(LensSessionHandle sessionid) throws LensException;

    public abstract void doDelete(LensSessionHandle sessionid, String entityName, Boolean cascade) throws LensException;

    public StringList getAll(LensSessionHandle sessionid) {
      checkSessionId(sessionid);
      List<String> allNames;
      try {
        allNames = doGetAll(sessionid);
      } catch (LensException e) {
        throw new WebApplicationException(e);
      }
      return new StringList(allNames);
    }

    public APIResult delete(LensSessionHandle sessionid, String entityName, Boolean cascade) {
      log.info("Drop {} {} cascade: {}", name(), entityName, cascade);
      checkSessionId(sessionid);
      try {
        doDelete(sessionid, entityName, cascade);
      } catch (LensException e) {
        checkTableNotFound(e, entityName);
        log.error("Error droping {} {}", name(), entityName, e);
        return failure(processLensException(e));
      }
      return success();
    }

    public APIResult delete(LensSessionHandle sessionid, String entityName) {
      return delete(sessionid, entityName, null);
    }

    public APIResult deleteAll(LensSessionHandle sessionid, Boolean cascade) {
      checkSessionId(sessionid);
      List<String> entities;
      int numDeleted = 0;
      int numExpected = 0;
      LensException exc = null;
      try {
        entities = doGetAll(sessionid);
        numExpected = entities.size();
        for (String entity : entities) {
          doDelete(sessionid, entity, cascade);
          numDeleted++;
        }
      } catch (LensException e) {
        log.error("Error deleting cubes:", e);
        exc = e;
      }
      return successOrPartialOrFailure(numDeleted, numExpected, processLensException(exc));
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
   * @return StringList consisting of all database names.
   * @throws WebApplicationException
   */
  @GET
  @Path("databases")
  public StringList getAllDatabases(@QueryParam("sessionid") LensSessionHandle sessionid) {
    return Entity.DATABASE.getAll(sessionid);
  }

  /**
   * Get the current database
   *
   * @param sessionid The sessionid in which user is working
   * @return The current db name
   */
  @GET
  @Path("databases/current")
  public String getDatabase(@QueryParam("sessionid") LensSessionHandle sessionid) {
    checkSessionId(sessionid);
    log.info("Get database");
    try {
      return getSvc().getCurrentDatabase(sessionid);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Set the current db
   *
   * @param sessionid The sessionid in which user is working
   * @param dbName    The db name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if set was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if set has failed
   */
  @PUT
  @Path("databases/current")
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public APIResult setDatabase(@QueryParam("sessionid") LensSessionHandle sessionid, String dbName) {
    checkSessionId(sessionid);
    log.info("Set database:{}", dbName);
    try {
      getSvc().setCurrentDatabase(sessionid, dbName);
      return success();
    } catch (LensException e) {
      log.error("Error changing current database", e);
      return failure(processLensException(e));
    }
  }

  /**
   * Delete the db specified by name. Deleting underlying tables is optional. If db does not exist, delete is ignored.
   *
   * @param sessionid The sessionid in which user is working
   * @param dbName    The db name
   * @param cascade   if true, all the tables inside the db will also be dropped.
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if delete was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if delete has failed
   */
  @DELETE
  @Path("databases/{dbName}")
  public APIResult dropDatabase(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dbName") String dbName,
    @QueryParam("cascade") boolean cascade) {
    return Entity.DATABASE.delete(sessionid, dbName, cascade);
  }

  /**
   * Create a new database
   *
   * @param sessionid        The sessionid in which user is working
   * @param ignoreIfExisting If true, create will be ignored if db already exists, otherwise it fails.
   * @param dbName           The db name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if create has failed
   */
  @POST
  @Path("databases")
  public APIResult createDatabase(@QueryParam("sessionid") LensSessionHandle sessionid,
    @QueryParam("ignoreIfExisting") @DefaultValue("true") boolean ignoreIfExisting,
    String dbName) {
    checkSessionId(sessionid);
    log.info("Create database {} Ignore Existing? {}", dbName, ignoreIfExisting);
    try {
      getSvc().createDatabase(sessionid, dbName, ignoreIfExisting);
      return success();
    } catch (LensException e) {
      log.error("Error creating database {}", dbName, e);
      return failure(processLensException(e));
    }
  }

  /**
   * Get all native tables.
   *
   * @param sessionid The sessionid in which user is working
   * @param dbOption  The options available are 'current' and 'all'. If option is current, gives all tables from current
   *                  db. If option is all, gives all tables from all databases. If dbname is passed, dbOption is
   *                  ignored. If no dbOption or dbname are passed, then default is to get tables from current db.
   * @param dbName    The db name. If not empty, the tables in the db will be returned
   * @return StringList consisting of all table names.
   * @throws WebApplicationException
   */
  @GET
  @Path("nativetables")
  public StringList getAllNativeTables(@QueryParam("sessionid") LensSessionHandle sessionid,
    @QueryParam("dbOption") String dbOption,
    @QueryParam("dbName") String dbName) {
    checkSessionId(sessionid);
    List<String> allNames;
    try {
      if (StringUtils.isBlank(dbName) && !StringUtils.isBlank(dbOption)) {
        if (!dbOption.equalsIgnoreCase("current") && !dbOption.equalsIgnoreCase("all")) {
          throw new BadRequestException("Invalid dbOption param:" + dbOption
            + " Allowed values are 'current' and 'all'");
        }
      }
      allNames = getSvc().getAllNativeTableNames(sessionid, dbOption, dbName);
    } catch (LensException e) {
      throw new WebApplicationException(e);
    }
    return new StringList(allNames);
  }

  /**
   * Get the native table passed in name
   *
   * @param sessionid The sessionid in which user is working
   * @param tableName The native table name
   * @return JAXB representation of {@link XNativeTable}
   * @throws WebApplicationException
   */
  @GET
  @Path("nativetables/{tableName}")
  public JAXBElement<XNativeTable> getNativeTable(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("tableName") String tableName) {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXNativeTable(getSvc().getNativeTable(sessionid, tableName));
    } catch (LensException e) {
      checkTableNotFound(e, tableName);
      log.error("Error getting native table", e);
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get all cubes in the metastores, of the specified type
   *
   * @param sessionid The sessionid in which user is working
   * @param cubeTypes The type of cubes. Accepted values are 'all' or 'base' or 'derived' or 'queryable'
   * @return StringList consisting of all cubes names
   */
  @GET
  @Path("cubes")
  public StringList getAllCubes(@QueryParam("sessionid") LensSessionHandle sessionid,
    @QueryParam("type") @DefaultValue("all") String cubeTypes) {
    checkSessionId(sessionid);
    try {
      switch (cubeTypes) {
      case "all":
        return new StringList(getSvc().getAllCubeNames(sessionid));
      case "base":
        return new StringList(getSvc().getAllBaseCubeNames(sessionid));
      case "derived":
        return new StringList(getSvc().getAllDerivedCubeNames(sessionid));
      case "queryable":
        return new StringList(getSvc().getAllQueryableCubeNames(sessionid));
      default:
        throw new BadRequestException("Invalid type " + cubeTypes + " Accepted"
          + " values are 'all' or 'base' or 'derived' or 'queryable'");
      }
    } catch (LensException e) {
      log.error("Error getting cube names", e);
      throw new WebApplicationException(e);
    }
  }

  /**
   * Delete all cubes
   *
   * @param sessionid The sessionid in which user is working
   * @return APIResult with state {@link Status#SUCCEEDED} in case of successful delete. APIResult with state {@link
   * Status#FAILED} in case of delete failure. APIResult with state {@link Status#PARTIAL} in case of partial delete.
   */
  @DELETE
  @Path("cubes")
  public APIResult deleteAllCubes(@QueryParam("sessionid") LensSessionHandle sessionid) {
    checkSessionId(sessionid);
    return Entity.CUBE.deleteAll(sessionid, null);
  }

  /**
   * Create a new cube
   *
   * @param sessionid The sessionid in which user is working
   * @param cube      The {@link XCube} representation of the cube definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if create has failed
   */
  @POST
  @Path("cubes")
  public APIResult createNewCube(@QueryParam("sessionid") LensSessionHandle sessionid, XCube cube) {
    checkSessionId(sessionid);
    try {
      getSvc().createCube(sessionid, cube);
    } catch (LensException e) {
      if (cube instanceof XDerivedCube) {
        // parent should exist
        checkTableNotFound(e, ((XDerivedCube) cube).getParent());
      }
      log.error("Error creating cube {}", cube.getName(), e);
      return failure(processLensException(e));
    }
    return success();
  }


  private static void checkTableNotFound(LensException e, String table) {
    List<String> messages = Lists.newArrayList();
    messages.add(e.getMessage());
    if (e.getCause() instanceof HiveException) {
      messages.add(e.getCause().getMessage());
    }
    for (String message : messages) {
      if (message != null && message.startsWith("Could not get table")) {
        throw new NotFoundException("Table not found " + table, e);
      }
    }
  }

  /**
   * Update cube definition
   *
   * @param sessionid The sessionid in which user is working
   * @param cubeName  The cube name
   * @param cube      The {@link XCube} representation of the updated cube definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/cubes/{cubeName}")
  public APIResult updateCube(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("cubeName") String cubeName, XCube cube) {
    checkSessionId(sessionid);
    try {
      getSvc().updateCube(sessionid, cube);
    } catch (LensException e) {
      if (cube instanceof XDerivedCube) {
        // parent should exist
        checkTableNotFound(e, ((XDerivedCube) cube).getParent());
      }
      checkTableNotFound(e, cube.getName());
      log.error("Error updating cube {}", cube.getName(), e);
      return failure(processLensException(e));
    }
    return success();
  }

  /**
   * Get the cube specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param cubeName  The cube name
   * @return JAXB representation of {@link XCube}
   */
  @GET
  @Path("/cubes/{cubeName}")
  public JAXBElement<XCube> getCube(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("cubeName") String cubeName) {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXCube(getSvc().getCube(sessionid, cubeName));
    } catch (LensException e) {
      checkTableNotFound(e, cubeName);
      log.error("Error getting cube", e);
      throw new WebApplicationException(e);
    }
  }

  /**
   * Drop the cube, specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param cubeName  The cube name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/cubes/{cubeName}")
  public APIResult dropCube(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("cubeName") String cubeName) {
    return Entity.CUBE.delete(sessionid, cubeName, null);
  }

  /**
   * Get all storages in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @return StringList consisting of all the storage names
   * @throws WebApplicationException Wraps LensException
   */
  @GET
  @Path("storages")
  public StringList getAllStorages(@QueryParam("sessionid") LensSessionHandle sessionid) {
    checkSessionId(sessionid);
    return Entity.STORAGE.getAll(sessionid);
  }

  /**
   * Create new storage
   *
   * @param sessionid The sessionid in which user is working
   * @param storage   The XStorage representation of storage
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if create has failed
   */
  @POST
  @Path("storages")
  public APIResult createNewStorage(@QueryParam("sessionid") LensSessionHandle sessionid, XStorage storage) {
    checkSessionId(sessionid);
    try {
      getSvc().createStorage(sessionid, storage);
    } catch (LensException e) {
      log.error("Error creating storage {}", storage.getName(), e);
      return failure(processLensException(e));
    }
    return success();
  }

  /**
   * Delete all storages in metastore
   *
   * @param sessionid The sessionid in which user is working
   * @return APIResult with state {@link Status#SUCCEEDED} in case of successful delete. APIResult with state {@link
   * Status#FAILED} in case of delete failure. APIResult with state {@link Status#PARTIAL} in case of partial delete.
   */
  @DELETE
  @Path("storages")
  public APIResult deleteAllStorages(@QueryParam("sessionid") LensSessionHandle sessionid) {
    return Entity.STORAGE.deleteAll(sessionid, null);
  }

  /**
   * Update storage definition
   *
   * @param sessionid   The sessionid in which user is working
   * @param storageName The storage name
   * @param storage     The {@link XStorage} representation of the updated storage definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/storages/{storageName}")
  public APIResult updateStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("storageName") String storageName, XStorage storage) {
    checkSessionId(sessionid);
    try {
      getSvc().alterStorage(sessionid, storageName, storage);
    } catch (LensException e) {
      checkTableNotFound(e, storageName);
      log.error("Error updating storage {}", storageName, e);
      return failure(processLensException(e));
    }
    return success();
  }

  /**
   * Get the storage specified by name
   *
   * @param sessionid   The sessionid in which user is working
   * @param storageName The storage name
   * @return JAXB representation of {@link XStorage}
   */
  @GET
  @Path("/storages/{storage}")
  public JAXBElement<XStorage> getStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("storage") String storageName) throws Exception {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXStorage(getSvc().getStorage(sessionid, storageName));
    } catch (LensException e) {
      checkTableNotFound(e, storageName);
      throw e;
    }
  }

  /**
   * Drop the storage, specified by name
   *
   * @param sessionid   The sessionid in which user is working
   * @param storageName The storage name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/storages/{storage}")
  public APIResult dropStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("storage") String storageName) {
    return Entity.STORAGE.delete(sessionid, storageName, null);
  }

  /**
   * Get all dimensions in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @return StringList consisting of all the dimension names
   * @throws WebApplicationException wraps lensException
   */
  @GET
  @Path("dimensions")
  public StringList getAllDimensionNames(@QueryParam("sessionid") LensSessionHandle sessionid) {
    return Entity.DIMENSION.getAll(sessionid);
  }

  /**
   * Create new dimension
   *
   * @param sessionid The sessionid in which user is working
   * @param dimension The XDimension representation of dimension
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if create has failed
   */
  @POST
  @Path("dimensions")
  public APIResult createDimension(@QueryParam("sessionid") LensSessionHandle sessionid, XDimension dimension) {
    checkSessionId(sessionid);
    try {
      getSvc().createDimension(sessionid, dimension);
    } catch (LensException e) {
      log.error("Error creating dimension {}", dimension.getName(), e);
      return failure(processLensException(e));
    }
    return success();
  }

  /**
   * Delete all dimensions in metastore
   *
   * @param sessionid The sessionid in which user is working
   * @return APIResult with state {@link Status#SUCCEEDED} in case of successful delete. APIResult with state {@link
   * Status#FAILED} in case of delete failure. APIResult with state {@link Status#PARTIAL} in case of partial delete.
   */
  @DELETE
  @Path("dimensions")
  public APIResult deleteAllDimensions(@QueryParam("sessionid") LensSessionHandle sessionid) {
    return Entity.DIMENSION.deleteAll(sessionid, null);
  }

  /**
   * Update dimension definition
   *
   * @param sessionid The sessionid in which user is working
   * @param dimName   The dimension name
   * @param dimension The {@link XDimension} representation of the updated dimension definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/dimensions/{dimName}")
  public APIResult updateDimension(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimName") String dimName, XDimension dimension) {
    checkSessionId(sessionid);
    try {
      getSvc().updateDimension(sessionid, dimName, dimension);
    } catch (LensException e) {
      checkTableNotFound(e, dimName);
      log.error("Error updating dimension {}", dimName, e);
      return failure(processLensException(e));
    }
    return success();
  }

  /**
   * Get the dimension specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param dimName   The dimension name
   * @return JAXB representation of {@link XDimension}
   */
  @GET
  @Path("/dimensions/{dimName}")
  public JAXBElement<XDimension> getDimension(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimName") String dimName) throws Exception {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXDimension(getSvc().getDimension(sessionid, dimName));
    } catch (LensException e) {
      checkTableNotFound(e, dimName);
      throw e;
    }
  }

  /**
   * Drop the dimension, specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param dimName   The dimension name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/dimensions/{dimName}")
  public APIResult dropDimension(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimName") String dimName) {
    return Entity.DIMENSION.delete(sessionid, dimName, null);
  }

  /**
   * Get all dimtables that belong to a dimension in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @param dimensionName name of the dimension
   * @return List of {@link XDimensionTable} objects
   */
  @GET
  @Path("/dimensions/{dimName}/dimtables")
  public StringList getAllDimensionTablesOfDimension(
    @QueryParam("sessionid") LensSessionHandle sessionid, @PathParam("dimName") String dimensionName)
    throws LensException {
    checkSessionId(sessionid);
    try {
      return new StringList(getSvc().getAllDimTableNames(sessionid, dimensionName));
    } catch (LensException exc) {
      checkTableNotFound(exc, dimensionName);
      throw exc;
    }
  }

  /**
   * Get all facts that belong to a cube in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @param cubeName  name of the base cube or derived cube
   * @return List of {@link XFactTable} objects
   */
  @GET
  @Path("/cubes/{cubeName}/facts")
  public StringList getAllFactsOfCube(
    @QueryParam("sessionid") LensSessionHandle sessionid, @PathParam("cubeName") String cubeName)
    throws LensException {
    checkSessionId(sessionid);
    try {
      return new StringList(getSvc().getAllFactNames(sessionid, cubeName));
    } catch (LensException exc) {
      checkTableNotFound(exc, cubeName);
      throw exc;
    }
  }

  /**
   * Get all fact tables in the metastore in the current database
   *
   * @param sessionid The sessionid in which user is working
   * @return StringList consisting of all fact table names
   */
  @GET
  @Path("/facts")
  public StringList getAllFacts(@QueryParam("sessionid") LensSessionHandle sessionid) throws LensException {
    checkSessionId(sessionid);
    return Entity.FACT.getAll(sessionid);
  }

  /**
   * Delete all fact tables
   *
   * @param sessionid The sessionid in which user is working
   * @param cascade   if set to true, all the underlying tables will be dropped, if set to false, only the fact table
   *                  will be dropped
   * @return APIResult with state {@link Status#SUCCEEDED} in case of successful delete. APIResult with state {@link
   * Status#FAILED} in case of delete failure. APIResult with state {@link Status#PARTIAL} in case of partial delete.
   */
  @DELETE
  @Path("facts")
  public APIResult deleteAllFacts(@QueryParam("sessionid") LensSessionHandle sessionid,
    @DefaultValue("false") @QueryParam("cascade") boolean cascade) {
    return Entity.FACT.deleteAll(sessionid, cascade);
  }

  /**
   * Get the fact table specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @return JAXB representation of {@link XFactTable}
   */
  @GET
  @Path("/facts/{factName}")
  public JAXBElement<XFactTable> getFactTable(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName)
    throws LensException {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXFactTable(getSvc().getFactTable(sessionid, factName));
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      throw exc;
    }
  }

  /**
   * Create a new fact tabble
   *
   * @param sessionid The sessionid in which user is working
   * @param fact      The {@link XFactTable} representation of the fact table definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if create has failed
   */
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @POST
  @Path("/facts")
  public APIResult createFactTable(@FormDataParam("sessionid") LensSessionHandle sessionid,
    @FormDataParam("fact") XFactTable fact)
    throws LensException {
    checkSessionId(sessionid);
    try {
      log.info("Create fact table");
      getSvc().createFactTable(sessionid, fact);
    } catch (LensException exc) {
      log.error("Exception creating fact:", exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Update fact table definition
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  name of the fact table
   * @param fact      The {@link XFactTable} representation of the updated fact table definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/facts/{factName}")
  public APIResult updateFactTable(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName, XFactTable fact)
    throws LensException {
    checkSessionId(sessionid);
    try {
      getSvc().updateFactTable(sessionid, fact);
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error updating fact {}", factName, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Drop the fact table, specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @param cascade   If true, all the storage tables of the fact will also be dropped
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/facts/{factName}")
  public APIResult dropFactTable(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @DefaultValue("false") @QueryParam("cascade") boolean cascade)
    throws LensException {
    return Entity.FACT.delete(sessionid, factName, cascade);
  }

  /**
   * Get all storages of the fact table in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @return {@link StringList} consisting of all the storage names
   * @throws LensException
   */
  @GET
  @Path("/facts/{factName}/storages")
  public StringList getStoragesOfFact(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName) throws LensException {
    checkSessionId(sessionid);
    try {
      return new StringList(getSvc().getStoragesOfFact(sessionid, factName));
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      throw new WebApplicationException(exc);
    }
  }

  /**
   * Drop all the storage tables of a fact table
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/facts/{factName}/storages")
  public APIResult dropAllStoragesOfFact(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName) {
    checkSessionId(sessionid);
    try {
      getSvc().dropAllStoragesOfFact(sessionid, factName);
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error dropping storages of fact {}", factName, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Add storage to fact table
   *
   * @param sessionid    The sessionid in which user is working
   * @param factName     The fact table name
   * @param storageTable The storage table description
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if add has failed
   */
  @POST
  @Path("/facts/{factName}/storages")
  public APIResult addStorageToFact(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName, XStorageTableElement storageTable) {
    checkSessionId(sessionid);
    try {
      getSvc().addStorageToFact(sessionid, factName, storageTable);
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error adding storage to fact {}", factName, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Drop the storage of a fact, specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @param storage   The storage name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/facts/{factName}/storages/{storage}")
  public APIResult dropStorageFromFact(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage) {
    checkSessionId(sessionid);
    try {
      getSvc().dropStorageOfFact(sessionid, factName, storage);
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error dropping storage of fact {}", factName, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Get the fact storage table
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @param storage   The storage name
   * @return JAXB representation of {@link XStorageTableElement}
   */
  @GET
  @Path("/facts/{factName}/storages/{storage}")
  public JAXBElement<XStorageTableElement> getStorageOfFact(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage) throws LensException {
    return X_CUBE_OBJECT_FACTORY.createXStorageTableElement(getSvc().getStorageOfFact(sessionid, factName, storage));
  }

  /**
   * Get all partitions of the fact table in the specified storage; can be filtered as well.
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @param storage   The storage name
   * @param filter    The filter for partitions, string representation of the filter for ex: x &lt; "xxx" and y &gt;
   *                  "yyy"
   * @return JAXB representation of {@link XPartitionList} containing {@link XPartition} objects
   */
  @GET
  @Path("/facts/{factName}/storages/{storage}/partitions")
  public JAXBElement<XPartitionList> getAllPartitionsOfFactStorageByFilter(
    @QueryParam("sessionid") LensSessionHandle sessionid, @PathParam("factName") String factName,
    @PathParam("storage") String storage,
    @QueryParam("filter") String filter) throws LensException {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY
        .createXPartitionList(getSvc().getAllPartitionsOfFactStorage(sessionid, factName, storage, filter));
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      throw exc;
    }
  }

  /**
   * Drop the partitions in the storage of a fact; can specified filter as well
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @param storage   The storage name
   * @param filter    The filter for partitions, string representation of the filter for ex: x &lt; "xxx" and y &gt;
   *                  "yyy"
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/facts/{factName}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfFactStorageByFilter(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage,
    @QueryParam("filter") String filter) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByFilter(sessionid, factName, storage, filter);
    } catch (LensException exc) {
      log.warn("Got exception while dropping partition.", exc);
      checkTableNotFound(exc, factName);
      return partial(processLensException(exc));
    }
    return success();
  }

  /**
   * Add a new partition for a storage of fact
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  fact table name
   * @param storage   storage name
   * @param partition {@link XPartition} representation of partition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if add has failed
   */
  @POST
  @Path("/facts/{factName}/storages/{storage}/partition")
  public APIResult addPartitionToFactStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage,
    XPartition partition) {
    checkSessionId(sessionid);
    checkNonNullArgs("Partition is null", partition);
    try {
      return successOrPartialOrFailure(getSvc().addPartitionToFactStorage(sessionid, factName, storage, partition), 1);
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error adding partition to storage of fact {}:{}", factName, storage, exc);
      return failure(processLensException(exc));
    }
  }

  /**
   * updates an existing partition for a storage of fact
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  fact table name
   * @param storage   storage name
   * @param partition {@link XPartition} representation of partition.
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state
   * {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/facts/{factName}/storages/{storage}/partition")
  public APIResult updatePartitionOfFactStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage,
    XPartition partition) {
    checkSessionId(sessionid);
    checkNonNullArgs("Partition is null", partition);
    try {
      getSvc().updatePartition(sessionid, factName, storage, partition);
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error adding partition to storage of fact {}:{}", factName, storage, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Batch Add partitions for a storage of fact
   *
   * @param sessionid  The sessionid in which user is working
   * @param factName   fact table name
   * @param storage    storage name
   * @param partitions {@link XPartitionList} representation of partitions
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if add has failed
   */
  @POST
  @Path("/facts/{factName}/storages/{storage}/partitions")
  public APIResult addPartitionsToFactStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage,
    XPartitionList partitions) {
    checkSessionId(sessionid);
    checkNonNullPartitionList(partitions);
    try {
      return successOrPartialOrFailure(getSvc().addPartitionsToFactStorage(sessionid, factName, storage, partitions),
        partitions.getPartition().size());
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error adding partition to storage of fact {}:{}", factName, storage, exc);
      return failure(processLensException(exc));
    }
  }

  /**
   * Batch Update partitions for a storage of fact
   *
   * @param sessionid  The sessionid in which user is working
   * @param factName   fact table name
   * @param storage    storage name
   * @param partitions {@link XPartitionList} representation of partitions
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state
   * {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/facts/{factName}/storages/{storage}/partitions")
  public APIResult updatePartitionsOfFactStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage,
    XPartitionList partitions) {
    checkSessionId(sessionid);
    checkNonNullPartitionList(partitions);
    try {
      getSvc().updatePartitions(sessionid, factName, storage, partitions);
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error adding partition to storage of fact {}:{}", factName, storage, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Drop the partitions in the storage of a fact table, specified by exact values
   *
   * @param sessionid The sessionid in which user is working
   * @param factName  The fact table name
   * @param storage   The storage name
   * @param values    Comma separated values
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/facts/{factName}/storages/{storage}/partition")
  public APIResult dropPartitionOfFactStorageByValues(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName,
    @PathParam("storage") String storage,
    @QueryParam("values") String values) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByValues(sessionid, factName, storage,
        values);

    } catch (LensException e) {
      checkTableNotFound(e, factName);
      log.error("Error dropping partition to storage of fact {}:{}", factName, storage, e);
      return failure(processLensException(e));
    }
    return success();
  }

  /**
   * Get all dimension tables in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @return StringList consisting of all dimension table names
   */
  @GET
  @Path("/dimtables")
  public StringList getAllDims(@QueryParam("sessionid") LensSessionHandle sessionid) throws LensException {
    return Entity.DIMTABLE.getAll(sessionid);
  }

  /**
   * Create a new dimension table
   *
   * @param sessionid      The sessionid in which user is working
   * @param dimensionTable The {@link XDimensionTable} representation of the dimension table definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if create was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if create has failed
   */
  @POST
  @Path("/dimtables")
  public APIResult createDimensionTable(@FormDataParam("sessionid") LensSessionHandle sessionid,
    @FormDataParam("dimensionTable") XDimensionTable dimensionTable) {
    checkSessionId(sessionid);
    try {
      getSvc().createDimensionTable(sessionid, dimensionTable);
    } catch (LensException exc) {
      log.error("Error creating cube dimension table {}", dimensionTable.getTableName(), exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Update dimension table definition
   *
   * @param sessionid      The sessionid in which user is working
   * @param dimensionTable The {@link XDimensionTable} representation of the updated dim table definition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful. {@link APIResult} with
   * state {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/dimtables/{dimTableName}")
  public APIResult updateCubeDimension(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    XDimensionTable dimensionTable) {
    checkSessionId(sessionid);
    try {
      getSvc().updateDimensionTable(sessionid, dimensionTable);
    } catch (LensException exc) {
      checkTableNotFound(exc, dimTableName);
      log.error("Error updating cube dimension table {}", dimTableName, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Drop the dimension table, specified by name
   *
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * @param cascade   if true, all the storage tables of dimension table will also be dropped
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/dimtables/{dimTableName}")
  public APIResult dropDimensionTable(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimension,
    @QueryParam("cascade") boolean cascade) {
    return Entity.DIMTABLE.delete(sessionid, dimension, cascade);
  }

  /**
   * Get the dimension table specified by name
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName The cube name
   * @return JAXB representation of {@link XDimensionTable}
   */
  @GET
  @Path("/dimtables/{dimTableName}")
  public JAXBElement<XDimensionTable> getDimensionTable(
    @QueryParam("sessionid") LensSessionHandle sessionid, @PathParam("dimTableName") String dimTableName)
    throws LensException {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXDimensionTable(getSvc().getDimensionTable(sessionid, dimTableName));
    } catch (LensException exc) {
      checkTableNotFound(exc, dimTableName);
      throw exc;
    }
  }

  /**
   * Get all storages of the dimension table in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * @return StringList consisting of all the storage names
   * @throws LensException
   */
  @GET
  @Path("/dimtables/{dimTableName}/storages")
  public StringList getDimensionStorages(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimension)
    throws LensException {
    checkSessionId(sessionid);
    return new StringList(getSvc().getDimTableStorages(sessionid, dimension));
  }

  /**
   * Add storage to dimension table
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName The dimension table name
   * @param storageTbl   The Storage table description
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if add has failed
   */
  @POST
  @Path("/dimtables/{dimTableName}/storages")
  public APIResult createDimensionStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    XStorageTableElement storageTbl) {
    checkSessionId(sessionid);
    try {
      getSvc().addDimTableStorage(sessionid, dimTableName, storageTbl);
    } catch (LensException e) {
      checkTableNotFound(e, dimTableName);
      log.error("Error creating dimension table storage {}:{}", dimTableName, storageTbl.getStorageName(), e);
      return failure(processLensException(e));
    }
    return success();
  }

  /**
   * Get the dim storage table
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName The fact table name
   * @param storage      The storage name
   * @return JAXB representation of {@link XStorageTableElement}
   */
  @GET
  @Path("/dimtables/{dimTableName}/storages/{storage}")
  public JAXBElement<XStorageTableElement> getStorageOfDim(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage) throws LensException {
    checkSessionId(sessionid);
    return X_CUBE_OBJECT_FACTORY.createXStorageTableElement(getSvc().getStorageOfDim(sessionid, dimTableName, storage));
  }

  /**
   * Drop all the storage tables of a dimension table
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName The dimension table name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/dimtables/{dimTableName}/storages")
  public APIResult dropAllStoragesOfDim(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName) {
    checkSessionId(sessionid);
    try {
      getSvc().dropAllStoragesOfDimTable(sessionid, dimTableName);
    } catch (LensException exc) {
      checkTableNotFound(exc, dimTableName);
      log.error("Error dropping storages of dimension table {}", dimTableName, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Drop the storage of a dimension table, specified by name
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName The dimension table name
   * @param storage      The storage name
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/dimtables/{dimTableName}/storages/{storage}")
  public APIResult dropStorageOfDim(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage) {
    checkSessionId(sessionid);
    try {
      getSvc().dropStorageOfDimTable(sessionid, dimTableName, storage);
    } catch (LensException exc) {
      checkTableNotFound(exc, dimTableName);
      log.error("Error dropping storage of dimension table {}:{}", dimTableName, storage, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Get all partition of the dimension table in the specified storage; can be filtered
   *
   * @param sessionid The sessionid in which user is working
   * @param dimension The dimension table name
   * @param storage   The storage name
   * @param filter    The filter for partitions, string representation of the filter for ex: x &lt; "xxx" and y &gt;
   *                  "yyy"
   * @return JAXB representation of {@link XPartitionList} containing {@link XPartition} objects
   */
  @GET
  @Path("/dimtables/{dimTableName}/storages/{storage}/partitions")
  public JAXBElement<XPartitionList> getAllPartitionsOfDimStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimension,
    @PathParam("storage") String storage,
    @QueryParam("filter") String filter)
    throws LensException {
    checkSessionId(sessionid);
    return X_CUBE_OBJECT_FACTORY
      .createXPartitionList(getSvc().getAllPartitionsOfDimTableStorage(sessionid, dimension, storage, filter));
  }

  /**
   * Drop the partitions in the storage of a dimension table; can specified filter as well
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName The dimension table name
   * @param storage      The storage name
   * @param filter       The filter for partitions, string representation of the filter for ex: x &lt; 'xxx' and y &gt;
   *                     'yyy'
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/dimtables/{dimTableName}/storages/{storage}/partitions")
  public APIResult dropPartitionsOfDimStorageByFilter(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage,
    @QueryParam("filter") String filter) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByFilter(sessionid, dimTableName, storage, filter);
    } catch (LensException exc) {
      log.error("Error dropping partition on storage of dimension table {}:{}", dimTableName, storage, exc);
      return partial(processLensException(exc));
    }
    return success();
  }

  /**
   * Drop the partitions in the storage of a dimension table, specified by exact values
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName The dimension table name
   * @param storage      The storage name
   * @param values       Comma separated values
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if drop was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if drop has failed
   */
  @DELETE
  @Path("/dimtables/{dimTableName}/storages/{storage}/partition")
  public APIResult dropPartitionsOfDimStorageByValue(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage,
    @QueryParam("values") String values) {
    checkSessionId(sessionid);
    try {
      getSvc().dropPartitionFromStorageByValues(sessionid, dimTableName, storage,
        values);
    } catch (LensException exc) {
      log.error("Error dropping partitions on storage of dimension table {}:{}", dimTableName, storage, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Add a new partition for a storage of dimension
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName dimension table name
   * @param storage      storage name
   * @param partition    {@link XPartition} representation of partition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if add has failed
   */
  @POST
  @Path("/dimtables/{dimTableName}/storages/{storage}/partition")
  public APIResult addPartitionToDimStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage,
    XPartition partition) {
    checkSessionId(sessionid);
    checkNonNullArgs("Partition is null", partition);
    try {
      return successOrPartialOrFailure(getSvc().addPartitionToDimStorage(sessionid, dimTableName, storage, partition),
        1);
    } catch (LensException exc) {
      log.error("Error adding partition to storage of dimension table {}:{}", dimTableName, storage, exc);
      return failure(processLensException(exc));
    }
  }

  /**
   * Updates an existing partition for a storage of dimension
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName dimension table name
   * @param storage      storage name
   * @param partition    {@link XPartition} representation of partition
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state
   * {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/dimtables/{dimTableName}/storages/{storage}/partition")
  public APIResult updatePartitionOfDimStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage,
    XPartition partition) {
    checkSessionId(sessionid);
    checkNonNullArgs("Partition is null", partition);
    try {
      getSvc().updatePartition(sessionid, dimTableName, storage, partition);
    } catch (LensException exc) {
      log.error("Error adding partition to storage of dimension table {}:{}", dimTableName, storage, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Add new partitions for a storage of dimension
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName dimension table name
   * @param storage      storage name
   * @param partitions   {@link XPartitionList} representation of list of partitions
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if add was successful. {@link APIResult} with state
   * {@link Status#FAILED}, if add has failed
   */
  @POST
  @Path("/dimtables/{dimTableName}/storages/{storage}/partitions")
  public APIResult addPartitionsToDimStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage,
    XPartitionList partitions) {
    checkSessionId(sessionid);
    checkNonNullPartitionList(partitions);
    try {
      return successOrPartialOrFailure(getSvc().addPartitionsToDimStorage(sessionid, dimTableName, storage, partitions),
        partitions.getPartition().size());
    } catch (LensException exc) {
      log.error("Error adding partition to storage of dimension table {}:{}", dimTableName, storage, exc);
      return failure(processLensException(exc));
    }
  }

  /**
   * Add new partitions for a storage of dimension
   *
   * @param sessionid    The sessionid in which user is working
   * @param dimTableName dimension table name
   * @param storage      storage name
   * @param partitions   {@link XPartitionList} representation of list of partitions
   * @return {@link APIResult} with state {@link Status#SUCCEEDED}, if update was successful.
   * {@link APIResult} with state
   * {@link Status#FAILED}, if update has failed
   */
  @PUT
  @Path("/dimtables/{dimTableName}/storages/{storage}/partitions")
  public APIResult updatePartitionsOfDimStorage(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("dimTableName") String dimTableName,
    @PathParam("storage") String storage,
    XPartitionList partitions) {
    checkSessionId(sessionid);
    checkNonNullPartitionList(partitions);
    try {
      getSvc().updatePartitions(sessionid, dimTableName, storage, partitions);
    } catch (LensException exc) {
      log.error("Error adding partition to storage of dimension table {}:{}", dimTableName, storage, exc);
      return failure(processLensException(exc));
    }
    return success();
  }

  /**
   * Get flattened list of columns reachable from a cube or a dimension
   *
   * @param sessionid  session id
   * @param tableName  name of the table
   * @param addChains whether columns accessed via chains should also be returned
   * @return list of measures, expressions or dimension attributes
   */
  @GET
  @Path("flattened/{tableName}")
  public JAXBElement<XFlattenedColumns> getFlattenedColumns(
    @QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("tableName") String tableName, @QueryParam("add_chains") @DefaultValue("true") boolean addChains) {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXFlattenedColumns(
        getSvc().getFlattenedColumns(sessionid, tableName, addChains));
    } catch (LensException exc) {
      throw new WebApplicationException(exc);
    }
  }

  /**
   * Get all chains that belong to a table(cube or dimension) in the metastore
   *
   * @param sessionid The sessionid in which user is working
   * @param tableName name of the table. can be either cube or dimension
   * @return {@link XJoinChains} object
   */
  @GET
  @Path("/chains/{tableName}")
  public JAXBElement<XJoinChains> getAllJoinChains(
    @QueryParam("sessionid") LensSessionHandle sessionid, @PathParam("tableName") String tableName)
    throws LensException {
    checkSessionId(sessionid);
    try {
      return X_CUBE_OBJECT_FACTORY.createXJoinChains(getSvc().getAllJoinChains(sessionid, tableName));
    } catch (LensException exc) {
      checkTableNotFound(exc, tableName);
      throw exc;
    }
  }

  /**
   * Get the latest available date upto which data is available for the base cubes, for the time dimension.
   *
   * @param sessionid     The sessionid in which user is working
   * @param timeDimension time dimension name
   * @param cubeName      name of the base cube
   * @return DateTime object which has Date in it.
   */
  @GET
  @Path("/cubes/{cubeName}/latestdate")
  public DateTime getLatestDateOfCube(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("cubeName") String cubeName, @QueryParam("timeDimension") String timeDimension) throws LensException,
    HiveException {
    checkSessionId(sessionid);
    try {
      return new DateTime(getSvc().getLatestDateOfCube(sessionid, cubeName, timeDimension));
    } catch (LensException exc) {
      throw new WebApplicationException(exc);
    }
  }

  /**
   * Get the partition timelines.
   *
   * @param sessionid     The sessionid in which user is working
   * @param factName      name of the fact
   * @param storage       storage Name
   * @param updatePeriod  update period
   * @param timeDimension time dimension name
   * @return List os partition timelines.
   */
  @GET
  @Path("/facts/{factName}/timelines")
  public StringList getPartitionTimelines(@QueryParam("sessionid") LensSessionHandle sessionid,
    @PathParam("factName") String factName, @QueryParam("storage") String storage,
    @QueryParam("updatePeriod") String updatePeriod, @QueryParam("timeDimension") String timeDimension)
    throws LensException, HiveException {
    checkSessionId(sessionid);
    try {
      return new StringList(getSvc().getPartitionTimelines(sessionid, factName, storage,
        updatePeriod, timeDimension));
    } catch (LensException exc) {
      checkTableNotFound(exc, factName);
      log.error("Error finding partition timelines for fact: {}", factName, exc);
      throw exc;
    }
  }
}
