package com.inmobi.grill.metastore.service;

import com.inmobi.grill.metastore.model.Database;
import com.sun.jersey.spi.resource.Singleton;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class MetastoreResource {
  public static final Logger LOG = LogManager.getLogger(MetastoreResource.class);

  /*Cube API:
  <grill-url>/metastore/database/
    - GET the current database
  - PUT set the current database
  - POST Not used
  - DELETE drop the database
  */

  @GET @Path("database")
  public Database getDatabase() {
    Database db = new Database();
    db.setName(CubeMetastoreServiceImpl.getInstance().getCurrentDatabase());
    return db;
  }

  @PUT @Path("database/{dbname}")
  public String setDatabase(@PathParam("dbname") String dbName) {
    return dbName;
  }

  @DELETE @Path("database/{dbname}")
  public void dropDatabase(@PathParam("dbname") String dbName, @QueryParam("cascade") boolean cascade) {
  }


  /*<grill-url>/metastore/cubes/
    - GET - get all cubes
    - POST - not used
  - PUT - Not used
  -DELETE - Drop all the cubes
  */
  @GET @Path("cubes")
  public String getAllCubes() {
    return "All cubes";
  }

  @DELETE @Path("cubes")
  public String deleteAllCubes() {
    return "delete all cubes";
  }

  /*
  <grill-url>/metastore/cubes/cubename
  - GET - Get the cube
  -  PUT - Update the cube
  - DELETE - drop the cube
  - POST - Create new cube
  */

  @GET @Path("/cubes/{cubename}")
  @Produces("text/plain")
  public void getCube(@PathParam("cubename") String cubeName) {
  }

  @POST @Path("/cubes/{cubename}")
  @Produces("text/plain")
  public void createNewCube(@PathParam("cubename") String cubeName) {
  }

  @PUT @Path("/cubes/{cubename}")
  @Produces("text/plain")
  public void updateCube(@PathParam("cubename") String cubeName) {
  }

  @DELETE @Path("/cubes/{cubename}")
  @Produces("text/plain")
  public void deleteCube(@PathParam("cubename") String cubeName, @QueryParam("cascade") boolean cascade) {
    LOG.info("Delete cube " + cubeName + " cascade? " + cascade);
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
