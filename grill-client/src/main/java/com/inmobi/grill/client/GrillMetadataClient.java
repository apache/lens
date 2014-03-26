package com.inmobi.grill.client;

import com.google.common.base.Joiner;
import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.api.metastore.*;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;
import java.util.List;

public class GrillMetadataClient {

  private final GrillConnection connection;
  private final GrillConnectionParams params;
  private final ObjectFactory objFact;

  public GrillMetadataClient(GrillConnection connection) {
    this.connection = connection;
    this.params = connection.getGrillConnectionParams();
    objFact = new ObjectFactory();

  }

  private WebTarget getMetastoreWebTarget(Client client) {
    return client.target(params.getBaseConnectionUrl()).path(
        params.getMetastoreResourcePath());
  }


  private WebTarget getMetastoreWebTarget() {
    Client client = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
    return getMetastoreWebTarget(client);
  }

  public List<String> getAlldatabases() {
    WebTarget target = getMetastoreWebTarget();
    StringList databases = target.path("databases")
        .queryParam("sessionid", connection.getSessionHandle())
        .request().get(StringList.class);
    return databases.getElements();
  }

  public String getCurrentDatabase() {
    WebTarget target = getMetastoreWebTarget();
    String database = target.path("databases").path("current")
        .queryParam("sessionid", connection.getSessionHandle())
        .request().get(String.class);
    return database;
  }


  public APIResult setDatabase(String database) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("databases").path("current")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML_TYPE)
        .put(Entity.xml(database), APIResult.class);
    return result;
  }

  public APIResult createDatabase(String database, boolean ignoreIfExists) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("databases")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("ignoreifexist", ignoreIfExists)
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(database), APIResult.class);
    return result;
  }

  public APIResult createDatabase(String database) {
    return createDatabase(database, false);
  }

  public APIResult dropDatabase(String database, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("databases").path(database)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("cascade", cascade)
        .request().delete(APIResult.class);
    return result;
  }

  public APIResult dropDatabase(String database) {
    return dropDatabase(database, false);
  }

  public List<String> getAllCubes() {
    WebTarget target = getMetastoreWebTarget();
    StringList cubes = target.path("cubes")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).get(StringList.class);
    return cubes.getElements();
  }

  public APIResult dropAllCubes() {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("cubes")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).delete(APIResult.class);
    return result;
  }

  public APIResult createCube(XCube cube) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("cubes")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXCube(cube)), APIResult.class);
    return result;
  }

  public APIResult updateCube(String cubeName, XCube cube) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("cubes").path(cubeName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(objFact.createXCube(cube)), APIResult.class);
    return result;
  }

  public XCube getCube(String cubeName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XCube> cube = target.path("cubes").path(cubeName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XCube>>() {
        });
    return cube.getValue();
  }

  public APIResult dropCube(String cubeName) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("cubes").path(cubeName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).delete(APIResult.class);
    return result;
  }

  public List<String> getAllStorages() {
    WebTarget target = getMetastoreWebTarget();
    StringList storages = target.path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request().get(StringList.class);
    return storages.getElements();
  }


  public APIResult createNewStorage(XStorage storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXStorage(storage)), APIResult.class);
    return result;
  }

  public APIResult dropAllStorages() {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult updateStorage(String storageName, XStorage storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("storages").path(storageName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(objFact.createXStorage(storage)), APIResult.class);
    return result;
  }

  public XStorage getStorage(String storageName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XStorage> result = target.path("storages").path(storageName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<XStorage>>() {
        });
    return result.getValue();
  }

  public APIResult deleteStorage(String storageName) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("storages").path(storageName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public List<FactTable> getAllFactTables(String cubeName) {
    WebTarget target = getMetastoreWebTarget();
    List<FactTable> factTables = target.path("cubes").path(cubeName).path("facts")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<List<FactTable>>() {
        });
    return factTables;
  }

  public List<String> getAllFactTables() {
    WebTarget target = getMetastoreWebTarget();
    StringList factTables = target.path("facts")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(StringList.class);
    return factTables.getElements();
  }

  public APIResult deleteAllFactTables(boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("cascade", cascade)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }


  public FactTable getFactTable(String factTableName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<FactTable> table = target.path("facts").path(factTableName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<FactTable>>() {
        });
    return table.getValue();
  }

  public APIResult createFactTable(FactTable f, XStorageTables tables) {
    WebTarget target = getMetastoreWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid")
        .build(), this.connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("fact").fileName("fact").build(),
        objFact.createFactTable(f), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("storagetables").fileName("storagetables").build(),
        objFact.createXStorageTables(tables), MediaType.APPLICATION_XML_TYPE));
    APIResult result = target.path("facts")
        .request(MediaType.APPLICATION_XML_TYPE)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
            APIResult.class);
    return result;
  }


  public APIResult updateFactTable(String factName, FactTable table) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML_TYPE)
        .put(Entity.xml(objFact.createFactTable(table)), APIResult.class);
    return result;
  }

  public APIResult deleteFactTable(String factName, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("cascade", cascade)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult deleteFactTable(String factName) {
    return deleteFactTable(factName, false);
  }

  public List<String> getAllStoragesOfFactTable(String factName) {
    WebTarget target = getMetastoreWebTarget();
    StringList storageList = target.path("facts").path(factName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(StringList.class);
    return storageList.getElements();
  }

  public APIResult dropAllStoragesOfFactTable(String factName) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult addStorageToFactTable(String factname, XStorageTableElement storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factname).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXStorageTableElement(storage)), APIResult.class);
    return result;
  }

  public APIResult dropStorageFromFactTable(String factName, String storageName) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName).path("storages").path(storageName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public XStorageTableElement getStorageOfFactTable(String factName, String storageName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XStorageTableElement> element = target.path("facts")
        .path(factName).path("storages").path(storageName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<XStorageTableElement>>() {
        });
    return element.getValue();
  }

  public List<XPartition> getPartitionsOfFactTable(String factName, String storage, String filter) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<PartitionList> elements = target.path("facts").path(factName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("filter", filter)
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<PartitionList>>() {
        });
    return elements.getValue().getXPartition();
  }

  public List<XPartition> getPartitionsOfFactTable(String factName, String storage) {
    return getPartitionsOfFactTable(factName, storage, "");
  }

  public APIResult dropPartitionsOfFactTable(String factName, String storage, String filter) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("filter", filter)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult dropPartitionsOfFactTable(String factName, String storage) {
    return dropPartitionsOfFactTable(factName, storage, "");
  }

  public APIResult dropPartitionsOfFactTable(String factName, String storage,
                                  List<String> partitions) {
    String values = Joiner.on(",").skipNulls().join(partitions);

    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("values", values)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }


  public List<String> getAllDimensionTables() {
    WebTarget target = getMetastoreWebTarget();
    StringList dimensions = target.path("dimensions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(StringList.class);
    return dimensions.getElements();
  }

  public APIResult createDimensionTable(DimensionTable table,
                                        XStorageTables storageTables) {
    WebTarget target = getMetastoreWebTarget();

    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("dimtable").fileName("dimtable").build(),
        objFact.createDimensionTable(table), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("storagetables").fileName("storagetables").build(),
        objFact.createXStorageTables(storageTables), MediaType.APPLICATION_XML_TYPE));

    APIResult result = target.path("dimensions")
        .request(MediaType.APPLICATION_XML)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }

  public APIResult updateDimensionTable(DimensionTable table) {
    String dimName = table.getName();
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(objFact.createDimensionTable(table)), APIResult.class);
    return result;
  }

  public APIResult dropDimensionTable(String table, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(table)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("cascade", cascade)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult dropDimensionTable(String table) {
    return dropDimensionTable(table, false);
  }

  public DimensionTable getDimensionTable(String table) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<DimensionTable> result = target.path("dimensions").path(table)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<DimensionTable>>() {
        });
    return result.getValue();
  }


  public List<String> getDimensionStorage(String dimName) {
    WebTarget target = getMetastoreWebTarget();
    StringList list = target.path("dimensions").path(dimName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(StringList.class);
    return list.getElements();
  }

  public APIResult addStorageToDimension(String dimName, XStorageTableElement table) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXStorageTableElement(table)), APIResult.class);
    return result;
  }

  public XStorageTableElement getStorageOfDimension(String dimName, String storage) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XStorageTableElement> result = target.path("dimensions").path(dimName)
        .path("storages").path(storage)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<XStorageTableElement>>() {
        });
    return result.getValue();
  }

  public APIResult dropAllStoragesOfDimension(String dimName) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }


  public APIResult dropStoragesOfDimension(String dimName, String storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
        .path("storages").path(storage)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public PartitionList getAllPartitionsOfDimension(String dimName, String storage,
                                                   String filter) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<PartitionList> partList = target.path("dimensions").path(dimName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("filter", filter)
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<PartitionList>>() {
        });
    return partList.getValue();
  }

  public PartitionList getAllPartitionsOfDimension(String dimName, String storage) {
    return getAllPartitionsOfDimension(dimName, storage, "");
  }

  public APIResult dropAllPartitionsOfDimension(String dimName, String storage,
                                                String filter) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("filter", filter)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult dropAllPartitionsOfDimension(String dimName, String storage) {
    return dropAllPartitionsOfDimension(dimName, storage, "");
  }

  public APIResult dropAllPartitionsOfDimension(String dimName, String storage,
                                                List<String> vals) {
    String values = Joiner.on(",").skipNulls().join(vals);
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("values", values)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult addPartitionToDimension(String dimName, String storage,
                                           XPartition partition) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXPartition(partition)), APIResult.class);
    return result;
  }


}
