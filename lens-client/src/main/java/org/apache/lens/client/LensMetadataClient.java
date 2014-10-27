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
package org.apache.lens.client;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.apache.lens.api.metastore.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lens.api.APIResult;
import org.apache.lens.api.StringList;
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
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class LensMetadataClient {

  private static final Log LOG = LogFactory.getLog(LensMetadataClient.class);

  private final LensConnection connection;
  private final LensConnectionParams params;
  private final ObjectFactory objFact;

  public LensMetadataClient(LensConnection connection) {
    this.connection = connection;
    this.params = connection.getLensConnectionParams();
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
        .queryParam("ignoreIfExisting", ignoreIfExists)
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

  public List<String> getAllNativeTables() {
    WebTarget target = getMetastoreWebTarget();
    StringList nativetables = target.path("nativetables")
        .queryParam("sessionid", connection.getSessionHandle())
        .request().get(StringList.class);
    return nativetables.getElements();
  }

  public NativeTable getNativeTable(String tblName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<NativeTable> htable = target.path("nativetables").path(tblName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<NativeTable>>() {
        });
    return htable.getValue();
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

  public APIResult createCube(String cubeSpec) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("cubes")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(getContent(cubeSpec)), APIResult.class);
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

  public APIResult updateCube(String cubeName, String cubespec) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("cubes").path(cubeName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(getContent(cubespec)), APIResult.class);
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

  public List<String> getAllDimensions() {
    WebTarget target = getMetastoreWebTarget();
    StringList dimensions = target.path("dimensions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).get(StringList.class);
    return dimensions.getElements();
  }

  public APIResult dropAllDimensions() {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).delete(APIResult.class);
    return result;
  }

  public APIResult createDimension(XDimension dimension) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXDimension(dimension)), APIResult.class);
    return result;
  }

  public APIResult createDimension(String dimSpec) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(getContent(dimSpec)), APIResult.class);
    return result;
  }

  public APIResult updateDimension(String dimName, XDimension dimension) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(objFact.createXDimension(dimension)), APIResult.class);
    return result;
  }

  public APIResult updateDimension(String dimName, String dimSpec) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(getContent(dimSpec)), APIResult.class);
    return result;
  }

  public XDimension getDimension(String dimName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XDimension> dim = target.path("dimensions").path(dimName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XDimension>>() {
        });
    return dim.getValue();
  }

  public APIResult dropDimension(String dimName) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimensions").path(dimName)
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


  public APIResult createNewStorage(String storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(getContent(storage)), APIResult.class);
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

  public APIResult updateStorage(String storageName, String storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("storages").path(storageName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(getContent(storage)), APIResult.class);
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

  public APIResult dropStorage(String storageName) {
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
        FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
        objFact.createXStorageTables(tables), MediaType.APPLICATION_XML_TYPE));
    APIResult result = target.path("facts")
        .request(MediaType.APPLICATION_XML_TYPE)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
            APIResult.class);
    return result;
  }

  public APIResult createFactTable(String factSpec,
      String storageSpecPath) {
    WebTarget target = getMetastoreWebTarget();
    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid")
        .build(), this.connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("fact").fileName("fact").build(),
        getContent(factSpec), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
        getContent(storageSpecPath), MediaType.APPLICATION_XML_TYPE));
    APIResult result = target.path("facts")
        .request(MediaType.APPLICATION_XML_TYPE)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE),
            APIResult.class);
    return result;
  }

  private String getContent(String path) {
    try {
      List<String> content = Files.readLines(new File(path),
          Charset.defaultCharset());
      return Joiner.on("\n").skipNulls().join(content);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }


  public APIResult updateFactTable(String factName, FactTable table) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML_TYPE)
        .put(Entity.xml(objFact.createFactTable(table)), APIResult.class);
    return result;
  }

  public APIResult updateFactTable(String factName, String table) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML_TYPE)
        .put(Entity.xml(getContent(table)), APIResult.class);
    return result;
  }

  public APIResult dropFactTable(String factName, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("cascade", cascade)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult dropFactTable(String factName) {
    return dropFactTable(factName, false);
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

  public APIResult addStorageToFactTable(String factname, String storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(factname).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(getContent(storage)), APIResult.class);
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
    StringList dimtables = target.path("dimtables")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(StringList.class);
    return dimtables.getElements();
  }

  public APIResult createDimensionTable(DimensionTable table,
      XStorageTables storageTables) {
    WebTarget target = getMetastoreWebTarget();

    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("dimensionTable").fileName("dimtable").build(),
        objFact.createDimensionTable(table), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
        objFact.createXStorageTables(storageTables), MediaType.APPLICATION_XML_TYPE));

    APIResult result = target.path("dimtables")
        .request(MediaType.APPLICATION_XML)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }

  public APIResult createDimensionTable(String table,
      String storageTables) {
    WebTarget target = getMetastoreWebTarget();

    FormDataMultiPart mp = new FormDataMultiPart();
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        this.connection.getSessionHandle(), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("dimensionTable").fileName("dimtable").build(),
        getContent(table), MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("storageTables").fileName("storagetables").build(),
        getContent(storageTables), MediaType.APPLICATION_XML_TYPE));

    APIResult result = target.path("dimtables")
        .request(MediaType.APPLICATION_XML)
        .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), APIResult.class);
    return result;
  }


  public APIResult updateDimensionTable(DimensionTable table) {
    String dimTableName = table.getTableName();
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTableName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(objFact.createDimensionTable(table)), APIResult.class);
    return result;
  }

  public APIResult updateDimensionTable(String dimTblName, String dimSpec) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .put(Entity.xml(getContent(dimSpec)), APIResult.class);
    return result;
  }

  public APIResult dropDimensionTable(String table, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(table)
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
    JAXBElement<DimensionTable> result = target.path("dimtables").path(table)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<DimensionTable>>() {
        });
    return result.getValue();
  }


  public List<String> getAllStoragesOfDimTable(String dimTblName) {
    WebTarget target = getMetastoreWebTarget();
    StringList list = target.path("dimtables").path(dimTblName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(StringList.class);
    return list.getElements();
  }

  public APIResult addStorageToDimTable(String dimTblName, XStorageTableElement table) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXStorageTableElement(table)), APIResult.class);
    return result;
  }

  public APIResult addStorageToDimTable(String dimTblName, String table) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(getContent(table)), APIResult.class);
    return result;
  }

  public XStorageTableElement getStorageOfDimensionTable(String dimTblName, String storage) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XStorageTableElement> result = target.path("dimtables").path(dimTblName)
        .path("storages").path(storage)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<XStorageTableElement>>() {
        });
    return result.getValue();
  }

  public APIResult dropAllStoragesOfDimension(String dimTblName) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName).path("storages")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }


  public APIResult dropStoragesOfDimensionTable(String dimTblName, String storage) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName)
        .path("storages").path(storage)
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public List<XPartition> getAllPartitionsOfDimensionTable(String dimTblName, String storage,
      String filter) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<PartitionList> partList = target.path("dimtables").path(dimTblName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("filter", filter)
        .request(MediaType.APPLICATION_XML)
        .get(new GenericType<JAXBElement<PartitionList>>() {
        });
    return partList.getValue().getXPartition();
  }

  public List<XPartition> getAllPartitionsOfDimensionTable(String dimTblName, String storage) {
    return getAllPartitionsOfDimensionTable(dimTblName, storage, "");
  }

  public APIResult dropAllPartitionsOfDimensionTable(String dimTblName, String storage,
      String filter) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("filter", filter)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult dropAllPartitionsOfDimensionTable(String dimTblName, String storage) {
    return dropAllPartitionsOfDimensionTable(dimTblName, storage, "");
  }

  public APIResult dropAllPartitionsOfDimensionTable(String dimTblName, String storage,
      List<String> vals) {
    String values = Joiner.on(",").skipNulls().join(vals);
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .queryParam("values", values)
        .request(MediaType.APPLICATION_XML)
        .delete(APIResult.class);
    return result;
  }

  public APIResult addPartitionToDimensionTable(String dimTblName, String storage,
      XPartition partition) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXPartition(partition)), APIResult.class);
    return result;
  }

  public APIResult addPartitionToDimensionTable(String dimTblName, String storage,
      String partition) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("dimtables").path(dimTblName)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(getContent(partition)), APIResult.class);
    return result;
  }


  public APIResult addPartitionToFactTable(String fact, String storage,
      XPartition partition) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(fact)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(objFact.createXPartition(partition)), APIResult.class);
    return result;
  }

  public APIResult addPartitionToFactTable(String fact, String storage,
      String partition) {
    WebTarget target = getMetastoreWebTarget();
    APIResult result = target.path("facts").path(fact)
        .path("storages").path(storage).path("partitions")
        .queryParam("sessionid", this.connection.getSessionHandle())
        .request(MediaType.APPLICATION_XML)
        .post(Entity.xml(getContent(partition)), APIResult.class);
    return result;
  }

}
