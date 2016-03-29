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

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.ws.rs.client.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.*;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.DateTime;
import org.apache.lens.api.StringList;
import org.apache.lens.api.jaxb.LensJAXBContext;
import org.apache.lens.api.metastore.*;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LensMetadataClient {

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
    return getMetastoreWebTarget(connection.buildClient());
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
    return target.path("databases").path("current")
      .queryParam("sessionid", connection.getSessionHandle())
      .request().get(String.class);
  }


  public APIResult setDatabase(String database) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("databases").path("current")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML_TYPE)
      .put(Entity.xml(database), APIResult.class);
  }

  public APIResult createDatabase(String database, boolean ignoreIfExists) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("databases")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("ignoreIfExisting", ignoreIfExists)
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(database), APIResult.class);
  }

  public APIResult createDatabase(String database) {
    return createDatabase(database, false);
  }

  public APIResult dropDatabase(String database, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("databases").path(database)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("cascade", cascade)
      .request().delete(APIResult.class);
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

  public XNativeTable getNativeTable(String tblName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XNativeTable> htable = target.path("nativetables").path(tblName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XNativeTable>>() {
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
    return target.path("cubes")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML).delete(APIResult.class);
  }

  public APIResult createCube(XCube cube) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("cubes")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XCube>>(objFact.createXCube(cube)){}), APIResult.class);
  }

  private <T> T readFromXML(String filename) throws JAXBException, IOException {
    return LensJAXBContext.unmarshallFromFile(filename);
  }

  public APIResult createCube(String cubeSpec) {
    try {
      return createCube(this.<XCube>readFromXML(cubeSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updateCube(String cubeName, XCube cube) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("cubes").path(cubeName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XCube>>(objFact.createXCube(cube)){}), APIResult.class);
  }

  public APIResult updateCube(String cubeName, String cubeSpec) {
    try {
      return updateCube(cubeName, this.<XCube>readFromXML(cubeSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public XCube getCube(String cubeName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XCube> cube = target.path("cubes").path(cubeName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XCube>>() {
      });
    return cube.getValue();
  }

  public XFlattenedColumns getQueryableFields(String tableName, boolean flattened) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XFlattenedColumns> fields = target.path("flattened").path(tableName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("add_chains", flattened)
      .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XFlattenedColumns>>() {
      });
    return fields.getValue();
  }

  public XJoinChains getJoinChains(String tableName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XJoinChains> fields = target.path("chains").path(tableName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML).get(new GenericType<JAXBElement<XJoinChains>>() {
      });
    return fields.getValue();
  }

  public APIResult dropCube(String cubeName) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("cubes").path(cubeName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML).delete(APIResult.class);
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
    return target.path("dimensions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML).delete(APIResult.class);
  }

  public APIResult createDimension(XDimension dimension) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimensions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XDimension>>(objFact.createXDimension(dimension)){}),
        APIResult.class);
  }

  public APIResult createDimension(String dimSpec) {
    try {
      return createDimension(this.<XDimension>readFromXML(dimSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updateDimension(String dimName, XDimension dimension) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimensions").path(dimName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XDimension>>(objFact.createXDimension(dimension)){}),
        APIResult.class);
  }

  public APIResult updateDimension(String dimName, String dimSpec) {
    try {
      return updateDimension(dimName, this.<XDimension>readFromXML(dimSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
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
    return target.path("dimensions").path(dimName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML).delete(APIResult.class);
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
    return target.path("storages")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XStorage>>(objFact.createXStorage(storage)){}), APIResult.class);
  }


  public APIResult createNewStorage(String storage) {
    try {
      return createNewStorage(this.<XStorage>readFromXML(storage));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult dropAllStorages() {
    WebTarget target = getMetastoreWebTarget();
    return target.path("storages")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public APIResult updateStorage(String storageName, XStorage storage) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("storages").path(storageName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XStorage>>(objFact.createXStorage(storage)){}), APIResult.class);
  }

  public APIResult updateStorage(String storageName, String storage) {
    try {
      return updateStorage(storageName, this.<XStorage>readFromXML(storage));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
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
    return target.path("storages").path(storageName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public List<String> getAllFactTables(String cubeName) {
    if (cubeName == null) {
      return getAllFactTables();
    }
    WebTarget target = getMetastoreWebTarget();
    StringList factTables;
    factTables = target.path("cubes").path(cubeName).path("facts")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(StringList.class);
    return factTables.getElements();
  }

  public List<String> getAllCubeSegmentations(String cubeName) {
    if (cubeName == null) {
      return getAllCubeSegmentations();
    }
    WebTarget target = getMetastoreWebTarget();
    StringList cubeSegmentations;
    cubeSegmentations = target.path("cubes").path(cubeName).path("cubesegmentations")
            .queryParam("sessionid", this.connection.getSessionHandle())
            .request(MediaType.APPLICATION_XML)
            .get(StringList.class);
    return cubeSegmentations.getElements();
  }


  public List<String> getAllFactTables() {
    WebTarget target = getMetastoreWebTarget();
    StringList factTables;
    factTables = target.path("facts")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(StringList.class);

    return factTables.getElements();
  }

  public List<String> getAllCubeSegmentations() {
    WebTarget target = getMetastoreWebTarget();
    StringList cubeSegmentations;
    cubeSegmentations = target.path("cubesegmentations")
            .queryParam("sessionid", this.connection.getSessionHandle())
            .request(MediaType.APPLICATION_XML)
            .get(StringList.class);

    return cubeSegmentations.getElements();
  }


  public APIResult deleteAllFactTables(boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("cascade", cascade)
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }


  public APIResult deleteAllCubeSegmentations() {
    WebTarget target = getMetastoreWebTarget();
    return target.path("cubesegmentations")
            .queryParam("sessionid", this.connection.getSessionHandle())
            .request(MediaType.APPLICATION_XML)
            .delete(APIResult.class);
  }

  public XFactTable getFactTable(String factTableName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XFactTable> table = target.path("facts").path(factTableName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(new GenericType<JAXBElement<XFactTable>>() {
      });
    return table.getValue();
  }

  public XCubeSegmentation getCubeSegmentation(String segName) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XCubeSegmentation> seg = target.path("cubesegmentations").path(segName)
            .queryParam("sessionid", this.connection.getSessionHandle())
            .request(MediaType.APPLICATION_XML)
            .get(new GenericType<JAXBElement<XCubeSegmentation>>() {
            });
    return seg.getValue();
  }

  public APIResult createFactTable(XFactTable f) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XFactTable>>(objFact.createXFactTable(f)){}), APIResult.class);
  }

  public APIResult createFactTable(String factSpec) {
    try {
      return createFactTable(this.<XFactTable>readFromXML(factSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult createCubeSegmentation(XCubeSegmentation seg) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("cubesegmentations")
            .queryParam("sessionid", this.connection.getSessionHandle())
            .request(MediaType.APPLICATION_XML)
            .post(Entity.xml(new GenericEntity<JAXBElement<XCubeSegmentation>>(objFact
                    .createXCubeSegmentation(seg)){}), APIResult.class);
  }

  public APIResult createCubeSegmentation(String segSpec) {
    try {
      return createCubeSegmentation(this.<XCubeSegmentation>readFromXML(segSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updateFactTable(String factName, XFactTable table) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(factName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML_TYPE)
      .put(Entity.xml(new GenericEntity<JAXBElement<XFactTable>>(objFact.createXFactTable(table)){}), APIResult.class);
  }

  public APIResult updateFactTable(String factName, String table) {
    try {
      return updateFactTable(factName, this.<XFactTable>readFromXML(table));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updateCubeSegmentation(String segName, XCubeSegmentation seg) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("cubesegmentations").path(segName)
            .queryParam("sessionid", this.connection.getSessionHandle())
            .request(MediaType.APPLICATION_XML_TYPE)
            .put(Entity.xml(new GenericEntity<JAXBElement<XCubeSegmentation>>(objFact.
                    createXCubeSegmentation(seg)){}), APIResult.class);
  }

  public APIResult updateCubeSegmentation(String segName, String seg) {
    try {
      return updateCubeSegmentation(segName, this.<XCubeSegmentation>readFromXML(seg));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }


  public APIResult dropFactTable(String factName, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(factName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("cascade", cascade)
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public APIResult dropFactTable(String factName) {
    return dropFactTable(factName, false);
  }

  public APIResult dropCubeSegmentation(String segName) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("cubesegmentations").path(segName)
            .queryParam("sessionid", this.connection.getSessionHandle())
            .request(MediaType.APPLICATION_XML)
            .delete(APIResult.class);
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
    return target.path("facts").path(factName).path("storages")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public APIResult addStorageToFactTable(String factname, XStorageTableElement storage) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(factname).path("storages")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XStorageTableElement>>(
        objFact.createXStorageTableElement(storage)){}), APIResult.class);
  }

  public APIResult addStorageToFactTable(String factname, String storageSpec) {
    try {
      return addStorageToFactTable(factname, this.<XStorageTableElement>readFromXML(storageSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult dropStorageFromFactTable(String factName, String storageName) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(factName).path("storages").path(storageName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
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
    JAXBElement<XPartitionList> elements = target.path("facts").path(factName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("filter", filter)
      .request(MediaType.APPLICATION_XML)
      .get(new GenericType<JAXBElement<XPartitionList>>() {
      });
    return elements.getValue().getPartition();
  }

  public List<XPartition> getPartitionsOfFactTable(String factName, String storage) {
    return getPartitionsOfFactTable(factName, storage, "");
  }

  public APIResult dropPartitionsOfFactTable(String factName, String storage, String filter) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(factName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("filter", filter)
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public APIResult dropPartitionsOfFactTable(String factName, String storage) {
    return dropPartitionsOfFactTable(factName, storage, "");
  }

  public APIResult dropPartitionsOfFactTable(String factName, String storage,
    List<String> partitions) {
    String values = Joiner.on(",").skipNulls().join(partitions);

    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(factName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("values", values)
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }


  public List<String> getAllDimensionTables(String dimensionName) {
    if (dimensionName == null) {
      return getAllDimensionTables();
    }
    WebTarget target = getMetastoreWebTarget();
    StringList dimtables;
    dimtables = target.path("dimensions").path(dimensionName).path("dimtables")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(StringList.class);
    return dimtables.getElements();
  }

  public List<String> getAllDimensionTables() {
    WebTarget target = getMetastoreWebTarget();
    StringList dimtables;
    dimtables = target.path("dimtables")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(StringList.class);
    return dimtables.getElements();
  }

  public APIResult createDimensionTable(XDimensionTable table) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XDimensionTable>>(objFact.createXDimensionTable(table)){}),
        APIResult.class);
  }

  public APIResult createDimensionTable(String tableXml) {
    try {
      return createDimensionTable(this.<XDimensionTable>readFromXML(tableXml));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }


  public APIResult updateDimensionTable(XDimensionTable table) {
    String dimTableName = table.getTableName();
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTableName)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XDimensionTable>>(objFact.createXDimensionTable(table)){}),
        APIResult.class);
  }

  public APIResult updateDimensionTable(String dimTblName, String dimSpec) {
    try {
      XDimensionTable dimensionTable = readFromXML(dimSpec);
      dimensionTable.setTableName(dimTblName);
      return updateDimensionTable(dimensionTable);
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult dropDimensionTable(String table, boolean cascade) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(table)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("cascade", cascade)
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public APIResult dropDimensionTable(String table) {
    return dropDimensionTable(table, false);
  }

  public XDimensionTable getDimensionTable(String table) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XDimensionTable> result = target.path("dimtables").path(table)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(new GenericType<JAXBElement<XDimensionTable>>() {
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
    return target.path("dimtables").path(dimTblName).path("storages")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XStorageTableElement>>(
        objFact.createXStorageTableElement(table)){}), APIResult.class);
  }

  public APIResult addStorageToDimTable(String dimTblName, String table) {
    try {
      return addStorageToDimTable(dimTblName, this.<XStorageTableElement>readFromXML(table));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
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
    return target.path("dimtables").path(dimTblName).path("storages")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }


  public APIResult dropStoragesOfDimensionTable(String dimTblName, String storage) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTblName)
      .path("storages").path(storage)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public List<XPartition> getAllPartitionsOfDimensionTable(String dimTblName, String storage,
    String filter) {
    WebTarget target = getMetastoreWebTarget();
    JAXBElement<XPartitionList> partList = target.path("dimtables").path(dimTblName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("filter", filter)
      .request(MediaType.APPLICATION_XML)
      .get(new GenericType<JAXBElement<XPartitionList>>() {
      });
    return partList.getValue().getPartition();
  }

  public List<XPartition> getAllPartitionsOfDimensionTable(String dimTblName, String storage) {
    return getAllPartitionsOfDimensionTable(dimTblName, storage, "");
  }

  public APIResult dropAllPartitionsOfDimensionTable(String dimTblName, String storage,
    String filter) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTblName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("filter", filter)
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public APIResult dropAllPartitionsOfDimensionTable(String dimTblName, String storage) {
    return dropAllPartitionsOfDimensionTable(dimTblName, storage, "");
  }

  public APIResult dropAllPartitionsOfDimensionTable(String dimTblName, String storage,
    List<String> vals) {
    String values = Joiner.on(",").skipNulls().join(vals);
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTblName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .queryParam("values", values)
      .request(MediaType.APPLICATION_XML)
      .delete(APIResult.class);
  }

  public APIResult addPartitionToDimensionTable(String dimTblName, String storage,
    XPartition partition) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTblName)
      .path("storages").path(storage).path("partition")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XPartition>>(objFact.createXPartition(partition)){}),
        APIResult.class);
  }

  public APIResult addPartitionToDimensionTable(String dimTblName, String storage,
    String partitionSpec) {
    try {
      return addPartitionToDimensionTable(dimTblName, storage, (XPartition) readFromXML(partitionSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult addPartitionsToDimensionTable(String dimTblName, String storage,
    XPartitionList partitions) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTblName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XPartitionList>>(objFact.createXPartitionList(partitions)){}),
        APIResult.class);
  }

  public APIResult addPartitionsToDimensionTable(String dimTblName, String storage,
    String partitionsSpec) {
    try {
      return addPartitionsToDimensionTable(dimTblName, storage, (XPartitionList) readFromXML(partitionsSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult addPartitionToFactTable(String fact, String storage,
    XPartition partition) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(fact)
      .path("storages").path(storage).path("partition")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XPartition>>(objFact.createXPartition(partition)){}),
        APIResult.class);
  }

  public APIResult addPartitionToFactTable(String fact, String storage,
    String partitionSpec) {
    try {
      return addPartitionToFactTable(fact, storage, (XPartition) readFromXML(partitionSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult addPartitionsToFactTable(String fact, String storage,
    XPartitionList partitions) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(fact)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .post(Entity.xml(new GenericEntity<JAXBElement<XPartitionList>>(objFact.createXPartitionList(partitions)){}),
        APIResult.class);
  }

  public APIResult addPartitionsToFactTable(String fact, String storage,
    String partitionsSpec) {
    try {
      return addPartitionsToFactTable(fact, storage, (XPartitionList) readFromXML(partitionsSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updatePartitionOfDimensionTable(String dimTblName, String storage,
    XPartition partition) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTblName)
      .path("storages").path(storage).path("partition")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XPartition>>(objFact.createXPartition(partition)){}),
        APIResult.class);
  }

  public APIResult updatePartitionOfDimensionTable(String dimTblName, String storage,
    String partitionSpec) {
    try {
      return updatePartitionOfDimensionTable(dimTblName, storage, (XPartition) readFromXML(partitionSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updatePartitionsOfDimensionTable(String dimTblName, String storage,
    XPartitionList partitions) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("dimtables").path(dimTblName)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XPartitionList>>(objFact.createXPartitionList(partitions)){}),
        APIResult.class);
  }

  public APIResult updatePartitionsOfDimensionTable(String dimTblName, String storage,
    String partitionsSpec) {
    try {
      return updatePartitionsOfDimensionTable(dimTblName, storage, (XPartitionList) readFromXML(partitionsSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updatePartitionOfFactTable(String fact, String storage,
    XPartition partition) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(fact)
      .path("storages").path(storage).path("partition")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XPartition>>(objFact.createXPartition(partition)){}),
        APIResult.class);
  }

  public APIResult updatePartitionOfFactTable(String fact, String storage,
    String partitionSpec) {
    try {
      return updatePartitionOfFactTable(fact, storage, (XPartition) readFromXML(partitionSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public APIResult updatePartitionsOfFactTable(String fact, String storage,
    XPartitionList partitions) {
    WebTarget target = getMetastoreWebTarget();
    return target.path("facts").path(fact)
      .path("storages").path(storage).path("partitions")
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .put(Entity.xml(new GenericEntity<JAXBElement<XPartitionList>>(objFact.createXPartitionList(partitions)){}),
        APIResult.class);
  }

  public APIResult updatePartitionsOfFactTable(String fact, String storage,
    String partitionsSpec) {
    try {
      return updatePartitionsOfFactTable(fact, storage, (XPartitionList) readFromXML(partitionsSpec));
    } catch (JAXBException | IOException e) {
      return failureAPIResult(e);
    }
  }

  public Date getLatestDateOfCube(String cubeName, String timePartition) {
    return getMetastoreWebTarget().path("cubes").path(cubeName).path("latestdate")
      .queryParam("timeDimension", timePartition)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(DateTime.class).getDate();
  }

  public List<String> getPartitionTimelines(String factName, String storageName, String updatePeriod,
    String timeDimension) {
    return getMetastoreWebTarget().path("facts").path(factName).path("timelines")
      .queryParam("storage", storageName)
      .queryParam("updatePeriod", updatePeriod)
      .queryParam("timeDimension", timeDimension)
      .queryParam("sessionid", this.connection.getSessionHandle())
      .request(MediaType.APPLICATION_XML)
      .get(StringList.class).getElements();
  }

  private APIResult failureAPIResult(Exception e) {
    log.error("Failed", e);
    return APIResult.failure(e);
  }
}
