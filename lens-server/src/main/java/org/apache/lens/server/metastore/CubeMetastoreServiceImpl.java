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

import static org.apache.lens.cube.metadata.JAXBUtils.*;

import java.util.*;
import java.util.Date;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.*;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.timeline.PartitionTimeline;
import org.apache.lens.server.BaseLensService;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.health.HealthStatus;
import org.apache.lens.server.api.metastore.CubeMetastoreService;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hive.service.cli.CLIService;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CubeMetastoreServiceImpl extends BaseLensService implements CubeMetastoreService {

  public CubeMetastoreServiceImpl(CLIService cliService) {
    super(NAME, cliService);
  }

  synchronized CubeMetastoreClient getClient(LensSessionHandle sessionid) throws LensException {
    return getSession(sessionid).getCubeMetastoreClient();
  }


  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return database name
   */
  @Override
  public String getCurrentDatabase(LensSessionHandle sessionid) throws LensException {
    try(SessionContext ignored = new SessionContext(sessionid)) {
      return getSession(sessionid).getCurrentDatabase();
    }
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database current database to set
   */
  @Override
  public void setCurrentDatabase(LensSessionHandle sessionid, String database) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      if (!Hive.get(getSession(sessionid).getHiveConf()).databaseExists(database)) {
        throw new NotFoundException("Database " + database + " does not exist");
      }
      log.info("Set database " + database);
      getSession(sessionid).setCurrentDatabase(database);
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  /**
   * Drop a database from cube metastore
   *
   * @param database database name
   * @param cascade  flag indicating if the tables in the database should be dropped as well
   */
  @Override
  public void dropDatabase(LensSessionHandle sessionid, String database, boolean cascade) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Hive.get(getSession(sessionid).getHiveConf()).dropDatabase(database, false, true, cascade);
      log.info("Database dropped " + database + " cascade? " + true);
    } catch (HiveException | NoSuchObjectException e) {
      throw new LensException(e);
    }
  }

  /**
   * Create a database in Hive metastore
   *
   * @param database database name
   * @param ignore   ignore if database already exists
   * @throws LensException
   */
  @Override
  public void createDatabase(LensSessionHandle sessionid, String database, boolean ignore) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Database db = new Database();
      db.setName(database);
      Hive.get(getSession(sessionid).getHiveConf()).createDatabase(db, ignore);
    } catch (AlreadyExistsException | HiveException e) {
      throw new LensException(e);
    }
    log.info("Database created " + database);
  }

  /**
   * @return get all database names
   */
  @Override
  public List<String> getAllDatabases(LensSessionHandle sessionid) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      return Hive.get(getSession(sessionid).getHiveConf()).getAllDatabases();
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  /**
   * Get list of all cubes names in the current database
   *
   * @return List of cube names
   * @throws LensException
   */
  @Override
  public List<String> getAllCubeNames(LensSessionHandle sessionid) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Collection<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<>(cubes.size());
        for (CubeInterface cube : cubes) {
          names.add(cube.getName());
        }
        return names;
      }
    }
    return null;
  }

  /**
   * Create cube based on the JAXB cube object
   *
   * @param cube cube spec
   * @throws LensException
   */
  @Override
  public void createCube(LensSessionHandle sessionid, XCube cube) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).createCube(cube, getSession(sessionid).getLoggedInUserGroups());
      log.info("Created cube " + cube.getName());
    }
  }

  /**
   * Get a cube from the metastore
   *
   * @param cubeName cube name
   * @return The cube object as {@link XCube}
   * @throws LensException
   */
  @Override
  public XCube getCube(LensSessionHandle sessionid, String cubeName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeInterface c = getClient(sessionid).getCube(cubeName);
      if (c != null) {
        return JAXBUtils.xCubeFromHiveCube(c);
      }
    }
    return null;
  }

  /**
   * Drop a cube from the metastore in the currently deleted database
   *
   * @param cubeName cube name
   */
  public void dropCube(LensSessionHandle sessionid, String cubeName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      getClient(sessionid).dropCube(cubeName, getSession(sessionid).getLoggedInUserGroups());
    }
  }

  /**
   * Update cube
   *
   * @param cube JAXB Cube object
   * @throws LensException
   */
  @Override
  public void updateCube(LensSessionHandle sessionid, XCube cube) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).alterCube(cube, getSession(sessionid).getLoggedInUserGroups());
      log.info("Cube updated " + cube.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  /**
   * Create a cube dimension table based on JAXB object
   *
   * @param xDimTable dim table spec
   * @throws LensException
   */
  @Override
  public void createDimensionTable(LensSessionHandle sessionid, XDimensionTable xDimTable) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).createCubeDimensionTable(xDimTable, getSession(sessionid).getLoggedInUserGroups());
      log.info("Dimension Table created " + xDimTable.getTableName());
    }
  }

  @Override
  public void dropDimensionTable(LensSessionHandle sessionid, String dimTblName, boolean cascade) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).dropDimensionTable(dimTblName, cascade, getSession(sessionid).getLoggedInUserGroups());
      log.info("Dropped dimension table " + dimTblName + " cascade? " + cascade);
    }
  }

  @Override
  public XDimensionTable getDimensionTable(LensSessionHandle sessionid, String dimTblName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      return getClient(sessionid).getXDimensionTable(dimTblName);
    }
  }

  @Override
  public void updateDimensionTable(LensSessionHandle sessionid, XDimensionTable dimensionTable) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).alterCubeDimensionTable(dimensionTable, getSession(sessionid).getLoggedInUserGroups());
      log.info("Updated dimension table " + dimensionTable.getTableName());
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public List<String> getDimTableStorages(LensSessionHandle sessionid, String dimension) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimension);
      return new ArrayList<>(dimTable.getStorages());
    }
  }


  @Override
  public void addDimTableStorage(LensSessionHandle sessionid,
    String dimTblName, XStorageTableElement storageTable) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient msClient = getClient(sessionid);
      CubeDimensionTable dimTable = msClient.getDimensionTable(dimTblName);
      UpdatePeriod period = null;
      if (storageTable.getUpdatePeriods() != null && !storageTable.getUpdatePeriods().getUpdatePeriod().isEmpty()) {
        period = UpdatePeriod.valueOf(storageTable.getUpdatePeriods().getUpdatePeriod().get(0).name());
      }
      msClient.addStorage(dimTable, storageTable.getStorageName(), period,
        JAXBUtils.storageTableDescFromXStorageTableDesc(storageTable.getTableDesc()),
        getSession(sessionid).getLoggedInUserGroups());
      log.info("Added storage " + storageTable.getStorageName() + " for dimension table " + dimTblName
        + " with update period " + period);
    }
  }

  @Override
  public void dropAllStoragesOfDimTable(LensSessionHandle sessionid, String dimTblName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient msClient = getClient(sessionid);
      CubeDimensionTable tab = msClient.getDimensionTable(dimTblName);
      int total = tab.getStorages().size();
      int i = 0;
      List<String> storageNames = new ArrayList<>(tab.getStorages());
      for (String s : storageNames) {
        msClient.dropStorageFromDim(dimTblName, s, getSession(sessionid).getLoggedInUserGroups());
        log.info("Dropped storage " + s + " from dimension table " + dimTblName
          + " [" + ++i + "/" + total + "]");
      }
      log.info("Dropped " + total + " storages from dimension table " + dimTblName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public List<String> getAllDimTableNames(LensSessionHandle sessionid, String dimensionName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient client = getClient(sessionid);
      Dimension dimension = client.getDimension(dimensionName);
      if (dimensionName != null && dimension == null) {
        throw new LensException("Could not get table: " + dimensionName + " as a dimension");
      }
      Collection<CubeDimensionTable> dims = client.getAllDimensionTables(dimension);
      List<String> dimNames = new ArrayList<>(dims.size());
      for (CubeDimensionTable cdt : dims) {
        dimNames.add(cdt.getName());
      }
      return dimNames;
    }
  }

  @Override
  public void dropAllStoragesOfFact(LensSessionHandle sessionid, String factName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient msClient = getClient(sessionid);
      CubeFactTable tab = msClient.getCubeFactTable(factName);
      int total = tab.getStorages().size();
      int i = 0;
      List<String> storageNames = new ArrayList<>(tab.getStorages());
      for (String s : storageNames) {
        msClient.dropStorageFromFact(factName, s, getSession(sessionid).getLoggedInUserGroups());
        log.info("Dropped storage " + s + " from fact table " + factName
          + " [" + ++i + "/" + total + "]");
      }
      log.info("Dropped " + total + " storages from fact table " + factName);
    }
  }

  @Override
  public void dropStorageOfDimTable(LensSessionHandle sessionid, String dimTblName, String storage)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient msClient = getClient(sessionid);
      CubeDimensionTable tab = msClient.getDimensionTable(dimTblName);
      if (!tab.getStorages().contains(storage)) {
        throw new NotFoundException("Storage " + storage + " not found for dimension " + dimTblName);
      }
      msClient.dropStorageFromDim(dimTblName, storage, getSession(sessionid).getLoggedInUserGroups());
      log.info("Dropped storage " + storage + " from dimension table " + dimTblName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public XFact getFactTable(LensSessionHandle sessionid, String fact) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      return getClient(sessionid).getXFactTable(fact);
    }
  }

  @Override
  public XSegmentation getSegmentation(LensSessionHandle sessionid, String cubeSegName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient msClient = getClient(sessionid);
      Segmentation cubeSeg = msClient.getSegmentation(cubeSegName);
      return JAXBUtils.xsegmentationFromSegmentation(cubeSeg);
    }
  }


  @Override
  public void createFactTable(LensSessionHandle sessionid, XFact fact) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).createFactTable(fact, getSession(sessionid).getLoggedInUserGroups());
      log.info("Created fact table " + fact.getName());
    }
  }

  @Override
  public void updateFactTable(LensSessionHandle sessionid, XFact fact) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).alterCubeFactTable(fact, getSession(sessionid).getLoggedInUserGroups());
      log.info("Updated fact table " + fact.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }


  @Override
  public void createSegmentation(LensSessionHandle sessionid, XSegmentation cubeSeg) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).createSegmentation(cubeSeg, getSession(sessionid).getLoggedInUserGroups());
      log.info("Created segmentation " + cubeSeg.getName());
    }
  }

  @Override
  public void updateSegmentation(LensSessionHandle sessionid, XSegmentation cubeSeg) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).alterSegmentation(cubeSeg, getSession(sessionid).getLoggedInUserGroups());
      log.info("Updated segmentation " + cubeSeg.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  @Override
  public void dropFactTable(LensSessionHandle sessionid, String fact, boolean cascade) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).dropFact(fact, cascade, getSession(sessionid).getLoggedInUserGroups());
      log.info("Dropped fact table " + fact + " cascade? " + cascade);
    }
  }

  @Override
  public void dropSegmentation(LensSessionHandle sessionid, String cubeSegName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).dropSegmentation(cubeSegName, getSession(sessionid).getLoggedInUserGroups());
      log.info("Dropped segemntation " + cubeSegName);
    }

  }

  @Override
  public List<String> getAllFactNames(LensSessionHandle sessionid, String cubeName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient client = getClient(sessionid);
      CubeInterface fact = client.getCube(cubeName);
      if (cubeName != null && fact == null) {
        throw new LensException("Could not get table: " + cubeName + " as a cube");
      }
      Collection<FactTable> facts = client.getAllFacts(fact);
      List<String> factNames = new ArrayList<>(facts.size());
      for (FactTable cft : facts) {
        factNames.add(cft.getName());
      }
      return factNames;
    }
  }

  @Override
  public List<String> getAllSegmentations(LensSessionHandle sessionid, String cubeName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient client = getClient(sessionid);
      CubeInterface seg = client.getCube(cubeName);
      if (cubeName != null && seg == null) {
        throw new LensException("Could not get table: " + cubeName + " as a cube");
      }
      Collection<Segmentation> segs = client.getAllSegmentations(seg);
      List<String> segNames = new ArrayList<>(segs.size());
      for (Segmentation cs : segs) {
        segNames.add(cs.getName());
      }
      return segNames;
    }

  }


  @Override
  public List<String> getStoragesOfFact(LensSessionHandle sessionid, String fact) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient msClient = getClient(sessionid);
      if (!msClient.isFactTable(fact)) {
        throw new NotFoundException("Not a fact table " + fact);
      }

      CubeFactTable cft = msClient.getCubeFactTable(fact);
      if (cft != null) {
        return new ArrayList<>(cft.getStorages());
      } else {
        throw new NotFoundException("Could not get fact table " + fact);
      }
    }
  }

  public XStorageTableElement getStorageOfFact(LensSessionHandle sessionid, String fact, String storageName)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      CubeMetastoreClient msClient = getClient(sessionid);
      FactTable factTable = msClient.getFactTable(fact);
      Set<UpdatePeriod> updatePeriods = factTable.getUpdatePeriods().get(storageName);
      XStorageTableElement tblElement = JAXBUtils.getXStorageTableFromHiveTable(
        msClient.getHiveTable(MetastoreUtil.getFactOrDimtableStorageTableName(fact, storageName)));
      tblElement.setStorageName(storageName);
      for (UpdatePeriod p : updatePeriods) {
        tblElement.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p.name()));
      }
      return tblElement;
    }
  }

  public XStorageTableElement getStorageOfDim(LensSessionHandle sessionid, String dimTblName, String storageName)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      CubeMetastoreClient msClient = getClient(sessionid);
      CubeDimensionTable dimTable = msClient.getDimensionTable(dimTblName);
      XStorageTableElement tblElement = JAXBUtils.getXStorageTableFromHiveTable(
        msClient.getHiveTable(MetastoreUtil.getFactOrDimtableStorageTableName(dimTblName, storageName)));
      tblElement.setStorageName(storageName);
      UpdatePeriod p = dimTable.getSnapshotDumpPeriods().get(storageName);
      if (p != null) {
        tblElement.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p.name()));
      }
      return tblElement;
    }
  }

  @Override
  public void addStorageToFact(LensSessionHandle sessionid, String fact, XStorageTableElement storageTable)
    throws LensException {
    Set<UpdatePeriod> updatePeriods = new TreeSet<>();
    for (XUpdatePeriod sup : storageTable.getUpdatePeriods().getUpdatePeriod()) {
      updatePeriods.add(UpdatePeriod.valueOf(sup.name()));
    }
    try (SessionContext ignored = new SessionContext(sessionid)) {
      CubeMetastoreClient msClient = getClient(sessionid);
      XStorageTables tables = new XStorageTables();
      tables.getStorageTable().add(storageTable);
      msClient.addStorage(msClient.getCubeFactTable(fact), storageTable.getStorageName(), updatePeriods,
        JAXBUtils.tableDescPrefixMapFromXStorageTables(tables),
        JAXBUtils.storageTablePrefixMapOfStorage(tables).get(storageTable.getStorageName()),
        getSession(sessionid).getLoggedInUserGroups());
      log.info("Added storage " + storageTable.getStorageName() + ":" + updatePeriods + " for fact " + fact);
    }
  }

  @Override
  public void dropStorageOfFact(LensSessionHandle sessionid, String fact, String storage) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      checkFactStorage(sessionid, fact, storage);
      getClient(sessionid).dropStorageFromFact(fact, storage, getSession(sessionid).getLoggedInUserGroups());
      log.info("Dropped storage " + storage + " from fact " + fact);
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  private CubeFactTable checkFactStorage(LensSessionHandle sessionid, String fact, String storage)
    throws HiveException, LensException {
    CubeMetastoreClient client = getClient(sessionid);
    CubeFactTable factTable = client.getCubeFactTable(fact);
    client.verifyStorageExists(factTable, storage);
    return factTable;
  }

  private Set<String> getAllTablesForStorage(LensSessionHandle sessionHandle, String fact, String storageName)
    throws LensException {
    Set<String> storageTableNames = new HashSet<>();
    if (getClient(sessionHandle).isFactTable(fact)) {
      CubeFactTable cft = getClient(sessionHandle).getCubeFactTable(fact);
      Map<UpdatePeriod, String> storageMap = cft.getStoragePrefixUpdatePeriodMap().get(storageName);
      for (Map.Entry entry : storageMap.entrySet()) {
        storageTableNames.add(MetastoreUtil.getStorageTableName(fact, Storage.getPrefix((String) entry.getValue())));
      }
    } else {
      storageTableNames.add(MetastoreUtil.getFactOrDimtableStorageTableName(fact, storageName));
    }
    return storageTableNames;
  }

  @Override
  public XPartitionList getAllPartitionsOfFactStorage(LensSessionHandle sessionid, String fact, String storageName,
    String filter) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      checkFactStorage(sessionid, fact, storageName);
      CubeMetastoreClient client = getClient(sessionid);
      Set<String> storageTableNames = getAllTablesForStorage(sessionid, fact, storageName);
      List<Partition> parts = new ArrayList<>();
      List<String> timePartCols = new ArrayList<>();
      for (String storageTableName : storageTableNames) {
        parts.addAll(client.getPartitionsByFilter(storageTableName, filter));
        timePartCols.addAll(client.getTimePartColNamesOfTable(storageTableName));
      }
      return xpartitionListFromPartitionList(fact, parts, timePartCols);
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public int addPartitionToFactStorage(LensSessionHandle sessionid, String fact, String storageName,
    XPartition partition) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      checkFactStorage(sessionid, fact, storageName);
      return getClient(sessionid)
        .addPartition(storagePartSpecFromXPartition(partition), storageName, CubeTableType.FACT).size();
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public int addPartitionsToFactStorage(LensSessionHandle sessionid, String fact, String storageName,
    XPartitionList partitions) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      checkFactStorage(sessionid, fact, storageName);
      return getClient(sessionid)
        .addPartitions(storagePartSpecListFromXPartitionList(partitions), storageName, CubeTableType.FACT).size();
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  private CubeDimensionTable checkDimTableStorage(LensSessionHandle sessionid, String dimTable, String storage)
    throws HiveException, LensException {
    CubeMetastoreClient client = getClient(sessionid);
    CubeDimensionTable cdt = client.getDimensionTable(dimTable);
    client.verifyStorageExists(cdt, storage);
    return cdt;
  }

  @Override
  public XPartitionList getAllPartitionsOfDimTableStorage(
    LensSessionHandle sessionid, String dimTable, String storageName, String filter)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      checkDimTableStorage(sessionid, dimTable, storageName);
      CubeMetastoreClient client = getClient(sessionid);
      String storageTableName = MetastoreUtil.getFactOrDimtableStorageTableName(dimTable, storageName);
      List<Partition> partitions = client.getPartitionsByFilter(storageTableName, filter);
      List<String> timePartCols = client.getTimePartColNamesOfTable(storageTableName);
      return xpartitionListFromPartitionList(dimTable, partitions, timePartCols);
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public int addPartitionToDimStorage(LensSessionHandle sessionid,
    String dimTblName, String storageName, XPartition partition) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      checkDimTableStorage(sessionid, dimTblName, storageName);
      return getClient(sessionid).addPartition(storagePartSpecFromXPartition(partition), storageName,
        CubeTableType.DIM_TABLE).size();
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public void updatePartition(LensSessionHandle sessionid, String tblName, String storageName, XPartition xPartition)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      CubeMetastoreClient client = getClient(sessionid);
      String storageTableName = client
        .getStorageTableName(tblName, storageName, UpdatePeriod.valueOf(xPartition.getUpdatePeriod().name()));
      Partition existingPartition = client.getPartitionByFilter(storageTableName,
        StorageConstants.getPartFilter(JAXBUtils.getFullPartSpecAsMap(xPartition)));
      JAXBUtils.updatePartitionFromXPartition(existingPartition, xPartition);
      client.updatePartition(tblName, storageName, existingPartition,
        UpdatePeriod.valueOf(xPartition.getUpdatePeriod().value()));
    } catch (HiveException | ClassNotFoundException | InvalidOperationException | UnsupportedOperationException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public void updatePartitions(LensSessionHandle sessionid, String tblName, String storageName,
    XPartitionList xPartitions) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      CubeMetastoreClient client = getClient(sessionid);
      Set<String> storageTableNames = getAllTablesForStorage(sessionid, tblName, storageName);
      Map<UpdatePeriod, List<Partition>> partitionsToUpdate = new HashMap<>();
      for (String storageTableName : storageTableNames) {
        for (XPartition xPartition : xPartitions.getPartition()) {
          Partition existingPartition = client.getPartitionByFilter(storageTableName,
            StorageConstants.getPartFilter(JAXBUtils.getFullPartSpecAsMap(xPartition)));
          JAXBUtils.updatePartitionFromXPartition(existingPartition, xPartition);
          UpdatePeriod updatePeriod = UpdatePeriod.valueOf(xPartition.getUpdatePeriod().value());
          List<Partition> partitionList = partitionsToUpdate.get(updatePeriod);
          if (partitionList == null) {
            partitionList = new ArrayList<>();
            partitionsToUpdate.put(updatePeriod, partitionList);
          }
          partitionList.add(existingPartition);
        }
      }
      client.updatePartitions(tblName, storageName, partitionsToUpdate);
    } catch (HiveException | ClassNotFoundException | InvalidOperationException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public int addPartitionsToDimStorage(LensSessionHandle sessionid,
    String dimTblName, String storageName, XPartitionList partitions) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      checkDimTableStorage(sessionid, dimTblName, storageName);
      return getClient(sessionid).addPartitions(storagePartSpecListFromXPartitionList(partitions), storageName,
        CubeTableType.DIM_TABLE).size();
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  private String getFilter(CubeMetastoreClient client, String tableName,
    String values) throws LensException {
    List<FieldSchema> cols = client.getHiveTable(tableName).getPartCols();
    String[] vals = StringUtils.split(values, ",");
    if (vals.length != cols.size()) {
      log.error("Values for all the part columns not specified, cols:" + cols + " vals:" + Arrays.toString(vals));
      throw new BadRequestException("Values for all the part columns not specified");
    }
    StringBuilder filter = new StringBuilder();
    for (int i = 0; i < vals.length; i++) {
      filter.append(cols.get(i).getName());
      filter.append("=");
      filter.append("\"");
      filter.append(vals[i]);
      filter.append("\"");
      if (i != (vals.length - 1)) {
        filter.append(" AND ");
      }
    }
    return filter.toString();
  }

  private UpdatePeriod populatePartSpec(Partition p, Map<String, Date> timeSpec,
    Map<String, String> nonTimeSpec) throws HiveException {
    String timePartColsStr = p.getTable().getTTable().getParameters().get(MetastoreConstants.TIME_PART_COLUMNS);
    String upParam = p.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD);
    UpdatePeriod period = UpdatePeriod.valueOf(upParam);
    Map<String, String> partSpec = new HashMap<>();
    partSpec.putAll(p.getSpec());
    if (timePartColsStr != null) {
      String[] timePartCols = StringUtils.split(timePartColsStr, ',');
      for (String partCol : timePartCols) {
        String dateStr = partSpec.get(partCol);
        Date date;
        try {
          date = period.parse(dateStr);
        } catch (Exception e) {
          continue;
        }
        partSpec.remove(partCol);
        timeSpec.put(partCol, date);
      }
    }
    if (!partSpec.isEmpty()) {
      nonTimeSpec.putAll(partSpec);
    }
    return period;
  }

  public void dropPartitionFromStorageByValues(LensSessionHandle sessionid, String cubeTableName, String storageName,
    String values) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      Set<String> storageTables = getAllTablesForStorage(sessionid, cubeTableName, storageName);
      Map<String, List<Partition>> partitions = new HashMap<>();
      CubeMetastoreClient msClient = getClient(sessionid);
      int totalPartitions = 0;
      Partition part = null;
      for (String tableName : storageTables) {
        String filter = getFilter(msClient, tableName, values);
        partitions.put(filter, msClient.getPartitionsByFilter(tableName, filter));
        if (partitions.get(filter).size() > 1) {
          log.error("More than one partition with specified values, corresponding filter:" + filter);
          throw new BadRequestException("More than one partition with specified values");
        }
        if (partitions.get(filter).size() == 1) {
          part = partitions.get(filter).get(0);
        }
        totalPartitions += partitions.get(filter).size();
      }
      if (totalPartitions == 0) {
        log.error("No partition exists with specified values");
        throw new NotFoundException("No partition exists with specified values");
      }
      Map<String, Date> timeSpec = new HashMap<>();
      Map<String, String> nonTimeSpec = new HashMap<>();
      UpdatePeriod updatePeriod = populatePartSpec(part, timeSpec, nonTimeSpec);
      msClient.dropPartition(cubeTableName, storageName, timeSpec, nonTimeSpec, updatePeriod);
      log.info("Dropped partition  for dimension: " + cubeTableName + " storage: " + storageName + " values:" + values);
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  public void dropPartitionFromStorageByFilter(LensSessionHandle sessionid, String cubeTableName,
    String storageName, String filter) throws LensException {
    dropPartitionFromStorageByFilter(sessionid, cubeTableName, storageName, filter, null);
  }
  public void dropPartitionFromStorageByFilter(LensSessionHandle sessionid, String cubeTableName,
    String storageName, String filter, String updatePeriodString) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Set<String> storageTables = getAllTablesForStorage(sessionid, cubeTableName, storageName);
      List<Partition> partitions  = new ArrayList<>();
      CubeMetastoreClient msClient = getClient(sessionid);
      for (String tableName : storageTables) {
        partitions.addAll(msClient.getPartitionsByFilter(tableName, filter));
      }
      if (updatePeriodString!=null) {
        partitions.removeIf(part -> !part.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD).
            equals((updatePeriodString.toUpperCase())));
      }
      for (Partition part : partitions) {
        try {
          Map<String, Date> timeSpec = new HashMap<>();
          Map<String, String> nonTimeSpec = new HashMap<>();
          UpdatePeriod updatePeriod= populatePartSpec(part, timeSpec, nonTimeSpec);
          msClient.dropPartition(cubeTableName, storageName, timeSpec, nonTimeSpec, updatePeriod);
        } catch (HiveException e) {
          if (!(e.getCause() instanceof NoSuchObjectException)) {
            throw new LensException(e);
          }
        }
      }
      log.info("Dropped partition  for cube table: " + cubeTableName
        + " storage: " + storageName + " by filter:" + filter);
    } catch (HiveException exc) {
      throw new LensException(exc);
    }
  }

  @Override
  public void createStorage(LensSessionHandle sessionid, XStorage storage)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).createStorage(storage, getSession(sessionid).getLoggedInUserGroups());
      log.info("Created storage " + storage.getName());
    }

  }

  @Override
  public void dropStorage(LensSessionHandle sessionid, String storageName)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).dropStorage(storageName);
      log.info("Dropped storage " + storageName);
    }
  }

  @Override
  public void alterStorage(LensSessionHandle sessionid, String storageName,
    XStorage storage) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).alterStorage(storage, getSession(sessionid).getLoggedInUserGroups());
      log.info("Altered storage " + storageName);
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  @Override
  public XStorage getStorage(LensSessionHandle sessionid, String storageName)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      return JAXBUtils.xstorageFromStorage(getClient(sessionid).getStorage(storageName));
    }
  }

  @Override
  public List<String> getAllStorageNames(LensSessionHandle sessionid)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Collection<Storage> storages = getClient(sessionid).getAllStorages();
      if (storages != null && !storages.isEmpty()) {
        List<String> names = new ArrayList<>(storages.size());
        for (Storage storage : storages) {
          names.add(storage.getName());
        }
        return names;
      }
    }
    return null;
  }

  @Override
  public List<String> getAllBaseCubeNames(LensSessionHandle sessionid)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Collection<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<>(cubes.size());
        for (CubeInterface cube : cubes) {
          if (!cube.isDerivedCube()) {
            names.add(cube.getName());
          }
        }
        return names;
      }
    }
    return null;
  }

  @Override
  public List<String> getAllDerivedCubeNames(LensSessionHandle sessionid)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Collection<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<>(cubes.size());
        for (CubeInterface cube : cubes) {
          if (cube.isDerivedCube()) {
            names.add(cube.getName());
          }
        }
        return names;
      }
    }
    return null;
  }

  @Override
  public List<String> getAllQueryableCubeNames(LensSessionHandle sessionid)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Collection<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<>(cubes.size());
        for (CubeInterface cube : cubes) {
          if (cube.allFieldsQueriable()) {
            names.add(cube.getName());
          }
        }
        return names;
      }
    }
    return null;
  }

  @Override
  public void createDimension(LensSessionHandle sessionid, XDimension dimension)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).createDimension(dimension, getSession(sessionid).getLoggedInUserGroups());
      log.info("Created dimension " + dimension.getName());
    }
  }

  @Override
  public XDimension getDimension(LensSessionHandle sessionid, String dimName)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      return JAXBUtils.xdimensionFromDimension(getClient(sessionid).getDimension(dimName));
    }
  }

  @Override
  public void dropDimension(LensSessionHandle sessionid, String dimName)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).dropDimension(dimName, getSession(sessionid).getLoggedInUserGroups());
      log.info("Dropped dimension " + dimName);
    }
  }

  @Override
  public void updateDimension(LensSessionHandle sessionid, String dimName, XDimension dimension)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      getClient(sessionid).alterDimension(dimension, getSession(sessionid).getLoggedInUserGroups());
      log.info("Altered dimension " + dimName);
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  @Override
  public List<String> getAllDimensionNames(LensSessionHandle sessionid)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      Collection<Dimension> dimensions = getClient(sessionid).getAllDimensions();
      if (dimensions != null && !dimensions.isEmpty()) {
        List<String> names = new ArrayList<>(dimensions.size());
        for (Dimension dim : dimensions) {
          names.add(dim.getName());
        }
        return names;
      }
    }
    return null;
  }

  @Override
  public XNativeTable getNativeTable(LensSessionHandle sessionid, String name)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      return JAXBUtils.nativeTableFromMetaTable(getClient(sessionid).getTableWithTypeFailFast(name, null));
    }
  }

  private List<String> getNativeTablesFromDB(LensSessionHandle sessionid, String dbName, boolean prependDbName)
    throws LensException {
    IMetaStoreClient msc;
    try {
      msc = getSession(sessionid).getMetaStoreClient();
      List<String> tables = msc.getAllTables(
        dbName);
      Configuration conf = getSession(sessionid).getSessionConf();
      if (!conf.getBoolean(LensConfConstants.EXCLUDE_CUBE_TABLES, LensConfConstants.DEFAULT_EXCLUDE_CUBE_TABLES)) {
        return tables;
      }
      List<String> result = new ArrayList<>();
      if (tables != null && !tables.isEmpty()) {
        List<org.apache.hadoop.hive.metastore.api.Table> tblObjects =
          msc.getTableObjectsByName(dbName, tables);
        for (Table tbl : tblObjects) {
          if (tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY) == null) {
            if (prependDbName) {
              result.add(dbName + "." + tbl.getTableName());
            } else {
              result.add(tbl.getTableName());
            }
          }
        }
      }
      return result;
    } catch (Exception e) {
      throw new LensException("Error getting native tables from DB", e);
    }
  }

  @Override
  public List<String> getAllNativeTableNames(LensSessionHandle sessionid,
    String dbOption, String dbName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionid)){
      if (!StringUtils.isBlank(dbName)) {
        if (!Hive.get(getSession(sessionid).getHiveConf()).databaseExists(dbName)) {
          throw new NotFoundException("Database " + dbName + " does not exist");
        }
      }
      if (StringUtils.isBlank(dbName)
        && (StringUtils.isBlank(dbOption)
        || dbOption.equalsIgnoreCase("current"))) {
        // use current db if no dbname/dboption is passed
        dbName = getSession(sessionid).getCurrentDatabase();
      }
      List<String> tables;
      if (!StringUtils.isBlank(dbName)) {
        tables = getNativeTablesFromDB(sessionid, dbName, false);
      } else {
        log.info("Getting tables from all dbs");
        tables = new ArrayList<>();
        for (String db : getAllDatabases(sessionid)) {
          tables.addAll(getNativeTablesFromDB(sessionid, db, true));
        }
      }
      return tables;
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  private void addAllMeasuresToFlattenedList(ObjectFactory objectFactory, CubeInterface cube,
    List<XFlattenedColumn> columnList) {
    for (CubeMeasure msr : cube.getMeasures()) {
      XFlattenedColumn fcol = objectFactory.createXFlattenedColumn();
      fcol.setMeasure(JAXBUtils.xMeasureFromHiveMeasure(msr));
      fcol.setTableName(cube.getName());
      columnList.add(fcol);
    }
  }

  private void addAllDirectAttributesToFlattenedListFromCube(ObjectFactory objectFactory, CubeInterface cube,
    List<XFlattenedColumn> columnList) {
    AbstractBaseTable baseTbl = (AbstractBaseTable) (cube instanceof DerivedCube
      ? ((DerivedCube) cube).getParent() : cube);
    for (CubeDimAttribute dim : cube.getDimAttributes()) {
      XFlattenedColumn fcol = objectFactory.createXFlattenedColumn();
      fcol.setDimAttribute(JAXBUtils.xDimAttrFromHiveDimAttr(dim, baseTbl));
      fcol.setTableName(cube.getName());
      columnList.add(fcol);
    }
  }

  private void addAllDirectAttributesToFlattenedListFromDimension(ObjectFactory objectFactory, Dimension dimension,
    List<XFlattenedColumn> columnList, String chainName) {
    for (CubeDimAttribute cd : dimension.getAttributes()) {
      XFlattenedColumn fcol = objectFactory.createXFlattenedColumn();
      fcol.setDimAttribute(JAXBUtils.xDimAttrFromHiveDimAttr(cd, dimension));
      fcol.setTableName(dimension.getName());
      if (chainName != null) {
        fcol.setChainName(chainName);
      }
      columnList.add(fcol);
    }
  }

  private void addAllDirectExpressionsToFlattenedList(ObjectFactory objectFactory, AbstractBaseTable baseTbl,
    List<XFlattenedColumn> columnList, String chainName) {
    if (baseTbl.getExpressions() != null) {
      for (ExprColumn expr : baseTbl.getExpressions()) {
        XFlattenedColumn fcol = objectFactory.createXFlattenedColumn();
        fcol.setExpression(JAXBUtils.xExprColumnFromHiveExprColumn(expr));
        fcol.setTableName(baseTbl.getName());
        if (chainName != null) {
          fcol.setChainName(chainName);
        }
        columnList.add(fcol);
      }
    }
  }

  private void addAllDirectExpressionsToFlattenedList(ObjectFactory objectFactory, CubeInterface baseTbl,
    List<XFlattenedColumn> columnList, String chainName) {
    if (baseTbl.getExpressions() != null) {
      for (ExprColumn expr : baseTbl.getExpressions()) {
        XFlattenedColumn fcol = objectFactory.createXFlattenedColumn();
        fcol.setExpression(JAXBUtils.xExprColumnFromHiveExprColumn(expr));
        fcol.setTableName(baseTbl.getName());
        if (chainName != null) {
          fcol.setChainName(chainName);
        }
        columnList.add(fcol);
      }
    }
  }

  private void addAllChainedColsToFlattenedListFromCube(CubeMetastoreClient client, ObjectFactory objectFactory,
    CubeInterface cube, List<XFlattenedColumn> columnList) throws HiveException, LensException {
    if (cube instanceof DerivedCube) {
      return;
    }
    addAllChainedColsToFlattenedList(client, objectFactory, (AbstractBaseTable) cube, columnList);
  }

  private void addAllChainedColsToFlattenedList(CubeMetastoreClient client, ObjectFactory objectFactory,
    AbstractBaseTable baseTbl, List<XFlattenedColumn> columnList) throws HiveException, LensException {
    for (JoinChain chain : baseTbl.getJoinChains()) {
      Dimension dim = client.getDimension(chain.getDestTable());
      addAllDirectAttributesToFlattenedListFromDimension(objectFactory, dim, columnList, chain.getName());
      addAllDirectExpressionsToFlattenedList(objectFactory, dim, columnList, chain.getName());
    }
  }

  @Override
  public XFlattenedColumns getFlattenedColumns(LensSessionHandle sessionHandle, String tableName, boolean addChains)
    throws LensException {
    try (SessionContext ignored = new SessionContext(sessionHandle)){
      CubeMetastoreClient client = getClient(sessionHandle);

      ObjectFactory objectFactory = new ObjectFactory();
      XFlattenedColumns flattenedColumns = objectFactory.createXFlattenedColumns();
      List<XFlattenedColumn> columnList = flattenedColumns.getFlattenedColumn();
      // check if the table is a cube or dimension
      if (client.isCube(tableName)) {
        CubeInterface cube = client.getCube(tableName);
        addAllMeasuresToFlattenedList(objectFactory, cube, columnList);
        addAllDirectAttributesToFlattenedListFromCube(objectFactory, cube, columnList);
        addAllDirectExpressionsToFlattenedList(objectFactory, cube, columnList, null);
        if (addChains) {
          addAllChainedColsToFlattenedListFromCube(client, objectFactory, cube, columnList);
        }
      } else if (client.isDimension(tableName)) {
        Dimension dimension = client.getDimension(tableName);
        addAllDirectAttributesToFlattenedListFromDimension(objectFactory, dimension, columnList, null);
        addAllDirectExpressionsToFlattenedList(objectFactory, dimension, columnList, null);
        if (addChains) {
          addAllChainedColsToFlattenedList(client, objectFactory, dimension, columnList);
        }
      } else {
        throw new BadRequestException("Can't get reachable columns. '"
          + tableName + "' is neither a cube nor a dimension");
      }
      return flattenedColumns;
    } catch (HiveException e) {
      throw new LensException("Error getting flattened view for " + tableName, e);
    }
  }

  @Override
  public Date getLatestDateOfCube(LensSessionHandle sessionid, String cubeName, String timeDimension)
    throws LensException, HiveException {
    try(SessionContext ignored = new SessionContext(sessionid)) {
      // get the partitionColumn corresponding to timeDimension passed
      CubeMetastoreClient msClient = getClient(sessionid);
      CubeInterface ci = msClient.getCube(cubeName);
      if (!(ci instanceof Cube)) {
        throw new BadRequestException("cubeName : " + cubeName + " is not a base cube.");
      }
      Cube c = (Cube) ci;
      return msClient.getLatestDateOfCube(c, timeDimension);
    }
  }

  public List<String> getPartitionTimelines(LensSessionHandle sessionid, String factName, String storage,
    String updatePeriod, String timeDimension) throws LensException, HiveException {
    try (SessionContext ignored = new SessionContext(sessionid)) {
      CubeMetastoreClient client = getClient(sessionid);
      List<String> ret = Lists.newArrayList();
      for (PartitionTimeline timeline : client.getTimelines(factName, storage, updatePeriod, timeDimension)) {
        ret.add(timeline.toString());
      }
      return ret;
    }
  }

  @Override
  public XJoinChains getAllJoinChains(LensSessionHandle sessionHandle, String tableName) throws LensException {
    try (SessionContext ignored = new SessionContext(sessionHandle)){
      CubeMetastoreClient client = getClient(sessionHandle);
      Set<JoinChain> chains;
      if (client.isCube(tableName)) {
        chains = client.getCube(tableName).getJoinChains();
      } else if (client.isDimension(tableName)) {
        chains = client.getDimension(tableName).getJoinChains();
      } else {
        throw new BadRequestException("Can't get join chains. '"
          + tableName + "' is neither a cube nor a dimension");
      }
      XJoinChains xJoinChains = new XJoinChains();
      List<XJoinChain> joinChains = xJoinChains.getJoinChain();
      if (chains != null) {
        for (JoinChain chain : chains) {
          joinChains.add(JAXBUtils.getXJoinChainFromJoinChain(chain));
        }
      }
      return xJoinChains;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HealthStatus getHealthStatus() {
    boolean isHealthy = true;
    StringBuilder details = new StringBuilder();

    try {
      /** Try to issue command on hive **/
      Hive.get(LensServerConf.getHiveConf()).getAllDatabases();
    } catch (HiveException e) {
      isHealthy = false;
      details.append("Could not connect to Hive.");
      log.error("Could not connect to Hive.", e);
    }

    /** Check if service is up **/
    if (!this.getServiceState().equals(STATE.STARTED)) {
      isHealthy = false;
      details.append("Cube metastore service is down");
      log.error("Cube metastore service is down");
    }

    return isHealthy
      ? new HealthStatus(true, "Cube metastore service is healthy.")
      : new HealthStatus(false, details.toString());
  }
}
