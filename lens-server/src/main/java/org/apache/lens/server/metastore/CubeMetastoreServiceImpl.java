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

import java.util.*;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.metastore.*;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.LensService;
import org.apache.lens.server.api.metastore.CubeMetastoreService;
import org.apache.lens.server.session.LensSessionImpl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class CubeMetastoreServiceImpl extends LensService implements CubeMetastoreService {
  public static final Logger LOG = LogManager.getLogger(CubeMetastoreServiceImpl.class);

  public CubeMetastoreServiceImpl(CLIService cliService) {
    super(NAME, cliService);
  }

  synchronized CubeMetastoreClient getClient(LensSessionHandle sessionid) throws LensException {
    return ((LensSessionImpl) getSession(sessionid)).getCubeMetastoreClient();
  }


  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return database name
   */
  @Override
  public String getCurrentDatabase(LensSessionHandle sessionid) throws LensException {
    try {
      acquire(sessionid);
      return getSession(sessionid).getCurrentDatabase();
    } finally {
      release(sessionid);
    }
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  @Override
  public void setCurrentDatabase(LensSessionHandle sessionid, String database) throws LensException {
    try {
      acquire(sessionid);
      if (!Hive.get(getSession(sessionid).getHiveConf()).databaseExists(database)) {
        throw new NotFoundException("Database " + database + " does not exist");
      }
      LOG.info("Set database " + database);
      getSession(sessionid).setCurrentDatabase(database);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
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
    try {
      acquire(sessionid);
      Hive.get(getSession(sessionid).getHiveConf()).dropDatabase(database, false, true, cascade);
      LOG.info("Database dropped " + database + " cascade? " + true);
    } catch (HiveException e) {
      throw new LensException(e);
    } catch (NoSuchObjectException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
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
    try {
      acquire(sessionid);
      Database db = new Database();
      db.setName(database);
      Hive.get(getSession(sessionid).getHiveConf()).createDatabase(db, ignore);
    } catch (AlreadyExistsException e) {
      throw new LensException(e);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    LOG.info("Database created " + database);
  }

  /**
   * @return get all database names
   */
  @Override
  public List<String> getAllDatabases(LensSessionHandle sessionid) throws LensException {
    try {
      acquire(sessionid);
      return Hive.get(getSession(sessionid).getHiveConf()).getAllDatabases();
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
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
    try {
      acquire(sessionid);
      List<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (CubeInterface cube : cubes) {
          names.add(cube.getName());
        }
        return names;
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  /**
   * Create cube based on the JAXB cube object
   *
   * @param cube
   * @throws LensException
   */
  @Override
  public void createCube(LensSessionHandle sessionid, XCube cube) throws LensException {
    try {
      acquire(sessionid);
      Cube parent = cube instanceof XDerivedCube ? (Cube) getClient(sessionid).getCube(
        ((XDerivedCube) cube).getParent()) : null;
      getClient(sessionid).createCube(JAXBUtils.hiveCubeFromXCube(cube, parent));
      LOG.info("Created cube " + cube.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    } catch (ParseException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Get a cube from the metastore
   *
   * @param cubeName
   * @return The cube object as {@link XCube}
   * @throws LensException
   */
  @Override
  public XCube getCube(LensSessionHandle sessionid, String cubeName) throws LensException {
    try {
      acquire(sessionid);
      CubeInterface c = getClient(sessionid).getCube(cubeName);
      if (c != null) {
        return JAXBUtils.xCubeFromHiveCube(c);
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  /**
   * Drop a cube from the metastore in the currently deleted database
   *
   * @param cubeName
   */
  public void dropCube(LensSessionHandle sessionid, String cubeName) throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropCube(cubeName);
      LOG.info("Dropped cube " + cubeName);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
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
    try {
      acquire(sessionid);
      Cube parent = cube instanceof XDerivedCube ? (Cube) getClient(sessionid).getCube(
        ((XDerivedCube) cube).getParent()) : null;
      getClient(sessionid).alterCube(cube.getName(), JAXBUtils.hiveCubeFromXCube(cube, parent));
      LOG.info("Cube updated " + cube.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    } catch (ParseException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Create a cube dimension table based on JAXB object
   *
   * @param xDimTable
   * @throws LensException
   */
  @Override
  public void createDimensionTable(LensSessionHandle sessionid, XDimensionTable xDimTable) throws LensException {
    String dimTblName = xDimTable.getTableName();
    List<FieldSchema> columns = JAXBUtils.fieldSchemaListFromColumns(xDimTable.getColumns());
    Map<String, UpdatePeriod> updatePeriodMap =
      JAXBUtils.dumpPeriodsFromStorageTables(xDimTable.getStorageTables());

    Map<String, String> properties = JAXBUtils.mapFromXProperties(xDimTable.getProperties());
    Map<String, StorageTableDesc> storageDesc = JAXBUtils.storageTableMapFromXStorageTables(
      xDimTable.getStorageTables());

    try {
      acquire(sessionid);
      LOG.info("# Columns: " + columns);
      getClient(sessionid).createCubeDimensionTable(xDimTable.getDimensionName(),
        dimTblName,
        columns,
        xDimTable.getWeight(),
        updatePeriodMap,
        properties,
        storageDesc);
      LOG.info("Dimension Table created " + xDimTable.getTableName());
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropDimensionTable(LensSessionHandle sessionid, String dimTblName, boolean cascade) throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropDimensionTable(dimTblName, cascade);
      LOG.info("Dropped dimension table " + dimTblName + " cascade? " + cascade);
    } catch (HiveException e) {
      LOG.error("@@@@ Got HiveException: >>>>>>>" + e.getMessage() + "<<<<<<<<<");
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public XDimensionTable getDimensionTable(LensSessionHandle sessionid, String dimTblName) throws LensException {
    try {
      acquire(sessionid);
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimTblName);
      XDimensionTable dt = JAXBUtils.dimTableFromCubeDimTable(dimTable);
      for (String storageName : dimTable.getStorages()) {
        XStorageTableElement tblElement = JAXBUtils.getXStorageTableFromHiveTable(
          getClient(sessionid).getHiveTable(MetastoreUtil.getDimStorageTableName(dimTblName, storageName)));
        tblElement.setStorageName(storageName);
        UpdatePeriod p = dimTable.getSnapshotDumpPeriods().get(storageName);
        if (p != null) {
          tblElement.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p.name()));
        }
        dt.getStorageTables().getStorageTable().add(tblElement);
      }
      return dt;
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void updateDimensionTable(LensSessionHandle sessionid, XDimensionTable dimensionTable) throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterCubeDimensionTable(dimensionTable.getTableName(),
        JAXBUtils.cubeDimTableFromDimTable(dimensionTable));
      // TODO alter storage tables
      LOG.info("Updated dimension table " + dimensionTable.getTableName());
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getDimTableStorages(LensSessionHandle sessionid, String dimension) throws LensException {
    try {
      acquire(sessionid);
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimension);
      return new ArrayList<String>(dimTable.getStorages());
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }


  @Override
  public void addDimTableStorage(LensSessionHandle sessionid,
    String dimTblName, XStorageTableElement storageTable) throws LensException {
    try {
      acquire(sessionid);
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimTblName);
      UpdatePeriod period = null;
      if (storageTable.getUpdatePeriods() != null && !storageTable.getUpdatePeriods().getUpdatePeriod().isEmpty()) {
        period = UpdatePeriod.valueOf(storageTable.getUpdatePeriods().getUpdatePeriod().get(0).name());
      }
      getClient(sessionid).addStorage(dimTable, storageTable.getStorageName(), period,
        JAXBUtils.storageTableDescFromXStorageTableDesc(storageTable.getTableDesc()));
      LOG.info("Added storage " + storageTable.getStorageName() + " for dimension table " + dimTblName
        + " with update period " + period);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropAllStoragesOfDimTable(LensSessionHandle sessionid, String dimTblName) throws LensException {
    try {
      acquire(sessionid);
      CubeDimensionTable tab = getClient(sessionid).getDimensionTable(dimTblName);
      int total = tab.getStorages().size();
      int i = 0;
      List<String> storageNames = new ArrayList<String>(tab.getStorages());
      for (String s : storageNames) {
        getClient(sessionid).dropStorageFromDim(dimTblName, s);
        LOG.info("Dropped storage " + s + " from dimension table " + dimTblName
          + " [" + ++i + "/" + total + "]");
      }
      LOG.info("Dropped " + total + " storages from dimension table " + dimTblName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropAllStoragesOfFact(LensSessionHandle sessionid, String factName) throws LensException {
    try {
      acquire(sessionid);
      CubeFactTable tab = getClient(sessionid).getFactTable(factName);
      int total = tab.getStorages().size();
      int i = 0;
      List<String> storageNames = new ArrayList<String>(tab.getStorages());
      for (String s : storageNames) {
        getClient(sessionid).dropStorageFromFact(factName, s);
        LOG.info("Dropped storage " + s + " from fact table " + factName
          + " [" + ++i + "/" + total + "]");
      }
      LOG.info("Dropped " + total + " storages from fact table " + factName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropStorageOfDimTable(LensSessionHandle sessionid, String dimTblName, String storage)
    throws LensException {
    try {
      acquire(sessionid);
      CubeDimensionTable tab = getClient(sessionid).getDimensionTable(dimTblName);
      if (!tab.getStorages().contains(storage)) {
        throw new NotFoundException("Storage " + storage + " not found for dimension " + dimTblName);
      }

      getClient(sessionid).dropStorageFromDim(dimTblName, storage);
      LOG.info("Dropped storage " + storage + " from dimension table " + dimTblName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<XFactTable> getAllFactsOfCube(LensSessionHandle sessionid, String cubeName) throws LensException {
    try {
      acquire(sessionid);
      List<CubeFactTable> cubeFacts = getClient(sessionid).getAllFactTables(getClient(sessionid).getCube(cubeName));
      if (cubeFacts != null && !cubeFacts.isEmpty()) {
        List<XFactTable> facts = new ArrayList<XFactTable>(cubeFacts.size());
        for (CubeFactTable cft : cubeFacts) {
          facts.add(JAXBUtils.factTableFromCubeFactTable(cft));
        }
        return facts;
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public XFactTable getFactTable(LensSessionHandle sessionid, String fact) throws LensException {
    try {
      acquire(sessionid);
      CubeFactTable cft = getClient(sessionid).getFactTable(fact);
      XFactTable factTable = JAXBUtils.factTableFromCubeFactTable(cft);
      for (String storageName : cft.getStorages()) {
        Set<UpdatePeriod> updatePeriods = cft.getUpdatePeriods().get(storageName);
        XStorageTableElement tblElement = JAXBUtils.getXStorageTableFromHiveTable(
          getClient(sessionid).getHiveTable(MetastoreUtil.getFactStorageTableName(fact, storageName)));
        tblElement.setStorageName(storageName);
        for (UpdatePeriod p : updatePeriods) {
          tblElement.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p.name()));
        }
        factTable.getStorageTables().getStorageTable().add(tblElement);
      }
      return factTable;
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void createFactTable(LensSessionHandle sessionid, XFactTable fact) throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).createCubeFactTable(fact.getCubeName(),
        fact.getName(),
        JAXBUtils.fieldSchemaListFromColumns(fact.getColumns()),
        JAXBUtils.getFactUpdatePeriodsFromStorageTables(fact.getStorageTables()),
        fact.getWeight(),
        JAXBUtils.mapFromXProperties(fact.getProperties()),
        JAXBUtils.storageTableMapFromXStorageTables(fact.getStorageTables()));
      LOG.info("Created fact table " + fact.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void updateFactTable(LensSessionHandle sessionid, XFactTable fact) throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterCubeFactTable(fact.getName(), JAXBUtils.cubeFactFromFactTable(fact));
      // TODO alter storage tables
      LOG.info("Updated fact table " + fact.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropFactTable(LensSessionHandle sessionid, String fact, boolean cascade) throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropFact(fact, cascade);
      LOG.info("Dropped fact table " + fact + " cascade? " + cascade);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getAllFactNames(LensSessionHandle sessionid) throws LensException {
    try {
      acquire(sessionid);
      List<CubeFactTable> facts = getClient(sessionid).getAllFacts();
      List<String> factNames = new ArrayList<String>(facts.size());
      for (CubeFactTable cft : facts) {
        factNames.add(cft.getName());
      }
      return factNames;
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getAllDimTableNames(LensSessionHandle sessionid) throws LensException {
    try {
      acquire(sessionid);
      List<CubeDimensionTable> dims = getClient(sessionid).getAllDimensionTables();
      List<String> dimNames = new ArrayList<String>(dims.size());
      for (CubeDimensionTable cdt : dims) {
        dimNames.add(cdt.getName());
      }
      return dimNames;
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getStoragesOfFact(LensSessionHandle sessionid, String fact) throws LensException {
    try {
      acquire(sessionid);
      if (!getClient(sessionid).isFactTable(fact)) {
        throw new NotFoundException("Not a fact table " + fact);
      }

      CubeFactTable cft = getClient(sessionid).getFactTable(fact);
      if (cft != null) {
        return new ArrayList<String>(cft.getStorages());
      } else {
        throw new NotFoundException("Could not get fact table " + fact);
      }
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  public XStorageTableElement getStorageOfFact(LensSessionHandle sessionid, String fact, String storageName)
    throws LensException {
    try {
      CubeFactTable factTable = getClient(sessionid).getFactTable(fact);
      Set<UpdatePeriod> updatePeriods = factTable.getUpdatePeriods().get(storageName);
      XStorageTableElement tblElement = JAXBUtils.getXStorageTableFromHiveTable(
        getClient(sessionid).getHiveTable(MetastoreUtil.getFactStorageTableName(fact, storageName)));
      tblElement.setStorageName(storageName);
      for (UpdatePeriod p : updatePeriods) {
        tblElement.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p.name()));
      }
      return tblElement;
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  public XStorageTableElement getStorageOfDim(LensSessionHandle sessionid, String dimTblName, String storageName)
    throws LensException {
    try {
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimTblName);
      XStorageTableElement tblElement = JAXBUtils.getXStorageTableFromHiveTable(
        getClient(sessionid).getHiveTable(MetastoreUtil.getDimStorageTableName(dimTblName, storageName)));
      tblElement.setStorageName(storageName);
      UpdatePeriod p = dimTable.getSnapshotDumpPeriods().get(storageName);
      if (p != null) {
        tblElement.getUpdatePeriods().getUpdatePeriod().add(XUpdatePeriod.valueOf(p.name()));
      }
      return tblElement;
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addStorageToFact(LensSessionHandle sessionid, String fact, XStorageTableElement storageTable)
    throws LensException {
    Set<UpdatePeriod> updatePeriods = new TreeSet<UpdatePeriod>();
    for (XUpdatePeriod sup : storageTable.getUpdatePeriods().getUpdatePeriod()) {
      updatePeriods.add(UpdatePeriod.valueOf(sup.name()));
    }
    try {
      acquire(sessionid);
      getClient(sessionid).addStorage(getClient(sessionid).getFactTable(fact),
        storageTable.getStorageName(), updatePeriods,
        JAXBUtils.storageTableDescFromXStorageTableElement(storageTable));
      LOG.info("Added storage " + storageTable.getStorageName() + ":" + updatePeriods + " for fact " + fact);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropStorageOfFact(LensSessionHandle sessionid, String fact, String storage) throws LensException {
    try {
      acquire(sessionid);
      checkFactStorage(sessionid, fact, storage);
      getClient(sessionid).dropStorageFromFact(fact, storage);
      LOG.info("Dropped storage " + storage + " from fact " + fact);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  private CubeFactTable checkFactStorage(LensSessionHandle sessionid, String fact, String storage)
    throws HiveException, LensException {
    CubeMetastoreClient client = getClient(sessionid);
    if (!client.isFactTable(fact)) {
      throw new NotFoundException("Fact table not found: " + fact);
    }

    CubeFactTable factTable = client.getFactTable(fact);

    if (!factTable.getStorages().contains(storage)) {
      throw new NotFoundException("Storage " + storage + " not found for fact table " + fact);
    }
    return factTable;
  }

  @Override
  public List<XPartition> getAllPartitionsOfFactStorage(
    LensSessionHandle sessionid, String fact, String storageName,
    String filter) throws LensException {
    try {
      acquire(sessionid);
      checkFactStorage(sessionid, fact, storageName);
      String storageTableName = MetastoreUtil.getFactStorageTableName(fact, storageName);
      List<Partition> parts = getClient(sessionid).getPartitionsByFilter(storageTableName, filter);
      if (parts != null) {
        List<XPartition> result = new ArrayList<XPartition>(parts.size());
        for (Partition p : parts) {
          XPartition xp = JAXBUtils.xpartitionFromPartition(p);
          result.add(xp);
        }
        return result;
      } else {
        return new ArrayList<XPartition>();
      }
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addPartitionToFactStorage(LensSessionHandle sessionid, String fact, String storageName,
    XPartition partition) throws LensException {
    try {
      acquire(sessionid);
      checkFactStorage(sessionid, fact, storageName);
      getClient(sessionid).addPartition(
        JAXBUtils.storagePartSpecFromXPartition(partition),
        storageName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addPartitionsToFactStorage(LensSessionHandle sessionid, String fact, String storageName,
    XPartitionList partitions) throws LensException {
    try {
      acquire(sessionid);
      checkFactStorage(sessionid, fact, storageName);
      getClient(sessionid).addPartitions(
        JAXBUtils.storagePartSpecListFromXPartitionList(partitions),
        storageName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  private CubeDimensionTable checkDimensionStorage(LensSessionHandle sessionid, String dimension, String storage)
    throws HiveException, LensException {
    CubeMetastoreClient client = getClient(sessionid);
    if (!client.isDimensionTable(dimension)) {
      throw new NotFoundException("Dimension table not found: " + dimension);
    }
    CubeDimensionTable cdt = client.getDimensionTable(dimension);
    if (!cdt.getStorages().contains(storage)) {
      throw new NotFoundException("Storage " + storage + " not found for dimension " + dimension);
    }
    return cdt;
  }

  @Override
  public List<XPartition> getAllPartitionsOfDimTableStorage(
    LensSessionHandle sessionid, String dimension, String storageName, String filter)
    throws LensException {
    try {
      acquire(sessionid);
      checkDimensionStorage(sessionid, dimension, storageName);
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimension,
        storageName);
      List<Partition> partitions = getClient(sessionid).getPartitionsByFilter(storageTableName, filter);
      if (partitions != null) {
        List<XPartition> result = new ArrayList<XPartition>(partitions.size());
        for (Partition p : partitions) {
          XPartition xp = JAXBUtils.xpartitionFromPartition(p);
          result.add(xp);
        }
        return result;
      } else {
        return new ArrayList<XPartition>();
      }
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addPartitionToDimStorage(LensSessionHandle sessionid,
    String dimTblName, String storageName, XPartition partition) throws LensException {
    try {
      acquire(sessionid);
      checkDimensionStorage(sessionid, dimTblName, storageName);
      getClient(sessionid).addPartition(
        JAXBUtils.storagePartSpecFromXPartition(partition),
        storageName);
      LOG.info("Added partition for dimension: " + dimTblName + " storage: " + storageName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addPartitionsToDimStorage(LensSessionHandle sessionid,
    String dimTblName, String storageName, XPartitionList partitions) throws LensException {
    try {
      acquire(sessionid);
      checkDimensionStorage(sessionid, dimTblName, storageName);
      getClient(sessionid).addPartitions(
        JAXBUtils.storagePartSpecListFromXPartitionList(partitions),
        storageName);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  private String getFilter(CubeMetastoreClient client, String tableName,
    String values) throws HiveException {
    List<FieldSchema> cols = client.getHiveTable(tableName).getPartCols();
    String[] vals = StringUtils.split(values, ",");
    if (vals.length != cols.size()) {
      LOG.error("Values for all the part columns not specified, cols:" + cols + " vals:" + vals);
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
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.putAll(p.getSpec());
    if (timePartColsStr != null) {
      String[] timePartCols = StringUtils.split(timePartColsStr, ',');
      for (String partCol : timePartCols) {
        String dateStr = partSpec.get(partCol);
        Date date = null;
        try {
          date = period.format().parse(dateStr);
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

  public void dropPartitionFromStorageByValues(LensSessionHandle sessionid,
    String cubeTableName, String storageName, String values) throws LensException {
    try {
      acquire(sessionid);
      String tableName = MetastoreUtil.getStorageTableName(cubeTableName,
        Storage.getPrefix(storageName));
      String filter = getFilter(getClient(sessionid), tableName, values);
      List<Partition> partitions = getClient(sessionid).getPartitionsByFilter(
        tableName, filter);
      if (partitions.size() > 1) {
        LOG.error("More than one partition with specified values, correspoding filter:" + filter);
        throw new BadRequestException("More than one partition with specified values");
      } else if (partitions.size() == 0) {
        LOG.error("No partition exists with specified values, correspoding filter:" + filter);
        throw new NotFoundException("No partition exists with specified values");
      }
      Map<String, Date> timeSpec = new HashMap<String, Date>();
      Map<String, String> nonTimeSpec = new HashMap<String, String>();
      UpdatePeriod updatePeriod = populatePartSpec(partitions.get(0), timeSpec, nonTimeSpec);
      getClient(sessionid).dropPartition(cubeTableName,
        storageName, timeSpec, nonTimeSpec, updatePeriod);
      LOG.info("Dropped partition  for dimension: " + cubeTableName
        + " storage: " + storageName + " values:" + values);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  public void dropPartitionFromStorageByFilter(LensSessionHandle sessionid, String cubeTableName,
    String storageName, String filter) throws LensException {
    try {
      acquire(sessionid);
      String tableName = MetastoreUtil.getStorageTableName(cubeTableName,
        Storage.getPrefix(storageName));
      List<Partition> partitions = getClient(sessionid).getPartitionsByFilter(
        tableName, filter);
      for (Partition part : partitions) {
        try {
          Map<String, Date> timeSpec = new HashMap<String, Date>();
          Map<String, String> nonTimeSpec = new HashMap<String, String>();
          UpdatePeriod updatePeriod = populatePartSpec(part, timeSpec, nonTimeSpec);
          getClient(sessionid).dropPartition(cubeTableName, storageName, timeSpec, nonTimeSpec, updatePeriod);
        } catch (HiveException e) {
          if (e.getCause() instanceof NoSuchObjectException) {
            continue;
          } else {
            throw new LensException(e);
          }
        }
      }
      LOG.info("Dropped partition  for cube table: " + cubeTableName
        + " storage: " + storageName + " by filter:" + filter);
    } catch (HiveException exc) {
      throw new LensException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void createStorage(LensSessionHandle sessionid, XStorage storage)
    throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).createStorage(JAXBUtils.storageFromXStorage(storage));
      LOG.info("Created storage " + storage.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }

  }

  @Override
  public void dropStorage(LensSessionHandle sessionid, String storageName)
    throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropStorage(storageName);
      LOG.info("Dropped storage " + storageName);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void alterStorage(LensSessionHandle sessionid, String storageName,
    XStorage storage) throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterStorage(storageName,
        JAXBUtils.storageFromXStorage(storage));
      LOG.info("Altered storage " + storageName);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public XStorage getStorage(LensSessionHandle sessionid, String storageName)
    throws LensException {
    try {
      acquire(sessionid);
      return JAXBUtils.xstorageFromStorage(getClient(sessionid).getStorage(storageName));
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getAllStorageNames(LensSessionHandle sessionid)
    throws LensException {
    try {
      acquire(sessionid);
      List<Storage> storages = getClient(sessionid).getAllStorages();
      if (storages != null && !storages.isEmpty()) {
        List<String> names = new ArrayList<String>(storages.size());
        for (Storage storage : storages) {
          names.add(storage.getName());
        }
        return names;
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public List<String> getAllBaseCubeNames(LensSessionHandle sessionid)
    throws LensException {
    try {
      acquire(sessionid);
      List<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (CubeInterface cube : cubes) {
          if (!cube.isDerivedCube()) {
            names.add(cube.getName());
          }
        }
        return names;
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public List<String> getAllDerivedCubeNames(LensSessionHandle sessionid)
    throws LensException {
    try {
      acquire(sessionid);
      List<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (CubeInterface cube : cubes) {
          if (cube.isDerivedCube()) {
            names.add(cube.getName());
          }
        }
        return names;
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public List<String> getAllQueryableCubeNames(LensSessionHandle sessionid)
    throws LensException {
    try {
      acquire(sessionid);
      List<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (CubeInterface cube : cubes) {
          if (cube.allFieldsQueriable()) {
            names.add(cube.getName());
          }
        }
        return names;
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public void createDimension(LensSessionHandle sessionid, XDimension dimension)
    throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).createDimension(JAXBUtils.dimensionFromXDimension(dimension));
      LOG.info("Created dimension " + dimension.getName());
    } catch (HiveException e) {
      throw new LensException(e);
    } catch (ParseException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public XDimension getDimension(LensSessionHandle sessionid, String dimName)
    throws LensException {
    try {
      acquire(sessionid);
      return JAXBUtils.xdimensionFromDimension(getClient(sessionid).getDimension(dimName));
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropDimension(LensSessionHandle sessionid, String dimName)
    throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropDimension(dimName);
      LOG.info("Dropped dimension " + dimName);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void updateDimension(LensSessionHandle sessionid, String dimName, XDimension dimension)
    throws LensException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterDimension(dimName,
        JAXBUtils.dimensionFromXDimension(dimension));
      LOG.info("Altered dimension " + dimName);
    } catch (HiveException e) {
      throw new LensException(e);
    } catch (ParseException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getAllDimensionNames(LensSessionHandle sessionid)
    throws LensException {
    try {
      acquire(sessionid);
      List<Dimension> dimensions = getClient(sessionid).getAllDimensions();
      if (dimensions != null && !dimensions.isEmpty()) {
        List<String> names = new ArrayList<String>(dimensions.size());
        for (Dimension dim : dimensions) {
          names.add(dim.getName());
        }
        return names;
      }
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public XNativeTable getNativeTable(LensSessionHandle sessionid, String name)
    throws LensException {
    try {
      acquire(sessionid);
      Table tbl = getClient(sessionid).getHiveTable(name);
      if (tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY) != null) {
        throw new BadRequestException(name + " is not a native table");
      }
      return JAXBUtils.nativeTableFromMetaTable(tbl);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
    }
  }

  private List<String> getTablesFromDB(LensSessionHandle sessionid,
    String dbName, boolean prependDbName)
    throws MetaException, UnknownDBException, HiveSQLException, TException, LensException {
    List<String> tables = getSession(sessionid).getMetaStoreClient().getAllTables(
      dbName);
    List<String> result = new ArrayList<String>();
    if (tables != null && !tables.isEmpty()) {
      Iterator<String> it = tables.iterator();
      while (it.hasNext()) {
        String tblName = it.next();
        org.apache.hadoop.hive.metastore.api.Table tbl =
          getSession(sessionid).getMetaStoreClient().getTable(dbName, tblName);
        if (tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY) == null) {
          if (prependDbName) {
            result.add(dbName + "." + tblName);
          } else {
            result.add(tblName);
          }
        }
      }
    }
    return result;
  }

  @Override
  public List<String> getAllNativeTableNames(LensSessionHandle sessionid,
    String dbOption, String dbName) throws LensException {
    try {
      acquire(sessionid);
      if (!StringUtils.isBlank(dbName)) {
        if (!Hive.get().databaseExists(dbName)) {
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
        tables = getTablesFromDB(sessionid, dbName, false);
      } else {
        LOG.info("Getting tables from all dbs");
        List<String> alldbs = getAllDatabases(sessionid);
        tables = new ArrayList<String>();
        for (String db : alldbs) {
          tables.addAll(getTablesFromDB(sessionid, db, true));
        }
      }
      return tables;
    } catch (HiveSQLException e) {
      throw new LensException(e);
    } catch (MetaException e) {
      throw new LensException(e);
    } catch (UnknownDBException e) {
      throw new NotFoundException("Database " + dbName + " does not exist");
    } catch (TException e) {
      throw new LensException(e);
    } catch (HiveException e) {
      throw new LensException(e);
    } finally {
      release(sessionid);
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

  private void addAllChainedColsToFlattenedListFromCube(CubeMetastoreClient client, ObjectFactory objectFactory,
    CubeInterface cube, List<XFlattenedColumn> columnList) throws HiveException {
    if (cube instanceof DerivedCube) {
      return;
    }
    addAllChainedColsToFlattenedList(client, objectFactory, (AbstractBaseTable) cube, columnList);
  }

  private void addAllChainedColsToFlattenedList(CubeMetastoreClient client, ObjectFactory objectFactory,
    AbstractBaseTable baseTbl, List<XFlattenedColumn> columnList) throws HiveException {
    for (JoinChain chain : baseTbl.getJoinChains()) {
      Dimension dim = client.getDimension(chain.getDestTable());
      addAllDirectAttributesToFlattenedListFromDimension(objectFactory, dim, columnList, chain.getName());
      addAllDirectExpressionsToFlattenedList(objectFactory, dim, columnList, chain.getName());
    }
  }

  @Override
  public XFlattenedColumns getFlattenedColumns(LensSessionHandle sessionHandle, String tableName) throws LensException {
    try {
      acquire(sessionHandle);
      CubeMetastoreClient client = getClient(sessionHandle);

      ObjectFactory objectFactory = new ObjectFactory();
      XFlattenedColumns flattenedColumns = objectFactory.createXFlattenedColumns();
      List<XFlattenedColumn> columnList = flattenedColumns.getFlattenedColumn();
      // check if the table is a cube or dimension
      if (client.isCube(tableName)) {
        CubeInterface cube = client.getCube(tableName);
        addAllMeasuresToFlattenedList(objectFactory, cube, columnList);
        addAllDirectAttributesToFlattenedListFromCube(objectFactory, cube, columnList);
        addAllDirectExpressionsToFlattenedList(objectFactory, (AbstractBaseTable) cube, columnList, null);
        addAllChainedColsToFlattenedListFromCube(client, objectFactory, cube, columnList);
      } else if (client.isDimension(tableName)) {
        Dimension dimension = client.getDimension(tableName);
        addAllDirectAttributesToFlattenedListFromDimension(objectFactory, dimension, columnList, null);
        addAllDirectExpressionsToFlattenedList(objectFactory, (AbstractBaseTable) dimension, columnList, null);
        addAllChainedColsToFlattenedList(client, objectFactory, dimension, columnList);
      } else {
        throw new BadRequestException("Can't get reachable columns. '"
          + tableName + "' is neither a cube nor a dimension");
      }
      return flattenedColumns;
    } catch (LensException exc) {
      throw exc;
    } catch (HiveException e) {
      throw new LensException("Error getting flattened view for " + tableName, e);
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public Date getLatestDateOfCube(LensSessionHandle sessionid, String cubeName, String timeDimension)
    throws LensException, HiveException {
    acquire(sessionid);
    // get the partitionColumn corresponding to timeDimension passed
    CubeInterface ci = getClient(sessionid).getCube(cubeName);
    if (!(ci instanceof Cube)) {
      throw new BadRequestException("cubeName : " + cubeName + " is not a base cube.");
    }
    Cube c = (Cube) ci;
    Date latest = getClient(sessionid).getLatestDateOfCube(c, timeDimension);
    release(sessionid);
    return latest;
  }
}
