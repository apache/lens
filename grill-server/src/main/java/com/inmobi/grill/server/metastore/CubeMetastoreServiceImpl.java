package com.inmobi.grill.server.metastore;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.metastore.CubeMetastoreService;
import com.inmobi.grill.server.session.GrillSessionImpl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.cube.metadata.*;
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

import java.util.*;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

public class CubeMetastoreServiceImpl extends GrillService implements CubeMetastoreService {
  public static final Logger LOG = LogManager.getLogger(CubeMetastoreServiceImpl.class);

  public CubeMetastoreServiceImpl(CLIService cliService) {
    super("metastore", cliService);
  }

  private HiveConf getUserConf() {
    return new HiveConf(CubeMetastoreServiceImpl.class);
  }

  synchronized CubeMetastoreClient getClient(GrillSessionHandle sessionid) throws GrillException {
    return ((GrillSessionImpl)getSession(sessionid)).getCubeMetastoreClient();
  }


  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return database name
   */
  @Override
  public String getCurrentDatabase(GrillSessionHandle sessionid) throws GrillException {
    return getSession(sessionid).getCurrentDatabase();
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  @Override
  public void setCurrentDatabase(GrillSessionHandle sessionid, String database) throws GrillException {
    try {
      if (!Hive.get().databaseExists(database)) {
        throw new NotFoundException("Database " + database + " does not exist");
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    }
    getSession(sessionid).setCurrentDatabase(database);
    LOG.info("Set database " + database);
  }

  /**
   * Drop a database from cube metastore
   *
   * @param database database name
   * @param cascade  flag indicating if the tables in the database should be dropped as well
   */
  @Override
  public void dropDatabase(GrillSessionHandle sessionid, String database, boolean cascade) throws GrillException {
    try {
      acquire(sessionid);
      Hive.get(getUserConf()).dropDatabase(database, false, true, cascade);
      LOG.info("Database dropped " + database + " cascade? " + true);
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (NoSuchObjectException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Create a database in Hive metastore
   * @param database database name
   * @param ignore ignore if database already exists
   * @throws GrillException
   */
  @Override
  public void createDatabase(GrillSessionHandle sessionid, String database, boolean ignore) throws GrillException {
    try {
      acquire(sessionid);
      Database db = new Database();
      db.setName(database);
      Hive.get(getHiveConf()).createDatabase(db, ignore);
    } catch (AlreadyExistsException e) {
      throw new GrillException(e);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    LOG.info("Database created " + database);
  }

  /**
   * @return get all database names
   */
  @Override
  public List<String> getAllDatabases(GrillSessionHandle sessionid) throws GrillException{
    try {
      acquire(sessionid);
      return Hive.get(getHiveConf()).getAllDatabases();
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Get list of all cubes names in the current database
   * 
   * @return List of cube names
   * 
   * @throws GrillException
   */
  @Override
  public List<String> getAllCubeNames(GrillSessionHandle sessionid) throws GrillException {
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
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  /**
   * Create cube based on the JAXB cube object
   * @param cube
   * @throws GrillException
   */
  @Override
  public void createCube(GrillSessionHandle sessionid, XCube cube) throws GrillException {
    try {
      acquire(sessionid);
      Cube parent = cube.isDerived() ? (Cube)getClient(sessionid).getCube(cube.getParent()) : null;
      getClient(sessionid).createCube(JAXBUtils.hiveCubeFromXCube(cube, parent));
      LOG.info("Created cube " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (ParseException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Get a cube from the metastore
   * 
   * @param cubeName
   * 
   * @return The cube object as {@link XCube}
   * @throws GrillException
   */
  @Override
  public XCube getCube(GrillSessionHandle sessionid, String cubeName) throws GrillException {
    try {
      acquire(sessionid);
      CubeInterface c = getClient(sessionid).getCube(cubeName);
      if (c != null) {
        return JAXBUtils.xCubeFromHiveCube(c);
      }
    } catch (HiveException e) {
      throw new GrillException(e);
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
  public void dropCube(GrillSessionHandle sessionid, String cubeName) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropCube(cubeName);
      LOG.info("Dropped cube " + cubeName);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Update cube
   * @param cube JAXB Cube object
   * @throws GrillException
   */
  @Override
  public void updateCube(GrillSessionHandle sessionid, XCube cube) throws GrillException {
    try {
      acquire(sessionid);
      Cube parent = cube.isDerived() ? (Cube)getClient(sessionid).getCube(cube.getParent()) : null;
      getClient(sessionid).alterCube(cube.getName(), JAXBUtils.hiveCubeFromXCube(cube, parent));
      LOG.info("Cube updated " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (ParseException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Create a cube dimension table based on JAXB object
   * @param xDimTable
   * @throws GrillException
   */
  @Override
  public void createCubeDimensionTable(GrillSessionHandle sessionid, DimensionTable xDimTable, XStorageTables storageTables) throws GrillException {
    String dimTblName = xDimTable.getTableName();
    List<FieldSchema> columns = JAXBUtils.fieldSchemaListFromColumns(xDimTable.getColumns());
    Map<String, UpdatePeriod> updatePeriodMap =
        JAXBUtils.dumpPeriodsFromUpdatePeriods(xDimTable.getStorageDumpPeriods());

    Map<String, String> properties = JAXBUtils.mapFromXProperties(xDimTable.getProperties());
    Map<String, StorageTableDesc> storageDesc = JAXBUtils.storageTableMapFromXStorageTables(storageTables);

    try {
      acquire(sessionid);
      System.out.println("# Columns: "+ columns);
      getClient(sessionid).createCubeDimensionTable(xDimTable.getDimName(),
          dimTblName,
          columns,
          xDimTable.getWeight(),
          updatePeriodMap,
          properties,
          storageDesc);
      LOG.info("Dimension Table created " + xDimTable.getTableName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropDimensionTable(GrillSessionHandle sessionid, String dimTblName, boolean cascade) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropDimensionTable(dimTblName, cascade);
      LOG.info("Dropped dimension table " + dimTblName + " cascade? " + cascade);
    } catch (HiveException e) {
      LOG.error("@@@@ Got HiveException: >>>>>>>" + e.getMessage() + "<<<<<<<<<");
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public DimensionTable getDimensionTable(GrillSessionHandle sessionid, String dimTblName) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable cubeDimTable = getClient(sessionid).getDimensionTable(dimTblName);
      return JAXBUtils.dimTableFromCubeDimTable(cubeDimTable);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void updateDimensionTable(GrillSessionHandle sessionid, DimensionTable dimensionTable) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterCubeDimensionTable(dimensionTable.getTableName(), 
          JAXBUtils.cubeDimTableFromDimTable(dimensionTable));
      LOG.info("Updated dimension table " + dimensionTable.getTableName());
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getDimTableStorages(GrillSessionHandle sessionid, String dimension) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimension);
      return new ArrayList<String>(dimTable.getStorages());
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  
  @Override
  public void createDimTableStorage(GrillSessionHandle sessionid,
      String dimTblName, XStorageTableElement storageTable) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimTblName);
      UpdatePeriod period = null;
      if (!storageTable.getUpdatePeriods().isEmpty()) {
        period = UpdatePeriod.valueOf(storageTable.getUpdatePeriods().get(0).toUpperCase());
      }
      getClient(sessionid).addStorage(dimTable, storageTable.getStorageName(), period,
          JAXBUtils.storageTableDescFromXStorageTableDesc(storageTable.getTableDesc()));
      LOG.info("Added storage " + storageTable.getStorageName() + " for dimension table " + dimTblName
          + " with update period " + period);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropAllStoragesOfDimTable(GrillSessionHandle sessionid, String dimTblName) throws GrillException {
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
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropAllStoragesOfFact(GrillSessionHandle sessionid, String factName) throws GrillException {
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
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropStorageOfDimTable(GrillSessionHandle sessionid, String dimTblName, String storage) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable tab = getClient(sessionid).getDimensionTable(dimTblName);
      if (!tab.getStorages().contains(storage)) {
        throw new NotFoundException("Storage " + storage + " not found for dimension " + dimTblName);
      }

      getClient(sessionid).dropStorageFromDim(dimTblName, storage);
      LOG.info("Dropped storage " + storage + " from dimension table " + dimTblName);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<FactTable> getAllFactsOfCube(GrillSessionHandle sessionid, String cubeName) throws GrillException {
    try {
      acquire(sessionid);
      List<CubeFactTable> cubeFacts = getClient(sessionid).getAllFactTables(getClient(sessionid).getCube(cubeName));
      if (cubeFacts != null && !cubeFacts.isEmpty()) {
        List<FactTable> facts = new ArrayList<FactTable>(cubeFacts.size());
        for (CubeFactTable cft : cubeFacts) {
          facts.add(JAXBUtils.factTableFromCubeFactTable(cft));
        }
        return facts;
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public FactTable getFactTable(GrillSessionHandle sessionid, String fact) throws GrillException {
    try {
      acquire(sessionid);
      return JAXBUtils.factTableFromCubeFactTable(getClient(sessionid).getFactTable(fact));
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void createFactTable(GrillSessionHandle sessionid, FactTable fact, XStorageTables storageTables) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).createCubeFactTable(fact.getCubeName(),
          fact.getName(), 
          JAXBUtils.fieldSchemaListFromColumns(fact.getColumns()), 
          JAXBUtils.getFactUpdatePeriodsFromUpdatePeriods(fact.getStorageUpdatePeriods()),
          fact.getWeight(), 
          JAXBUtils.mapFromXProperties(fact.getProperties()),
          JAXBUtils.storageTableMapFromXStorageTables(storageTables));
      LOG.info("Created fact table " + fact.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void updateFactTable(GrillSessionHandle sessionid, FactTable fact) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterCubeFactTable(fact.getName(), JAXBUtils.cubeFactFromFactTable(fact));
      LOG.info("Updated fact table " + fact.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }		
  }

  @Override
  public void dropFactTable(GrillSessionHandle sessionid, String fact, boolean cascade) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropFact(fact, cascade);
      LOG.info("Dropped fact table " + fact + " cascade? " + cascade);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getAllFactNames(GrillSessionHandle sessionid) throws GrillException {
    try {
      acquire(sessionid);
      List<CubeFactTable> facts = getClient(sessionid).getAllFacts();
      List<String> factNames = new ArrayList<String>(facts.size());
      for (CubeFactTable cft : facts) {
        factNames.add(cft.getName());
      }
      return factNames;
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getAllDimTableNames(GrillSessionHandle sessionid) throws GrillException {
    try {
      acquire(sessionid);
      List<CubeDimensionTable> dims = getClient(sessionid).getAllDimensionTables();
      List<String> dimNames = new ArrayList<String>(dims.size());
      for (CubeDimensionTable cdt : dims) {
        dimNames.add(cdt.getName());
      }
      return dimNames;
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getStoragesOfFact(GrillSessionHandle sessionid, String fact) throws GrillException {
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
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  public XStorageTableElement getStorageOfFact(GrillSessionHandle sessionid, String fact, String storageName) throws GrillException {
    try {
      CubeFactTable factTable = getClient(sessionid).getFactTable(fact);
      Set<UpdatePeriod> updatePeriods = factTable.getUpdatePeriods().get(storageName);
      XStorageTableElement tblElement =  JAXBUtils.getXStorageTableFromHiveTable(
          getClient(sessionid).getHiveTable(MetastoreUtil.getFactStorageTableName(fact, storageName)));
      tblElement.setStorageName(storageName);
      for (UpdatePeriod p : updatePeriods) {
        tblElement.getUpdatePeriods().add(p.name());
      }
      return tblElement;
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  public XStorageTableElement getStorageOfDim(GrillSessionHandle sessionid, String dimTblName, String storageName) throws GrillException {
    try {
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimTblName);
      XStorageTableElement tblElement =  JAXBUtils.getXStorageTableFromHiveTable(
          getClient(sessionid).getHiveTable(MetastoreUtil.getDimStorageTableName(dimTblName, storageName)));
      tblElement.setStorageName(storageName);
      UpdatePeriod p = dimTable.getSnapshotDumpPeriods().get(storageName);
      if (p != null) {
        tblElement.getUpdatePeriods().add(p.name());
      }
      return tblElement;
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addStorageToFact(GrillSessionHandle sessionid, String fact, XStorageTableElement storageTable) throws GrillException {
    Set<UpdatePeriod> updatePeriods = new TreeSet<UpdatePeriod>();
    for (String sup : storageTable.getUpdatePeriods()) {
      updatePeriods.add(UpdatePeriod.valueOf(sup.toUpperCase()));
    }
    try {
      acquire(sessionid);
      getClient(sessionid).addStorage(getClient(sessionid).getFactTable(fact),
          storageTable.getStorageName(), updatePeriods,
          JAXBUtils.storageTableDescFromXStorageTableElement(storageTable));
      LOG.info("Added storage " + storageTable.getStorageName() + ":" + updatePeriods + " for fact " + fact);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropStorageOfFact(GrillSessionHandle sessionid, String fact, String storage) throws GrillException {
    try {
      acquire(sessionid);
      checkFactStorage(sessionid, fact, storage);
      getClient(sessionid).dropStorageFromFact(fact, storage);
      LOG.info("Dropped storage " + storage + " from fact " + fact);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  private CubeFactTable checkFactStorage(GrillSessionHandle sessionid, String fact, String storage) throws HiveException, GrillException {
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
      GrillSessionHandle sessionid, String fact, String storageName,
      String filter) throws GrillException {
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
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addPartitionToFactStorage(GrillSessionHandle sessionid, String fact, String storageName, XPartition partition) throws GrillException {
    try {
      acquire(sessionid);
      CubeFactTable factTable = checkFactStorage(sessionid, fact, storageName);
      getClient(sessionid).addPartition(
          JAXBUtils.storagePartSpecFromXPartition(partition),
          storageName);
      LOG.info("Added partition for fact " + fact + " on storage:" + storageName
          + " dates: " + partition.getTimePartitionSpec() + " spec:" +
          partition.getNonTimePartitionSpec() + " update period: " + partition.getUpdatePeriod());
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  private CubeDimensionTable checkDimensionStorage(GrillSessionHandle sessionid, String dimension, String storage)
      throws HiveException, GrillException {
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
      GrillSessionHandle sessionid, String dimension, String storageName, String filter)
          throws GrillException {
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
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void addPartitionToDimStorage(GrillSessionHandle sessionid,
      String dimTblName, String storageName, XPartition partition) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable dim = checkDimensionStorage(sessionid, dimTblName, storageName);
      getClient(sessionid).addPartition(
          JAXBUtils.storagePartSpecFromXPartition(partition),
          storageName);
      LOG.info("Added partition for dimension: " + dimTblName + " storage: " + storageName);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropPartitionFromStorage(GrillSessionHandle sessionid,
      String cubeTableName, String storageName, XTimePartSpec timePartSpec,
      XPartSpec nonTimePartSpec, String updatePeriod) throws GrillException {
    try {
      acquire(sessionid);
      checkDimensionStorage(sessionid, cubeTableName, storageName);
      getClient(sessionid).dropPartition(cubeTableName,
          storageName,
          JAXBUtils.timePartSpecfromXTimePartSpec(timePartSpec),
          JAXBUtils.nonTimePartSpecfromXNonTimePartSpec(nonTimePartSpec),
          UpdatePeriod.valueOf(updatePeriod.toUpperCase()));
      LOG.info("Dropped partition  for dimension: " + cubeTableName +
          "storage: " + storageName + " partition:" + timePartSpec + " " + nonTimePartSpec);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  private String getFilter(CubeMetastoreClient client, String tableName,
      String values) throws HiveException {
    List<FieldSchema> cols = client.getHiveTable(tableName).getPartCols();
    String[] vals = StringUtils.split(values, ",");
    if (vals.length != cols.size()) {
      LOG.error("Values for all the part columns not specified, cols:" + cols + " vals:" + vals );
      throw new BadRequestException("Values for all the part columns not specified");
    }
    StringBuilder filter = new StringBuilder();
    for (int i = 0; i < vals.length; i++) {
      filter.append(cols.get(i).getName());
      filter.append("=");
      filter.append("\"");
      filter.append(vals[i]);
      filter.append("\"");
      if (i != (vals.length -1)) {
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

  public void dropPartitionFromStorageByValues(GrillSessionHandle sessionid,
      String cubeTableName, String storageName, String values) throws GrillException {
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
      LOG.info("Dropped partition  for dimension: " + cubeTableName +
          " storage: " + storageName + " values:" + values);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }    
  }

  public void dropPartitionFromStorageByFilter(GrillSessionHandle sessionid, String cubeTableName,
      String storageName, String filter) throws GrillException {
    try {
      acquire(sessionid);
      String tableName = MetastoreUtil.getStorageTableName(cubeTableName,
          Storage.getPrefix(storageName));
      List<Partition> partitions = getClient(sessionid).getPartitionsByFilter(
          tableName, filter);
      for (Partition part : partitions) {
        Map<String, Date> timeSpec = new HashMap<String, Date>();
        Map<String, String> nonTimeSpec = new HashMap<String, String>();
        UpdatePeriod updatePeriod = populatePartSpec(part, timeSpec, nonTimeSpec);
        getClient(sessionid).dropPartition(cubeTableName,
            storageName, timeSpec, nonTimeSpec,
            updatePeriod);
      }
      LOG.info("Dropped partition  for cube table: " + cubeTableName +
          " storage: " + storageName + " by filter:" + filter);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    } 
  }

  @Override
  public void createStorage(GrillSessionHandle sessionid, XStorage storage)
      throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).createStorage(JAXBUtils.storageFromXStorage(storage));
      LOG.info("Created storage " + storage.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }

  }

  @Override
  public void dropStorage(GrillSessionHandle sessionid, String storageName)
      throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropStorage(storageName);
      LOG.info("Dropped storage " + storageName);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }    
  }

  @Override
  public void alterStorage(GrillSessionHandle sessionid, String storageName,
      XStorage storage) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterStorage(storageName,
          JAXBUtils.storageFromXStorage(storage));
      LOG.info("Altered storage " + storageName);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }        
  }

  @Override
  public XStorage getStorage(GrillSessionHandle sessionid, String storageName)
      throws GrillException {
    try {
      acquire(sessionid);
      return JAXBUtils.xstorageFromStorage(getClient(sessionid).getStorage(storageName));
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }        
  }

  @Override
  public List<String> getAllStorageNames(GrillSessionHandle sessionid)
      throws GrillException {
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
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public List<String> getAllBaseCubeNames(GrillSessionHandle sessionid)
      throws GrillException {
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
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public List<String> getAllDerivedCubeNames(GrillSessionHandle sessionid)
      throws GrillException {
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
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public List<String> getAllQueryableCubeNames(GrillSessionHandle sessionid)
      throws GrillException {
    try {
      acquire(sessionid);
      List<CubeInterface> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (CubeInterface cube : cubes) {
          if (cube.canBeQueried()) {
            names.add(cube.getName());
          }
        }
        return names;
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public void createDimension(GrillSessionHandle sessionid, XDimension dimension)
      throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).createDimension(JAXBUtils.dimensionFromXDimension(dimension));
      LOG.info("Created dimension " + dimension.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (ParseException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public XDimension getDimension(GrillSessionHandle sessionid, String dimName)
      throws GrillException {
    try {
      acquire(sessionid);
      return JAXBUtils.xdimensionFromDimension(getClient(sessionid).getDimension(dimName));
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropDimension(GrillSessionHandle sessionid, String dimName)
      throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropDimension(dimName);
      LOG.info("Dropped dimension " + dimName);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void updateDimension(GrillSessionHandle sessionid, String dimName, XDimension dimension)
      throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).alterDimension(dimName,
          JAXBUtils.dimensionFromXDimension(dimension));
      LOG.info("Altered dimension " + dimName);
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (ParseException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getAllDimensionNames(GrillSessionHandle sessionid)
      throws GrillException {
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
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
    return null;
  }

  @Override
  public NativeTable getNativeTable(GrillSessionHandle sessionid, String name)
      throws GrillException {
    try {
      acquire(sessionid);
      Table tbl = getClient(sessionid).getHiveTable(name);
      if (tbl.getParameters().get(MetastoreConstants.TABLE_TYPE_KEY) != null) {
        throw new BadRequestException(name + " is not a native table");
      }
      return JAXBUtils.nativeTableFromMetaTable(tbl);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  private List<String> getTablesFromDB(GrillSessionHandle sessionid,
      String dbName, boolean prependDbName)
      throws MetaException, UnknownDBException, HiveSQLException, TException, GrillException {
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
  public List<String> getAllNativeTableNames(GrillSessionHandle sessionid,
      String dbOption, String dbName) throws GrillException {
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
      throw new GrillException(e);
    } catch (MetaException e) {
      throw new GrillException(e);
    } catch (UnknownDBException e) {
      throw new NotFoundException("Database " + dbName + " does not exist");
    } catch (TException e) {
      throw new GrillException(e);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

}
