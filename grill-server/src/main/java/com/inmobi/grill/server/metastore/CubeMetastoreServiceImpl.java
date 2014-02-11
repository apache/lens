package com.inmobi.grill.server.metastore;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.metastore.CubeMetastoreService;
import com.inmobi.grill.server.session.GrillSessionImpl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.session.SessionState;
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
   * @return
   */
  @Override
  public String getCurrentDatabase(GrillSessionHandle sessionid) throws GrillException {
    return getSession(sessionid).getSessionState().getCurrentDatabase();
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  @Override
  public void setCurrentDatabase(GrillSessionHandle sessionid, String database) throws GrillException {
    getSession(sessionid).getSessionState().setCurrentDatabase(database);
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
      Hive.get(getUserConf()).dropDatabase(database, true, true, cascade);
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
   * @return
   * @throws GrillException
   */
  @Override
  public List<String> getAllCubeNames(GrillSessionHandle sessionid) throws GrillException {
    try {
      acquire(sessionid);
      List<Cube> cubes = getClient(sessionid).getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (Cube cube : cubes) {
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
      getClient(sessionid).createCube(JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Created cube " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  /**
   * Get a cube from the metastore
   * @param cubeName
   * @return
   * @throws GrillException
   */
  @Override
  public XCube getCube(GrillSessionHandle sessionid, String cubeName) throws GrillException {
    try {
      acquire(sessionid);
      Cube c = getClient(sessionid).getCube(cubeName);
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
   * @param cubeName
   * @param cascade
   */
  public void dropCube(GrillSessionHandle sessionid, String cubeName, boolean cascade) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropCube(cubeName);
      LOG.info("Dropped cube " + cubeName + " cascade? " + cascade);
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
      getClient(sessionid).alterCube(cube.getName(), JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Cube updated " + cube.getName());
    } catch (HiveException e) {
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
    String dimName = xDimTable.getName();
    List<FieldSchema> columns = JAXBUtils.fieldSchemaListFromColumns(xDimTable.getColumns());
    Map<String, List<TableReference>> references =
        JAXBUtils.mapFromDimensionReferences(xDimTable.getDimensionsReferences());
    Map<String, UpdatePeriod> updatePeriodMap =
        JAXBUtils.dumpPeriodsFromUpdatePeriods(xDimTable.getStorageDumpPeriods());

    Map<String, String> properties = JAXBUtils.mapFromXProperties(xDimTable.getProperties());
    Map<String, StorageTableDesc> storageDesc = JAXBUtils.storageTableMapFromXStorageTables(storageTables);

    try {
      acquire(sessionid);
      System.out.println("# Columns: "+ columns);
      getClient(sessionid).createCubeDimensionTable(dimName,
          columns,
          xDimTable.getWeight(),
          references,
          updatePeriodMap,
          properties,
          storageDesc);
      LOG.info("Dimension Table created " + xDimTable.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropDimensionTable(GrillSessionHandle sessionid, String dimension, boolean cascade) throws GrillException {
    try {
      acquire(sessionid);
      getClient(sessionid).dropDimension(dimension, cascade);
      LOG.info("Dropped dimension table " + dimension + " cascade? " + cascade);
    } catch (HiveException e) {
      LOG.error("@@@@ Got HiveException: >>>>>>>" + e.getMessage() + "<<<<<<<<<");
      throw new GrillException(e);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public DimensionTable getDimensionTable(GrillSessionHandle sessionid, String dimName) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable cubeDimTable = getClient(sessionid).getDimensionTable(dimName);
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
      getClient(sessionid).alterCubeDimensionTable(dimensionTable.getName(), 
          JAXBUtils.cubeDimTableFromDimTable(dimensionTable));
      LOG.info("Updated dimension table " + dimensionTable.getName());
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public List<String> getDimensionStorages(GrillSessionHandle sessionid, String dimension) throws GrillException {
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
  public void createDimensionStorage(GrillSessionHandle sessionid,
      String dimName, XStorageTableElement storageTable) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimName);
      UpdatePeriod period = null;
      if (!storageTable.getUpdatePeriods().isEmpty()) {
        period = UpdatePeriod.valueOf(storageTable.getUpdatePeriods().get(0).toUpperCase());
      }
      getClient(sessionid).addStorage(dimTable, storageTable.getStorageName(), period,
          JAXBUtils.storageTableDescFromXStorageTableDesc(storageTable.getTableDesc()));
      LOG.info("Added storage " + storageTable.getStorageName() + " for dimension table " + dimName
          + " with update period " + period);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropAllStoragesOfDim(GrillSessionHandle sessionid, String dimName) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable tab = getClient(sessionid).getDimensionTable(dimName);
      int total = tab.getStorages().size();
      int i = 0;
      List<String> storageNames = new ArrayList<String>(tab.getStorages());
      for (String s : storageNames) {
        getClient(sessionid).dropStorageFromDim(dimName, s);
        LOG.info("Dropped storage " + s + " from dimension table " + dimName 
            + " [" + ++i + "/" + total + "]");
      }
      LOG.info("Dropped " + total + " storages from dimension table " + dimName);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid);
    }
  }

  @Override
  public void dropStorageOfDim(GrillSessionHandle sessionid, String dimName, String storage) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable tab = getClient(sessionid).getDimensionTable(dimName);
      if (!tab.getStorages().contains(storage)) {
        throw new NotFoundException("Storage " + storage + " not found for dimension " + dimName);
      }

      getClient(sessionid).dropStorageFromDim(dimName, storage);
      LOG.info("Dropped storage " + storage + " from dimension table " + dimName);
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
      getClient(sessionid).createCubeFactTable(Arrays.asList(fact.getCubeName()),
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
  public List<XPartition> getAllPartitionsOfDimStorage(
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
      String dimName, String storageName, XPartition partition) throws GrillException {
    try {
      acquire(sessionid);
      CubeDimensionTable dim = checkDimensionStorage(sessionid, dimName, storageName);
      getClient(sessionid).addPartition(
          JAXBUtils.storagePartSpecFromXPartition(partition),
          storageName);
      LOG.info("Added partition for dimension: " + dimName + " storage: " + storageName);
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
}
