package com.inmobi.grill.metastore.service;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.*;
import com.inmobi.grill.server.api.CubeMetastoreService;
import com.inmobi.grill.service.GrillService;
import com.inmobi.grill.service.session.GrillSessionImpl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Database;
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
    try {
      return ((GrillSessionImpl)getSessionManager().getSession(sessionid.getSessionHandle())).getCubeMetastoreClient();
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (HiveSQLException e) {
      throw new GrillException(e);
    }
  }


  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return
   */
  @Override
  public String getCurrentDatabase(GrillSessionHandle sessionid) throws GrillException {
    return getSession(sessionid.getSessionHandle()).getSessionState().getCurrentDatabase();
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  @Override
  public void setCurrentDatabase(GrillSessionHandle sessionid, String database) throws GrillException {
    getSession(sessionid.getSessionHandle()).getSessionState().setCurrentDatabase(database);
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
      acquire(sessionid.getSessionHandle());
      Hive.get(getUserConf()).dropDatabase(database, true, true, cascade);
      LOG.info("Database dropped " + database + " cascade? " + true);
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (NoSuchObjectException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
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
      acquire(sessionid.getSessionHandle());
      Database db = new Database();
      db.setName(database);
      Hive.get(getHiveConf()).createDatabase(db, ignore);
    } catch (AlreadyExistsException e) {
      throw new GrillException(e);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
    LOG.info("Database created " + database);
  }

  /**
   * @return get all database names
   */
  @Override
  public List<String> getAllDatabases(GrillSessionHandle sessionid) throws GrillException{
    try {
      acquire(sessionid.getSessionHandle());
      return Hive.get(getHiveConf()).getAllDatabases();
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
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
      acquire(sessionid.getSessionHandle());
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
      release(sessionid.getSessionHandle());
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
      acquire(sessionid.getSessionHandle());
      getClient(sessionid).createCube(JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Created cube " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
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
      acquire(sessionid.getSessionHandle());
      Cube c = getClient(sessionid).getCube(cubeName);
      if (c != null) {
        return JAXBUtils.xCubeFromHiveCube(c);
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
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
      acquire(sessionid.getSessionHandle());
      getClient(sessionid).dropCube(cubeName);
      LOG.info("Dropped cube " + cubeName + " cascade? " + cascade);
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
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
      acquire(sessionid.getSessionHandle());
      getClient(sessionid).alterCube(cube.getName(), JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Cube updated " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  /**
   * Create a cube dimension table based on JAXB object
   * @param xDimTable
   * @throws GrillException
   */
  @Override
  public void createCubeDimensionTable(GrillSessionHandle sessionid, DimensionTable xDimTable) throws GrillException {
    String dimName = xDimTable.getName();
    List<FieldSchema> columns = JAXBUtils.fieldSchemaListFromColumns(xDimTable.getColumns());
    Map<String, List<TableReference>> references =
      JAXBUtils.mapFromDimensionReferences(xDimTable.getDimensionsReferences());
    Map<String, UpdatePeriod> updatePeriodMap =
      JAXBUtils.dumpPeriodsFromUpdatePeriods(xDimTable.getUpdatePeriods());

    Map<String, String> properties = JAXBUtils.mapFromXProperties(xDimTable.getProperties());
    Map<Storage, StorageTableDesc> storageDesc = new HashMap<Storage, StorageTableDesc>();

    try {
      acquire(sessionid.getSessionHandle());
      System.out.println("# Columns: "+ columns);
      getClient(sessionid).createCubeDimensionTable(dimName,
        columns,
        xDimTable.getWeight(),
        references,
        updatePeriodMap,
        properties,
        new HashMap<Storage, StorageTableDesc>());
      LOG.info("Dimension Table created " + xDimTable.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void dropDimensionTable(GrillSessionHandle sessionid, String dimension, boolean cascade) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      getClient(sessionid).dropDimension(dimension, cascade);
      LOG.info("Dropped dimension table " + dimension + " cascade? " + cascade);
    } catch (HiveException e) {
    	LOG.error("@@@@ Got HiveException: >>>>>>>" + e.getMessage() + "<<<<<<<<<");
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }
  
  @Override
  public DimensionTable getDimensionTable(GrillSessionHandle sessionid, String dimName) throws GrillException {
  	try {
      acquire(sessionid.getSessionHandle());
  		CubeDimensionTable cubeDimTable = getClient(sessionid).getDimensionTable(dimName);
  		return JAXBUtils.dimTableFromCubeDimTable(cubeDimTable);
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }
  
  @Override
  public void updateDimensionTable(GrillSessionHandle sessionid, DimensionTable dimensionTable) throws GrillException {
  	try {
      acquire(sessionid.getSessionHandle());
  		getClient(sessionid).alterCubeDimensionTable(dimensionTable.getName(), 
  				JAXBUtils.cubeDimTableFromDimTable(dimensionTable));
  		LOG.info("Updated dimension table " + dimensionTable.getName());
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }
  
  @Override
  public Collection<String> getDimensionStorages(GrillSessionHandle sessionid, String dimension) throws GrillException {
  	try {
      acquire(sessionid.getSessionHandle());
  		CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimension);
  		return new ArrayList<String>(dimTable.getStorages());
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }
  
  @Override
  public void createDimensionStorage(GrillSessionHandle sessionid, String dimName, String updatePeriod, XStorage storageAttr) 
  throws GrillException {
  	try {
      acquire(sessionid.getSessionHandle());
	  	Storage storage = JAXBUtils.storageFromXStorage(storageAttr);
	  	CubeDimensionTable dimTable = getClient(sessionid).getDimensionTable(dimName);
	  	UpdatePeriod period = UpdatePeriod.valueOf(updatePeriod.toUpperCase());
	  	getClient(sessionid).addStorage(dimTable, storage, period, new StorageTableDesc());
	  	LOG.info("Added storage " + storageAttr.getName() + " for dimension table " + dimName
	  			+ " with update period " + period);
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }
  
  @Override
  public void dropAllStoragesOfDim(GrillSessionHandle sessionid, String dimName) throws GrillException {
  	try {
      acquire(sessionid.getSessionHandle());
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
      release(sessionid.getSessionHandle());
    }
  }
  
  @Override
  public XStorage getStorageOfDimension(GrillSessionHandle sessionid, String dimname, String storage) throws GrillException {
  	try {
      acquire(sessionid.getSessionHandle());
  		CubeDimensionTable tab = getClient(sessionid).getDimensionTable(dimname);
  		if (!tab.getStorages().contains(storage)) {
  			throw new NotFoundException("Storage " + storage + " not found for dimension " + dimname);
  		}
  		
  		XStorage xs = new XStorage();
  		xs.setName(storage);
  		return xs;
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void dropStorageOfDim(GrillSessionHandle sessionid, String dimName, String storage) throws GrillException {
  	try {
      acquire(sessionid.getSessionHandle());
  		CubeDimensionTable tab = getClient(sessionid).getDimensionTable(dimName);
  		if (!tab.getStorages().contains(storage)) {
  			throw new NotFoundException("Storage " + storage + " not found for dimension " + dimName);
  		}
  		
  		getClient(sessionid).dropStorageFromDim(dimName, storage);
  		LOG.info("Dropped storage " + storage + " from dimension table " + dimName);
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

	@Override
	public List<FactTable> getAllFactsOfCube(GrillSessionHandle sessionid, String cubeName) throws GrillException {
		try {
      acquire(sessionid.getSessionHandle());
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
      release(sessionid.getSessionHandle());
    }
		return null;
	}

	@Override
	public FactTable getFactTable(GrillSessionHandle sessionid, String fact) throws GrillException {
		try {
      acquire(sessionid.getSessionHandle());
			return JAXBUtils.factTableFromCubeFactTable(getClient(sessionid).getFactTable(fact));
		} catch (HiveException e) {
			throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
	}
	
	@Override
	public void createFactTable(GrillSessionHandle sessionid, FactTable fact) throws GrillException {
		Map<Storage, Set<UpdatePeriod>> updatePeriods = new HashMap<Storage, Set<UpdatePeriod>>();
		
		if (fact.getUpdatePeriods() != null 
				&& fact.getUpdatePeriods().getUpdatePeriodElement() != null) {
			for (UpdatePeriodElement uel : fact.getUpdatePeriods().getUpdatePeriodElement()) {
				Storage s = JAXBUtils.storageFromXStorage(uel.getStorageAttr());
				UpdatePeriod upd = UpdatePeriod.valueOf(uel.getUpdatePeriod().toUpperCase());
				Set<UpdatePeriod> periods = updatePeriods.get(s);
				if (periods == null) {
					periods = new HashSet<UpdatePeriod>();
					updatePeriods.put(s, periods);
				}
				periods.add(upd);
			}
		}
		
		try {
      // TODO Convert update period map to String, UpdatePeriod and
      // add <storage, StorageTableDesc> map
      acquire(sessionid.getSessionHandle());
			getClient(sessionid).createCubeFactTable(Arrays.asList(fact.getCubeName()),
					fact.getName(), 
					JAXBUtils.fieldSchemaListFromColumns(fact.getColumns()), 
					new HashMap<String, Set<UpdatePeriod>>(),
					fact.getWeight(), 
					JAXBUtils.mapFromXProperties(fact.getProperties()),
          new HashMap<Storage, StorageTableDesc>());
			LOG.info("Created fact table " + fact.getName());
		} catch (HiveException e) {
			throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
	}

	@Override
	public void updateFactTable(GrillSessionHandle sessionid, FactTable fact) throws GrillException {
		try {
      acquire(sessionid.getSessionHandle());
			getClient(sessionid).alterCubeFactTable(fact.getName(), JAXBUtils.cubeFactFromFactTable(fact));
			LOG.info("Updated fact table " + fact.getName());
		} catch (HiveException e) {
			throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }		
	}

	@Override
	public void dropFactTable(GrillSessionHandle sessionid, String fact, boolean cascade) throws GrillException {
		try {
      acquire(sessionid.getSessionHandle());
			getClient(sessionid).dropFact(fact, cascade);
			LOG.info("Dropped fact table " + fact + " cascade? " + cascade);
		} catch (HiveException e) {
			throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
	}

  @Override
  public List<String> getAllFactNames(GrillSessionHandle sessionid) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      List<CubeFactTable> facts = getClient(sessionid).getAllFacts();
      List<String> factNames = new ArrayList<String>(facts.size());
      for (CubeFactTable cft : facts) {
        factNames.add(cft.getName());
      }
      return factNames;
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public List<String> getStoragesOfFact(GrillSessionHandle sessionid, String fact) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
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
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void addStorageToFact(GrillSessionHandle sessionid, String fact, FactStorage s) throws GrillException {
    XStorage storage = s.getStorage();
    Set<UpdatePeriod> updatePeriods = new TreeSet<UpdatePeriod>();
    for (StorageUpdatePeriod sup : s.getStorageUpdatePeriod()) {
      updatePeriods.add(UpdatePeriod.valueOf(sup.getUpdatePeriod().toUpperCase()));
    }
    try {
      acquire(sessionid.getSessionHandle());
      getClient(sessionid).addStorage(getClient(sessionid).getFactTable(fact), JAXBUtils.storageFromXStorage(storage), updatePeriods, new StorageTableDesc());
      LOG.info("Added storage " + storage.getName() + ":" + updatePeriods + " for fact " + fact);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void dropStorageOfFact(GrillSessionHandle sessionid, String fact, String storage) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      checkFactStorage(sessionid, fact, storage);
      getClient(sessionid).dropStorageFromFact(fact, storage);
      LOG.info("Dropped storage " + storage + " from fact " + fact);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public FactStorage getStorageOfFact(GrillSessionHandle sessionid, String fact, String storage) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
        CubeFactTable cft = checkFactStorage(sessionid, fact, storage);
        XStorage xs = new XStorage();
        xs.setName(storage);
        // TODO set rest of the storage attributes here
        FactStorage f = new FactStorage();
        f.setStorage(xs);
        for (UpdatePeriod period : cft.getUpdatePeriods().get(storage)) {
          StorageUpdatePeriod sup = new StorageUpdatePeriod();
          sup.setUpdatePeriod(period.toString());
          f.getStorageUpdatePeriod().add(sup);
        }
        return f;

    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
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
  public void alterFactStorageUpdatePeriod(GrillSessionHandle sessionid, String fact, String storage, StorageUpdatePeriodList periods)
    throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      CubeFactTable factTable = checkFactStorage(sessionid, fact, storage);

      UpdatePeriod oldPeriods[] = factTable.getUpdatePeriods().get(storage).toArray(new UpdatePeriod[]{});
      for (UpdatePeriod old : oldPeriods) {
        factTable.removeUpdatePeriod(storage, old);
      }

      if (periods.getStorageUpdatePeriod() != null && !periods.getStorageUpdatePeriod().isEmpty()) {
        for (StorageUpdatePeriod p : periods.getStorageUpdatePeriod()) {
          factTable.addUpdatePeriod(storage, UpdatePeriod.valueOf(p.getUpdatePeriod().toUpperCase()));
        }
      }

      getClient(sessionid).alterCubeFactTable(fact, factTable);
      LOG.info("Altered update periods for storage:" + storage + " fact: " + fact + " periods: "
        + factTable.getUpdatePeriods().get(storage));
    } catch (HiveException e) {
      throw new GrillException(e);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public List<XPartition> getAllPartitionsOfFactStorage(GrillSessionHandle sessionid, String fact, String storage, String filter) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      checkFactStorage(sessionid, fact, storage);
      String storageTableName = MetastoreUtil.getFactStorageTableName(fact, Storage.getPrefix(storage));
      List<Partition> parts = getClient(sessionid).getPartitionsByFilter(storageTableName, filter);
      if (parts != null) {
        List<XPartition> result = new ArrayList<XPartition>(parts.size());
        for (Partition p : parts) {
          XPartition xp = new XPartition();
          xp.setName(p.getCompleteName());
          xp.setDataLocation(p.getDataLocation().toString());
          xp.setLocation(p.getLocation());

          for (Map.Entry<String, String> e : p.getSpec().entrySet()) {
            PartitionSpec spec = new PartitionSpec();
            spec.setKey(e.getKey());
            spec.setValue(e.getValue());
            xp.getPartitionSpec().add(spec);
          }

          for (String v : p.getValues()) {
            xp.getPartitionValues().add(v);
          }

          for (FieldSchema fs : p.getCols()) {
            xp.getPartitionColumns().add(fs.getName());
          }

          result.add(xp);
        }
        return result;
      } else {
        return new ArrayList<XPartition>();
      }
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void addPartitionToFactStorage(GrillSessionHandle sessionid, String fact, String storage, XPartition partition) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      CubeFactTable factTable = checkFactStorage(sessionid, fact, storage);

      Map<String, Date> partitionTimeStamps = new HashMap<String, Date>();
      for (PartitionTimeStamp ts : partition.getPartitionTimeStamp()) {
        partitionTimeStamps.put(ts.getColumn(), JAXBUtils.getDateFromXML(ts.getDate()));
      }

      Map<String, String> partitionSpec = new HashMap<String, String>();
      for (PartitionSpec spec : partition.getPartitionSpec()) {
        partitionSpec.put(spec.getKey(), spec.getValue());
      }

      // TODO consider for non-HDFS storages
      Storage str = Storage.createInstance("", "");

      StorageUpdatePeriod sup = partition.getUpdatePeriod();
      UpdatePeriod up = UpdatePeriod.valueOf(sup.getUpdatePeriod().toUpperCase());

      getClient(sessionid).addPartition(new StoragePartitionDesc(), str);
      LOG.info("Added partition for fact " + fact + " storage:" + storage
        + " dates: " + partitionTimeStamps + " spec:" + partitionSpec + " update period: " + up);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void dropPartitionsOfFactStorageByFilter(GrillSessionHandle sessionid, String fact, String storage, String filter) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      checkFactStorage(sessionid, fact, storage);
      Storage str = Storage.createInstance("TODO", "TODO");
      String storageTableName = MetastoreUtil.getFactStorageTableName(fact, str.getPrefix());
      List<Partition> partitions = getClient(sessionid).getPartitionsByFilter(storageTableName, filter);
      if (partitions != null) {
        for (int i = 0; i < partitions.size(); i++) {
          Partition p = partitions.get(i);
          //str.dropPartition(storageTableName, p.getValues(), getUserConf());
          LOG.info("Dropped partition [" + (i+1) + "/" + partitions.size() + "] " + p.getLocation());
        }
      } else {
        LOG.info("No partitions matching fact: " + fact + " storage: " + storage + " filter:" + filter);
      }
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void dropPartitionOfFactStorageByValue(GrillSessionHandle sessionid, String fact, String storage, String values) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      checkFactStorage(sessionid, fact, storage);
      String[] vals = StringUtils.split(values, ",");
      Storage s = Storage.createInstance("TODO", "TODO");
      //s.dropPartition(MetastoreUtil.getFactStorageTableName(fact, s.getPrefix()), Arrays.asList(vals), getUserConf());
      LOG.info("Dropped partition fact: " + fact + " storage: " + storage + " values:" + values);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
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
  public List<XPartition> getAllPartitionsOfDimStorage(GrillSessionHandle sessionid, String dimension, String storage, String filter)
    throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      checkDimensionStorage(sessionid, dimension, storage);
      Storage s = Storage.createInstance("TODO", "TODO");
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimension, s.getPrefix());
      List<Partition> partitions = getClient(sessionid).getPartitionsByFilter(storageTableName, filter);
      if (partitions != null) {
        List<XPartition> result = new ArrayList<XPartition>(partitions.size());
        for (Partition p : partitions) {
          XPartition xp = new XPartition();
          xp.setName(p.getCompleteName());
          xp.setDataLocation(p.getDataLocation().toString());
          xp.setLocation(p.getLocation());

          for (Map.Entry<String, String> e : p.getSpec().entrySet()) {
            PartitionSpec spec = new PartitionSpec();
            spec.setKey(e.getKey());
            spec.setValue(e.getValue());
            xp.getPartitionSpec().add(spec);
          }

          for (String v : p.getValues()) {
            xp.getPartitionValues().add(v);
          }

          for (FieldSchema fs : p.getCols()) {
            xp.getPartitionColumns().add(fs.getName());
          }

          result.add(xp);
        }
        return result;
      } else {
        return new ArrayList<XPartition>();
      }
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void addPartitionToDimStorage(GrillSessionHandle sessionid, String dimension, String storage, XPartition partition) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      CubeDimensionTable dim = checkDimensionStorage(sessionid, dimension, storage);
      Storage s = Storage.createInstance("TODO", "TODO");
      //getClient(sessionid).addPartition(dim, s, JAXBUtils.getDateFromXML(partition.getTimeStamp()));
      LOG.info("Added partition for dimension: " + dimension + " storage: " + storage);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void dropPartitionOfDimStorageByFilter(GrillSessionHandle sessionid, String dimension, String storage, String filter) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      checkDimensionStorage(sessionid, dimension, storage);
      Storage s = Storage.createInstance("TODO", "TODO");
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimension, s.getPrefix());
      List<Partition> partitions =
        getClient(sessionid).getPartitionsByFilter(storageTableName, filter);
      if (partitions != null && !partitions.isEmpty()) {
        int total = partitions.size();
        int i = 0;
        for (Partition p : partitions) {
          //s.dropPartition(storageTableName, p.getValues(), getUserConf());
          LOG.info("Dropped partition [" +  ++i + "/" + total + "]" + " for dimension: " + dimension +
          "storage: " + storage + " filter:" + filter + " partition:" + p.getValues());
        }
      }
      LOG.info("");
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }

  @Override
  public void dropPartitionOfDimStorageByValue(GrillSessionHandle sessionid, String dimension, String storage, String values) throws GrillException {
    try {
      acquire(sessionid.getSessionHandle());
      checkDimensionStorage(sessionid, dimension, storage);
      Storage s = Storage.createInstance("TODO", "TODO");
      //s.dropPartition(MetastoreUtil.getDimStorageTableName(dimension, s.getPrefix()),
      //  Arrays.asList(StringUtils.split(values, ",")), getUserConf());
      LOG.info("Dropped partition  for dimension: " + dimension +
        "storage: " + storage + " partition:" + values);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    } finally {
      release(sessionid.getSessionHandle());
    }
  }
}
