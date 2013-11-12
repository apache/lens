package com.inmobi.grill.metastore.service;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.*;
import com.inmobi.grill.server.api.CubeMetastoreService;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

import javax.ws.rs.NotFoundException;

public class CubeMetastoreServiceImpl implements CubeMetastoreService, Configurable {
  public static final Logger LOG = LogManager.getLogger(CubeMetastoreServiceImpl.class);

  private String user;
  private CubeMetastoreClient client;
  private SessionState sessionState;
  private HiveConf userConf;
  private Configuration conf;

  private static final Map<String, CubeMetastoreServiceImpl> instances =
    new HashMap<String, CubeMetastoreServiceImpl>();

  public synchronized static CubeMetastoreService getInstance(String user) {
    if (!instances.containsKey(user)) {
      CubeMetastoreServiceImpl instance = new CubeMetastoreServiceImpl();
      instance.user = user;
      instances.put(user, instance);
    }
    return instances.get(user);
  }

  public CubeMetastoreServiceImpl() {
    userConf = new HiveConf(CubeMetastoreServiceImpl.class);
    sessionState = new SessionState(userConf); 
  }

  @Override
  public String getName() {
    return "metastore";
  }

  @Override
  public void start() throws GrillException {
    LOG.info("Starting cube metastore service");
  }

  @Override
  public void stop() throws GrillException {
    LOG.info("Stopping cube metastore service");
  }

  private HiveConf getUserConf() {
    return new HiveConf(CubeMetastoreServiceImpl.class);
  }

  synchronized CubeMetastoreClient getClient() throws GrillException {
    if (client == null) {
      try {
        LOG.info("Create new CubeMetastoreClient");
        client = CubeMetastoreClient.getInstance(userConf);
      } catch (HiveException e) {
        throw new GrillException(e);
      }
    }

    // Start session state for the current thread
    SessionState.start(sessionState);
    return client;
  }


  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return
   */
  @Override
  public String getCurrentDatabase() throws GrillException {
    return getClient().getCurrentDatabase();
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  @Override
  public void setCurrentDatabase(String database) throws GrillException {
    getClient().setCurrentDatabase(database);
    LOG.info("Set database " + database);
  }

  /**
   * Drop a database from cube metastore
   *
   * @param database database name
   * @param cascade  flag indicating if the tables in the database should be dropped as well
   */
  @Override
  public void dropDatabase(String database, boolean cascade) throws GrillException {
    try {
      Hive.get(getUserConf()).dropDatabase(database, true, true, cascade);
      LOG.info("Database dropped " + database + " cascade? " + true);
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (NoSuchObjectException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Create a database in Hive metastore
   * @param database database name
   * @param ignore ignore if database already exists
   * @throws GrillException
   */
  @Override
  public void createDatabase(String database, boolean ignore) throws GrillException {
    try {
      Database db = new Database();
      db.setName(database);
      Hive.get(userConf).createDatabase(db, ignore);
    } catch (AlreadyExistsException e) {
      throw new GrillException(e);
    } catch (HiveException e) {
      throw new GrillException(e);
    }
    LOG.info("Database created " + database);
  }

  /**
   * @return get all database names
   */
  @Override
  public List<String> getAllDatabases() throws GrillException{
    try {
      return Hive.get(userConf).getAllDatabases();
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Get list of all cubes names in the current database
   * @return
   * @throws GrillException
   */
  @Override
  public List<String> getAllCubeNames() throws GrillException {
    try {
      List<Cube> cubes = getClient().getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (Cube cube : cubes) {
          names.add(cube.getName());
        }
        return names;
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    }
    return null;
  }

  /**
   * Create cube based on the JAXB cube object
   * @param cube
   * @throws GrillException
   */
  @Override
  public void createCube(XCube cube) throws GrillException {
    try {
      getClient().createCube(JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Created cube " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Get a cube from the metastore
   * @param cubeName
   * @return
   * @throws GrillException
   */
  @Override
  public XCube getCube(String cubeName) throws GrillException {
    try {
      Cube c = getClient().getCube(cubeName);
      if (c != null) {
        return JAXBUtils.xCubeFromHiveCube(c);
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    }
    return null;
  }

  /**
   * Drop a cube from the metastore in the currently deleted database
   * @param cubeName
   * @param cascade
   */
  public void dropCube(String cubeName, boolean cascade) throws GrillException {
    try {
      getClient().dropCube(cubeName);
      LOG.info("Dropped cube " + cubeName + " cascade? " + cascade);
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Update cube
   * @param cube JAXB Cube object
   * @throws GrillException
   */
  @Override
  public void updateCube(XCube cube) throws GrillException {
    try {
      getClient().alterCube(cube.getName(), JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Cube updated " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  @Override
  public void init() throws GrillException {
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Create a cube dimension table based on JAXB object
   * @param xDimTable
   * @throws GrillException
   */
  @Override
  public void createCubeDimensionTable(DimensionTable xDimTable) throws GrillException {
    String dimName = xDimTable.getName();
    List<FieldSchema> columns = JAXBUtils.fieldSchemaListFromColumns(xDimTable.getColumns());
    Map<String, List<TableReference>> references =
      JAXBUtils.mapFromDimensionReferences(xDimTable.getDimensionsReferences());
    Map<Storage, UpdatePeriod> updatePeriodMap =
      JAXBUtils.dumpPeriodsFromUpdatePeriods(xDimTable.getUpdatePeriods());
    Map<String, String> properties = JAXBUtils.mapFromXProperties(xDimTable.getProperties());

    try {
      System.out.println("# Columns: "+ columns);
      getClient().createCubeDimensionTable(dimName,
        columns,
        xDimTable.getWeight(),
        references,
        updatePeriodMap,
        properties);
      LOG.info("Dimension Table created " + xDimTable.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  @Override
  public void dropDimensionTable(String dimension, boolean cascade) throws GrillException {
    try {
      getClient().dropDimension(dimension, cascade);
      LOG.info("Dropped dimension table " + dimension + " cascade? " + cascade);
    } catch (HiveException e) {
    	LOG.error("@@@@ Got HiveException: >>>>>>>" + e.getMessage() + "<<<<<<<<<");
      throw new GrillException(e);
    }
  }
  
  @Override
  public DimensionTable getDimensionTable(String dimName) throws GrillException {
  	try {
  		CubeDimensionTable cubeDimTable = getClient().getDimensionTable(dimName);
  		return JAXBUtils.dimTableFromCubeDimTable(cubeDimTable);
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
  
  @Override
  public void updateDimensionTable(DimensionTable dimensionTable) throws GrillException {
  	try {
  		getClient().alterCubeDimensionTable(dimensionTable.getName(), 
  				JAXBUtils.cubeDimTableFromDimTable(dimensionTable));
  		LOG.info("Updated dimension table " + dimensionTable.getName());
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
  
  @Override
  public Collection<String> getDimensionStorages(String dimension) throws GrillException {
  	try {
  		CubeDimensionTable dimTable = getClient().getDimensionTable(dimension);
  		return new ArrayList<String>(dimTable.getStorages());
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
  
  @Override
  public void createDimensionStorage(String dimName, String updatePeriod, XStorage storageAttr) 
  throws GrillException {
  	try {
	  	Storage storage = JAXBUtils.storageFromXStorage(storageAttr);
	  	CubeDimensionTable dimTable = getClient().getDimensionTable(dimName);
	  	UpdatePeriod period = UpdatePeriod.valueOf(updatePeriod.toUpperCase());
	  	getClient().addStorage(dimTable, storage, period);
	  	LOG.info("Added storage " + storageAttr.getName() + " for dimension table " + dimName
	  			+ " with update period " + period);
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
  
  @Override
  public void dropAllStoragesOfDim(String dimName) throws GrillException {
  	try {
	  	CubeDimensionTable tab = getClient().getDimensionTable(dimName);
	  	int total = tab.getStorages().size();
	  	int i = 0;
	  	List<String> storageNames = new ArrayList<String>(tab.getStorages());
	  	for (String s : storageNames) {
	  		getClient().dropStorageFromDim(dimName, s);
	  		LOG.info("Dropped storage " + s + " from dimension table " + dimName 
	  				+ " [" + ++i + "/" + total + "]");
	  	}
	  	LOG.info("Dropped " + total + " storages from dimension table " + dimName);
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
  
  @Override
  public XStorage getStorageOfDimension(String dimname, String storage) throws GrillException {
  	try {
  		CubeDimensionTable tab = getClient().getDimensionTable(dimname);
  		if (!tab.getStorages().contains(storage)) {
  			throw new NotFoundException("Storage " + storage + " not found for dimension " + dimname);
  		}
  		
  		XStorage xs = new XStorage();
  		xs.setName(storage);
  		return xs;
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }

  @Override
  public void dropStorageOfDim(String dimName, String storage) throws GrillException {
  	try {
  		CubeDimensionTable tab = getClient().getDimensionTable(dimName);
  		if (!tab.getStorages().contains(storage)) {
  			throw new NotFoundException("Storage " + storage + " not found for dimension " + dimName);
  		}
  		
  		getClient().dropStorageFromDim(dimName, storage);
  		LOG.info("Dropped storage " + storage + " from dimension table " + dimName) ;
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
  
  @Override
  public List<XPartition> getAllPartitionsOfDimStorage(String dimName, String storage,
  		String filter) throws GrillException {
  	try {
  		String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
  	      Storage.getPrefix(storage));
  		List<Partition> parts = getClient().getPartitionsByFilter(storageTableName, filter);
  		List<XPartition> xParts = new ArrayList<XPartition>();
  		for (Partition part : parts) {
  			XPartition xPart = new XPartition();
  			xPart.setName(part.getName());
  			xPart.setLocation(part.getLocation());
  			xParts.add(xPart);
  		}
  		return xParts;
  	} catch (Exception ex) {
  		throw new GrillException(ex);
  	}
  }

	@Override
	public void addPartitionToDimStorage(String dimName, String storage, XPartition partition)
			throws GrillException {
		try {
			String storageName = MetastoreUtil.getDimStorageTableName(dimName,
		      Storage.getPrefix(storage));
			HDFSStorage storageTable = 
					new HDFSStorage(getClient().getHiveTable(storageName));
			getClient().addPartition(getClient().getDimensionTable(dimName), storageTable, 
					JAXBUtils.getDateFromXML(partition.getDate()));
			LOG.info("Added partition dimension=" + dimName + " storage=" + storage + " date=" + 
					partition.getDate().toString());
		} catch (HiveException exc) {
			throw new GrillException(exc);
		}
	}

	@Override
	public void dropAllPartitionsOfDimStorage(String dimName, String storage, String partFilter)
			throws GrillException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public XPartition getPartitionOfDimStorage(String dimName, String storage, String partSpec)
			throws GrillException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updatePartitionOfDimStorage(String dimName, String storage, String partSpec)
			throws GrillException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropPartitionOfDimStorage(String dimName, String storage, String partSpec)
			throws GrillException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public List<FactTable> getAllFactsOfCube(String cubeName) throws GrillException {
		try {
			List<CubeFactTable> cubeFacts = getClient().getAllFactTables(getClient().getCube(cubeName));
			if (cubeFacts != null && !cubeFacts.isEmpty()) {
				List<FactTable> facts = new ArrayList<FactTable>(cubeFacts.size());
				for (CubeFactTable cft : cubeFacts) {
					facts.add(JAXBUtils.factTableFromCubeFactTable(cft));
				}
				return facts;
			}
		} catch (HiveException e) {
			throw new GrillException(e);
		}
		return null;
	}

	@Override
	public FactTable getFactTable(String fact) throws GrillException {
		try {
			return JAXBUtils.factTableFromCubeFactTable(getClient().getFactTable(fact));
		} catch (HiveException e) {
			throw new GrillException(e);
		}
	}
	
	@Override
	public void createFactTable(FactTable fact) throws GrillException {
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
			getClient().createCubeFactTable(Arrays.asList(fact.getCubeName()),
					fact.getName(), 
					JAXBUtils.fieldSchemaListFromColumns(fact.getColumns()), 
					updatePeriods, 
					fact.getWeight(), 
					JAXBUtils.mapFromXProperties(fact.getProperties()));
			LOG.info("Created fact table " + fact.getName());
		} catch (HiveException e) {
			throw new GrillException(e);
		}
	}

	@Override
	public void updateFactTable(FactTable fact) throws GrillException {
		try {
			getClient().alterCubeFactTable(fact.getName(), JAXBUtils.cubeFactFromFactTable(fact));
			LOG.info("Updated fact table " + fact.getName());
		} catch (HiveException e) {
			throw new GrillException(e);
		}
		
	}

	@Override
	public void dropFactTable(String fact, boolean cascade) throws GrillException {
		try {
			getClient().dropFact(fact, cascade);
			LOG.info("Dropped fact table " + fact + " cascade? " + cascade);
		} catch (HiveException e) {
			throw new GrillException(e);
		}
	}

  @Override
  public List<String> getAllFactNames() throws GrillException {
    try {
      List<CubeFactTable> facts = getClient().getAllFacts();
      List<String> factNames = new ArrayList<String>(facts.size());
      for (CubeFactTable cft : facts) {
        factNames.add(cft.getName());
      }
      return factNames;
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  @Override
  public List<String> getStoragesOfFact(String fact) throws GrillException {
    try {
      if (!getClient().isFactTable(fact)) {
        throw new NotFoundException("Not a fact table " + fact);
      }

      CubeFactTable cft = getClient().getFactTable(fact);
      if (cft != null) {
        return new ArrayList<String>(cft.getStorages());
      } else {
        throw new NotFoundException("Could not get fact table " + fact);
      }
    } catch (HiveException exc) {
      throw new GrillException(exc);
    }
  }

  @Override
  public void addStorageToFact(String fact, FactStorage s) throws GrillException {
    XStorage storage = s.getStorage();
    Set<UpdatePeriod> updatePeriods = new TreeSet<UpdatePeriod>();
    for (StorageUpdatePeriod sup : s.getStorageUpdatePeriod()) {
      updatePeriods.add(UpdatePeriod.valueOf(sup.getUpdatePeriod().toUpperCase()));
    }
    try {
      getClient().addStorage(getClient().getFactTable(fact), JAXBUtils.storageFromXStorage(storage), updatePeriods);
      LOG.info("Added storage " + storage.getName() + ":" + updatePeriods + " for fact " + fact);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    }
  }

  @Override
  public void dropStorageOfFact(String fact, String storage) throws GrillException {
    try {
      getClient().dropStorageFromFact(fact, storage);
      LOG.info("Dropped storage " + storage + " from fact " + fact);
    } catch (HiveException exc) {
      throw new GrillException(exc);
    }
  }

  @Override
  public FactStorage getStorageOfFact(String fact, String storage) throws GrillException {
    try {
      if (getClient().isFactTable(fact)) {
        CubeFactTable cft = getClient().getFactTable(fact);
        if (!cft.getStorages().contains(storage)) {
          throw new NotFoundException("Storage " + storage + " not found in fact " + fact);
        }

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
      } else {
        throw new NotFoundException("Fact table not found: " + fact);
      }
    } catch (HiveException exc) {
      throw new GrillException(exc);
    }
  }
}
