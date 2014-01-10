package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.BaseDimension;
import org.apache.hadoop.hive.ql.cube.metadata.ColumnMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimension;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreConstants;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimension;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.StorageTableDesc;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.columnar.LazyNOBColumnarSerde;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.inmobi.dw.yoda.proto.NetworkObject.KeyLessNetworkObject;
import com.inmobi.dw.yoda.tools.util.cube.CubeDefinitionReader;
import com.inmobi.dw.yoda.tools.util.cube.Grain;
import com.inmobi.grill.api.GrillConfConstants;

public class CubeDDL {
  private static final Log LOG = LogFactory.getLog(
      CubeDDL.class);
  public static final String MEASURE_TYPE = "double";
  public static final String DIM_TYPE = "string";    
  public static final String MEASURE_DEFAULT_AGGREGATE = "sum";
  public static final String YODA_STORAGE = "ua2";
  public static final String YODA_PIE_STORAGE = "ua2_pie";
  public static final String CUBE_NAME_PFX = "cube_";
  public static final String RAW_FACT_NAME = "raw";
  public static final String PART_KEY_IT = "it";
  public static final String PART_KEY_ET = "et";
  public static final String PART_KEY_PT = "pt";
  public static final String PART_KEY_COLO = "colo";

  public static String cubeStorageSchema = "network_object.proto";
  public static String cubeNameInJoinChain = "network_object";
  private Map<String, Cube> cubes = new HashMap<String, Cube>();
  // fact name to list of cubes it belongs
  private Map<String, List<String>> factToCubes = new HashMap<String, List<String>>();
  // raw fact to list of cubes it belongs
  private Map<String, List<String>> rawFactPathToCubes = new HashMap<String, List<String>>();
  private final CubeDefinitionReader cubeReader;
  private final Properties allProps;
  private final DimensionDDL dimDDL;
  private final CubeMetastoreClient client;
  private static List<FieldSchema> nobColList;

  private final Map<String, Map<String, String>> summaryProperties =
      new HashMap<String, Map<String, String>>();

  static final Set<String> allMeasures = new HashSet<String>();
  static {
    CubeDefinitionReader reader = CubeDefinitionReader.get();
    for (String cubeName : reader.getCubeNames()) {
      allMeasures.addAll(reader.getAllMeasureNames(cubeName));
      for (String summary : reader.getSummaryNames(cubeName)) {
        allMeasures.addAll(reader.getSummaryMeasureNames(cubeName, summary));
      }
    }
  }

  public CubeDDL(String cubeName, CubeDefinitionReader cubeReader,
      Properties properties, DimensionDDL dimDDL, HiveConf conf)
          throws HiveException {
    this.cubeReader = cubeReader;
    this.allProps = properties;
    this.dimDDL = dimDDL;
    this.client = CubeMetastoreClient.getInstance(conf);
    loadCubeDefinition();
  }

  public CubeDDL(DimensionDDL dimDDL, HiveConf conf)
      throws HiveException, IOException {
    CubeReader reader = new CubeReader();
    cubeReader = reader.getReader();
    this.allProps = reader.getProps();
    this.dimDDL = dimDDL;
    this.client = CubeMetastoreClient.getInstance(conf);
    loadCubeDefinition();
    createStorages(conf);
  }

  static DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH").withZoneUTC();
  private static Date defaultStartTime;

  static{
    defaultStartTime = dateFormatter.parseDateTime("2011-04-01-00").toDate();
  }

  private void loadCubeDefinition() {
    for (String cubeName : cubeReader.getCubeNames()) {
      String cubeTableName = CUBE_NAME_PFX + cubeName;

      Set<CubeDimension> dimensions = new HashSet<CubeDimension>();
      Set<CubeMeasure> measures = new HashSet<CubeMeasure>();
      Map<String, String> cubeProperties = new HashMap<String, String>();
      cubeProperties.putAll(getProperties(cubeName));
      cubeProperties.put(MetastoreUtil.getCubeTimedDimensionListKey(cubeTableName),
          "pt,et,it");
      // Construct CubeDimension and CubeMeasure objects for each dimension and 
      // measure
      for (Map.Entry<String, DateTime> dimEntry :
        cubeReader.getAllDimensionsWithActualStartTime(cubeName).entrySet()) {
        String dimName = dimEntry.getKey();
        CubeDimension dim;
        FieldSchema column = new FieldSchema(dimName, CubeDDL.DIM_TYPE,
            "dim col");
        Date startTime = defaultStartTime;
        if (dimEntry.getValue() != null) {
          startTime = dimEntry.getValue().toDate();
        }
        if (dimDDL.getDimensionReferences(cubeNameInJoinChain, dimName) != null)
        {
          dim = new ReferencedDimension(column, dimDDL.getDimensionReferences(
              cubeNameInJoinChain, dimName), startTime, null, 0.0);
        } else {
          dim = new BaseDimension(column, startTime, null, 0.0);
        }
        dimensions.add(dim);
      }

      for (Map.Entry<String, DateTime> msrEntry:
        cubeReader.getAllMeasuresWithActualStartTime(cubeName).entrySet()) {
        String msrName = msrEntry.getKey();
        Date startTime = defaultStartTime;
        if (msrEntry.getValue() != null) {
          startTime = msrEntry.getValue().toDate();
        }
        FieldSchema column = new FieldSchema(msrName, MEASURE_TYPE, "msr col");
        CubeMeasure msr = new ColumnMeasure(column, null,
            MEASURE_DEFAULT_AGGREGATE, null, startTime, null, 0.0);
        measures.add(msr);
      }
      this.cubes.put(cubeName, new Cube(cubeTableName, measures, dimensions,
          cubeProperties, cubeReader.getCost(cubeName))); 


      // raw fact path
      String rawPath = cubeReader.getCubePath(cubeName);
      List<String> cubeNames = rawFactPathToCubes.get(rawPath);
      if (cubeNames == null) {
        cubeNames = new ArrayList<String>();
        rawFactPathToCubes.put(rawPath, cubeNames);
      }
      cubeNames.add(cubeTableName);

      // Read summary definitions
      for (String summary : cubeReader.getSummaryNames(cubeName)) {
        List<String> factcCubeNames = factToCubes.get(summary);
        if (factcCubeNames == null) {
          factcCubeNames = new ArrayList<String>();
          factToCubes.put(summary, factcCubeNames);
          // read the summary definition for the first time
          Map<String, String> props = getSummaryProperties(cubeName, summary);
          Set<String> cols = new HashSet<String>();
          cols.addAll(cubeReader.getSummaryDimensionNames(cubeName, summary));
          cols.addAll(cubeReader.getSummaryMeasureNames(cubeName, summary));
          props.put(MetastoreUtil.getValidColumnsKey(
              summary.toLowerCase()),
              StringUtils.join(cols, ','));
          summaryProperties.put(summary, props);
        }
        factcCubeNames.add(cubeTableName);
      }
    }
  }

  public static void createStorages(HiveConf conf) throws HiveException {
    CubeMetastoreClient cc = CubeMetastoreClient.getInstance(conf);
    if (!cc.tableExists(YODA_STORAGE)) {
      cc.createStorage(new HDFSStorage(YODA_STORAGE));
    }
    if (!cc.tableExists(YODA_PIE_STORAGE)) {
      cc.createStorage(new HDFSStorage(YODA_PIE_STORAGE));
    }
  }

  public void createAllCubes() throws HiveException {
    for (String cubeName : cubes.keySet()) {
      if (Hive.get().getTable(cubes.get(cubeName).getName(), false) == null) {
        createCube(cubeName);        
      } else {
        Cube original = client.getCube(cubes.get(cubeName).getName());
        if (original == null) {
          System.out.println(cubes.get(cubeName).getName() + " is not a cube table");
        } else if (!original.equals(cubes.get(cubeName))) {
          System.out.println("Altering cube  original:" + original + "new:" + cubes.get(cubeName));
          client.alterCube(cubes.get(cubeName).getName(), cubes.get(cubeName));
        }
      }
    }

    // create raw facts
    for (Map.Entry<String, List<String>> entry : rawFactPathToCubes.entrySet()) {
      createOrAlterRawFact(entry);
    }

    // create summaries
    for (String summary : factToCubes.keySet()) {
      createOrAlterSummaryFact(summary);
    }
    System.out.println("All cubes and fact tables created/altered!");
  }

  private void createOrAlterRawFact(Map.Entry<String, List<String>> entry) throws HiveException {
    Set<Grain> rawGrain = new HashSet<Grain>();
    List<String> cubeNames = entry.getValue();
    String cubeName = cubeNames.get(0).substring(CUBE_NAME_PFX.length());
    rawGrain.add(cubeReader.getCubeGrain(cubeName));
    Map<String, String> rawProps = new HashMap<String, String>();
    rawProps.put(MetastoreConstants.FACT_AGGREGATED_PROPERTY, "false");
    String factName = cubeNames.get(0) + "_" + RAW_FACT_NAME;
    createOrAlterFactTable(cubeNames, factName,
        cubeReader.getCost(cubeName), rawProps, rawGrain);

  }

  private void createOrAlterSummaryFact(String summary) throws HiveException {
    List<String> cubeNames = factToCubes.get(summary);
    String cubeName = cubeNames.get(0).substring(CUBE_NAME_PFX.length());
    summaryProperties.get(summary).put(MetastoreConstants.FACT_AGGREGATED_PROPERTY, "true");
    createOrAlterFactTable(cubeNames, summary,
        cubeReader.getAvgSummaryCost(cubeName, summary),
        summaryProperties.get(summary),
        cubeReader.getSummaryGrains(cubeName, summary));    
  }

  private void createCube(String cubeName) throws HiveException {
    if (cubes.get(cubeName) == null) {
      throw new IllegalArgumentException("cube definition not available for "
          + cubeName);
    }
    LOG.info("creating cube " + cubeName + " with measures:" +  
        cubes.get(cubeName).getMeasures() + " with dimensions:" +
        cubes.get(cubeName).getDimensions());
    client.createCube(cubes.get(cubeName));
    System.out.println("Created cube " + cubeName);
  }

  public static List<FieldSchema> getNobColList() {
    if (nobColList == null) {
      List<FieldDescriptor> fdList = KeyLessNetworkObject.getDescriptor()
          .getFields();
      nobColList = new ArrayList<FieldSchema>();
      for (FieldDescriptor fd : fdList) {
        String name = fd.getName();
        String type;
        if (isMeasure(name)) {
          type = MEASURE_TYPE;
        } else {
          type = DIM_TYPE;
        }
        nobColList.add(new FieldSchema(name, type, "nob col"));
      }
    }
    return nobColList;
  }

  private static boolean isMeasure(String name) {
    return allMeasures.contains(name);
  }

  private void createOrAlterFactTable(List<String> cubeNames, String summary, double cost,
      Map<String, String> props, Set<Grain> grains) throws HiveException {
    List<FieldSchema> columns = getNobColList();
    Map<String, StorageTableDesc> storageTables = new HashMap<String, StorageTableDesc>();
    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = createStorages(
        summary, grains, cost,
        cubeReader.getCubeColoPath(cubeNames.get(0).substring(CUBE_NAME_PFX.length())) != null,
        storageTables);

    if (Hive.get().getTable(summary, false) == null) {
      LOG.info("Creating fact table " + summary +
          " with storageAggregatePeriods:" + storageAggregatePeriods +
          "columns:" + columns + " cost:" + cost);

      client.createCubeFactTable(cubeNames, summary, columns,
          storageAggregatePeriods, cost, props, storageTables);
      System.out.println("Created fact:" + summary);
    } else {
      CubeFactTable original = client.getFactTable(summary);
      CubeFactTable factTable = new CubeFactTable(cubeNames, summary, columns,
          storageAggregatePeriods, cost, props);
      if (original == null) {
        System.out.println(summary + " is not a fact table");
      } else if (!original.equals(factTable)) {
        System.out.println("Altering fact  original:" + original + " new:" + summary);
        client.alterCubeFactTable(summary, factTable);
      } else {
        System.out.println("Nothing to alter for" + summary);
      }
    }
  }

  public Map<String, Set<UpdatePeriod>> createStorages(
      String summary, Set<Grain> grains, double cost, boolean hasPIEStorage,
      Map<String, StorageTableDesc> storageTables) {
    //Path summaryPath = new Path(cubeReader.getCubePath(cubeName), summary);
    //Path storagePath = new Path(summaryPath, updatePeriod.getName());
    Map<String, Set<UpdatePeriod>> storageAggregatePeriods = 
        new HashMap<String, Set<UpdatePeriod>>();
    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put(GrillConfConstants.STORAGE_COST, Double.toString(cost));
    StorageTableDesc sTbl = new StorageTableDesc();
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(new FieldSchema(PART_KEY_IT, "string", "date partition"));
    timePartCols.add(PART_KEY_IT);
    sTbl.setExternal(true);
    sTbl.setInputFormat(RCFileInputFormat.class.getCanonicalName());
    sTbl.setOutputFormat(RCFileOutputFormat.class.getCanonicalName());
    sTbl.setSerName(LazyNOBColumnarSerde.class.getCanonicalName());
    sTbl.setTblProps(tableParams);
    sTbl.setPartCols(partCols);
    sTbl.setTimePartCols(timePartCols);
    Set<UpdatePeriod> updatePeriods = new HashSet<UpdatePeriod>();
    for (Grain g : grains) {
      if (g.getFileSystemPrefix().equals("none")) {
        updatePeriods.add(UpdatePeriod.HOURLY);
      } else if (g.getFileSystemPrefix().equals(
          Grain.quarter.getFileSystemPrefix())) {
        updatePeriods.add(UpdatePeriod.QUARTERLY);
      } else {
        updatePeriods.add(UpdatePeriod.valueOf(g.getFileSystemPrefix()
            .toUpperCase()));          
      }
    }
    storageAggregatePeriods.put(YODA_STORAGE, updatePeriods);
    storageTables.put(YODA_STORAGE, sTbl);

    // create storage with PIE partitions
    if (hasPIEStorage) {
      Map<String, String> pieTableParams = new HashMap<String, String>();
      pieTableParams.put(GrillConfConstants.STORAGE_COST, Double.toString(cost));
      StorageTableDesc pieTbl = new StorageTableDesc();
      ArrayList<FieldSchema> piePartCols = new ArrayList<FieldSchema>();
      List<String> pieTimePartCols = new ArrayList<String>();
      if (!summary.endsWith(RAW_FACT_NAME)) {
        piePartCols.add(new FieldSchema(PART_KEY_PT, "string", "date partition"));
        piePartCols.add(new FieldSchema(PART_KEY_IT, "string", "date partition"));
        piePartCols.add(new FieldSchema(PART_KEY_ET, "string", "date partition"));
        pieTimePartCols.add(PART_KEY_PT);
        pieTimePartCols.add(PART_KEY_IT);
        pieTimePartCols.add(PART_KEY_ET);
      } else {
        piePartCols.add(new FieldSchema(PART_KEY_IT, "string", "date partition"));
        piePartCols.add(new FieldSchema(PART_KEY_COLO, "string", "colo name"));
        pieTimePartCols.add(PART_KEY_IT);
      }
      pieTbl.setExternal(true);
      pieTbl.setInputFormat(RCFileInputFormat.class.getCanonicalName());
      pieTbl.setOutputFormat(RCFileOutputFormat.class.getCanonicalName());
      pieTbl.setSerName(LazyNOBColumnarSerde.class.getCanonicalName());
      pieTbl.setTblProps(pieTableParams);
      pieTbl.setPartCols(piePartCols);
      pieTbl.setTimePartCols(pieTimePartCols);
      storageAggregatePeriods.put(YODA_PIE_STORAGE, updatePeriods);
      storageTables.put(YODA_PIE_STORAGE, pieTbl);
    }
    return storageAggregatePeriods;
  }

  private Map<String, String> getProperties(String cubeName) {
    return getProperties("cube." + cubeName, allProps);
  }

  private Map<String, String> getSummaryProperties(String cubeName,
      String summary) {
    return getProperties("cube." + cubeName + ".summary." + summary, allProps);
  }

  static Map<String, String> getProperties(String prefix, Properties allProps) {
    Map<String, String> props = new HashMap<String, String>();
    for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
      String key = entry.getKey().toString();
      if (key.startsWith(prefix)) {
        props.put(key, entry.getValue().toString());
      }
    }
    return props;
  }

  public static void main(String[] args) throws Exception {
    HiveConf conf = new HiveConf(CubeDDL.class);
    SessionState.start(conf);

    DimensionDDL dimDDL = new DimensionDDL(conf);
    CubeDDL cc = new CubeDDL(dimDDL, conf);
    if (args.length > 0) {
      if (args[0].equals("-db")) {
        String dbName = args[1];
        Database database = new Database();
        database.setName(dbName);
        Hive.get().createDatabase(database, true);
        SessionState.get().setCurrentDatabase(dbName);
      }
    }
    LOG.info("Creating all cubes ");
    cc.createAllCubes();
  }
}
