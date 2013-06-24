package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryConstants;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.columnar.LazyNOBColumnarSerde;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.inmobi.dw.yoda.proto.NetworkObject.KeyLessNetworkObject;
import com.inmobi.dw.yoda.tools.util.cube.CubeDefinitionReader;
import com.inmobi.dw.yoda.tools.util.cube.Grain;

public class CubeDDL {
  private static final Log LOG = LogFactory.getLog(
      CubeDDL.class);
  public static final String MEASURE_TYPE = "double";
  public static final String DIM_TYPE = "string";    
  public static final String MEASURE_DEFAULT_AGGREGATE = "sum";
  public static final String YODA_STORAGE = "ua2";

  public static String cubeStorageSchema = "network_object.proto";
  private final String cubeName;
  private Cube cube;
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
    this.cubeName = cubeName;
    this.cubeReader = cubeReader;
    this.allProps = properties;
    this.dimDDL = dimDDL;
    this.client = CubeMetastoreClient.getInstance(conf);
    loadCubeDefinition();
  }

  private void loadCubeDefinition() {
    Set<CubeDimension> dimensions = new HashSet<CubeDimension>();
    Set<CubeMeasure> measures = new HashSet<CubeMeasure>();
    Map<String, String> cubeProperties = new HashMap<String, String>();
    cubeProperties.putAll(getProperties());
    cubeProperties.put(MetastoreUtil.getCubeTimedDimensionListKey(cubeName),
        "dt,pt,et,it");
    // Construct CubeDimension and CubeMeasure objects for each dimension and 
    // measure
    for (String dimName : cubeReader.getAllDimensionNames(cubeName)) {
      CubeDimension dim;
      FieldSchema column = new FieldSchema(dimName, CubeDDL.DIM_TYPE,
          "dim col");
      if (dimDDL.getDimensionReferences(dimName) != null) {
        dim = new ReferencedDimension(column, dimDDL.getDimensionReferences(dimName));
      } else {
        dim = new BaseDimension(column);
      }
      dimensions.add(dim);
    }

    for (String msrName : cubeReader.getAllMeasureNames(cubeName)) {
      FieldSchema column = new FieldSchema(msrName, MEASURE_TYPE, "msr col");
      CubeMeasure msr = new ColumnMeasure(column, null,
          MEASURE_DEFAULT_AGGREGATE, null);
      measures.add(msr);
    }
    this.cube = new Cube(cubeName, measures, dimensions, cubeProperties); 


    // Read summary definitions
    for (String summary : cubeReader.getSummaryNames(cubeName)) {
      Map<String, String> props = getSummaryProperties(summary);
      Set<String> cols = new HashSet<String>();
      cols.addAll(cubeReader.getSummaryDimensionNames(cubeName, summary));
      cols.addAll(cubeReader.getSummaryMeasureNames(cubeName, summary));
      props.put(MetastoreUtil.getValidColumnsKey(summary), StringUtils.join(cols, ','));
      summaryProperties.put(summary, getSummaryProperties(summary));
      
    }
  }

  private void createCube() throws HiveException {
    System.out.println("creating cube " + cubeName + " with measures:" +  
      cube.getMeasures() + " with dimensions:" + cube.getDimensions());
    client.createCube(cube);
  }

  public void createSummaries() throws HiveException {
    for (String summary : cubeReader.getSummaryNames(cubeName)) {
      createSummary(summary);
    }
  }
  
  public static List<FieldSchema> getNobColList() {
    if (nobColList == null) {
      List<FieldDescriptor> fdList = KeyLessNetworkObject.getDescriptor().getFields();
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

  public void createSummary(String summary) throws HiveException {
    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods = createStorages(summary);
    List<FieldSchema> columns = getNobColList();
    
    System.out.println("storageAggregatePeriods:" + storageAggregatePeriods);
    System.out.println("columns:" + columns);
    System.out.println("Summary :" + summary + " cost:" + cubeReader.getAvgSummaryCost(cubeName, summary));
    client.createCubeFactTable(cubeName, summary, columns,
        storageAggregatePeriods,
        cubeReader.getAvgSummaryCost(cubeName, summary),
        summaryProperties.get(summary));
  }

  public Map<Storage, List<UpdatePeriod>> createStorages(String summary) {
    //Path summaryPath = new Path(cubeReader.getCubePath(cubeName), summary);
    //Path storagePath = new Path(summaryPath, updatePeriod.getName());
    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods = 
        new HashMap<Storage, List<UpdatePeriod>>();
    Storage storage = new HDFSStorage(YODA_STORAGE,
        RCFileInputFormat.class.getCanonicalName(),
        RCFileOutputFormat.class.getCanonicalName(),
        LazyNOBColumnarSerde.class.getCanonicalName(),
        true, null, null, null);
    storage.addToPartCols(new FieldSchema("dt", "string", "date partition"));
    Set<Grain> grains = cubeReader.getSummaryGrains(cubeName, summary);
    List<UpdatePeriod> updatePeriods = new ArrayList<UpdatePeriod>();
    for (Grain g : grains) {
      if (!g.getFileSystemPrefix().equals("none")) {
        if (g.getFileSystemPrefix().equals(Grain.quarter.getFileSystemPrefix())) {
          updatePeriods.add(UpdatePeriod.QUARTERLY);
        } else {
          updatePeriods.add(UpdatePeriod.valueOf(g.getFileSystemPrefix().toUpperCase()));          
        }
      }
    }
    storageAggregatePeriods.put(storage, updatePeriods);
    return storageAggregatePeriods;
  }

  private Map<String, String> getProperties() {
    return getProperties("cube." + cubeName, allProps);
  }

  private Map<String, String> getSummaryProperties(String fact) {
    return getProperties("cube." + cubeName + ".summary." + fact, allProps);
  }

  static Map<String, String> getProperties(String prefix, Properties allProps) {
    Map<String, String> props = new HashMap<String, String>();
    for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
      String key = entry.toString();
      if (key.startsWith(prefix)) {
        props.put(key, entry.getValue().toString());
      }
    }
    return props;
  }

  public static void main(String[] args) throws IOException, HiveException {
    CubeReader cubeReader = new CubeReader();
    CubeDDL cc;
    HiveConf conf = new HiveConf(CubeDDL.class);

    DimensionDDL dimDDL = new DimensionDDL(conf);
    if (args.length == 0) {
      LOG.info("Creating all cubes " + cubeReader.getReader().getCubeNames());
      for (String cubeName : cubeReader.getReader().getCubeNames()) {
        cc = new CubeDDL(cubeName, cubeReader.getReader(),
            cubeReader.getProps(), dimDDL, conf);
        cc.createCube();
        cc.createSummaries();
      }
    } else {
      if (!cubeReader.getReader().getCubeNames().contains(args[0])) {
        LOG.error("Cube " + args[0] + " is not defined in cubeDefinition");
        return;
      } else {
        LOG.info("Creating cube " + args[0]);
        cc = new CubeDDL(args[0], cubeReader.getReader(),
            cubeReader.getProps(), dimDDL, conf);
        cc.createCube();        
        cc.createSummaries();
      }
    }
  }
}
