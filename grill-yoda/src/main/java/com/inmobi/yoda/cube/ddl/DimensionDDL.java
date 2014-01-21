package com.inmobi.yoda.cube.ddl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimension;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreConstants;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.StorageTableDesc;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;


public class DimensionDDL {
  private static final Log LOG = LogFactory.getLog(
      DimensionDDL.class);
  public static final String metadata_field_info_file = "field_info.csv";
  public static final String join_chain_info_file = "join_chain_info.csv";
  public static final UpdatePeriod dimension_dump_period = UpdatePeriod.HOURLY;
  public static final String dim_time_part_column = "dt";

  public static class FieldInfo {
    int id; // seq id
    String name;
    String tableName;
    String desc;
    String type;
    List<Integer> references = new ArrayList<Integer>();

    public String toString() {
      return id + ":" + tableName + ":" + name + ":" + references;
    }
  }

  HiveConf conf;
  Map<String, CubeDimension> dimensions = new HashMap<String, CubeDimension>();
  CubeMetastoreClient client;
  DimensionDDL(HiveConf conf)
      throws IOException, HiveException {
    this.conf = conf;
    client = CubeMetastoreClient.getInstance(conf);
    loadDimensionDefinitions();
  }

  private Map<Integer, FieldInfo> fieldMap = new HashMap<Integer, FieldInfo>();
  private Map<String, List<FieldInfo>> noColTables = new HashMap<String, List<FieldInfo>>(); 
  private Map<String, Map<Integer, FieldInfo>> tableFields = new HashMap<String,
      Map<Integer, FieldInfo>>();
  private Map<String, Map<String, FieldInfo>> tableFieldSchema = new HashMap<String, Map<String, FieldInfo>>();
  private Map<String, Map<String, List<TableReference>>> cubeDimReferences = 
      new HashMap<String,  Map<String, List<TableReference>>>();

  private void loadDimensionDefinitions() throws IOException {
    BufferedReader fieldInforeader = new BufferedReader(new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            metadata_field_info_file)));
    BufferedReader joinInforeader = new BufferedReader(new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            join_chain_info_file)));
    // skip first line as that would be header
    fieldInforeader.readLine();
    joinInforeader.readLine();

    // Start reading the fields from fieldinfo
    String line = fieldInforeader.readLine();
    while (line != null)  {
      String[] fields = line.split(",");
      //Field 0 : Seq. No
      //Field 1: field name
      //Field 2: Table name
      //Field 3: Desc
      //Field 4: sequence number of the field within the table.
      //Fild 5: Field data type
      FieldInfo fi = new FieldInfo();
      fi.id = Integer.parseInt(fields[0]);
      fi.name = fields[1].trim();
      fi.tableName = fields[2];
      fieldMap.put(fi.id, fi);

      Map<String, FieldInfo> fieldSchemaMap = tableFieldSchema.get(fi.tableName);
      if (fieldSchemaMap == null) {
        fieldSchemaMap = new HashMap<String, FieldInfo>();
        tableFieldSchema.put(fi.tableName, fieldSchemaMap);
      }
      fieldSchemaMap.put(fi.name, fi);

      if (fields.length > 3 && fields[3] != null) {
        fi.desc = fields[3];
      } else {
        fi.desc = "";
      }
      if (fields.length > 4 && fields[4] != null && !fields[4].isEmpty()) {
        int colPos = Integer.parseInt(fields[4]);
        Map<Integer, FieldInfo> colMap = tableFields.get(fi.tableName);
        if (colMap == null) {
          colMap = new HashMap<Integer, FieldInfo>();
          tableFields.put(fi.tableName, colMap);
        }
        colMap.put(colPos, fi);
      } else {
        List<FieldInfo> directCubeDims = noColTables.get(fi.tableName);
        if (directCubeDims == null) {
          directCubeDims = new ArrayList<FieldInfo>();
          noColTables.put(fi.tableName, directCubeDims);
        }
        directCubeDims.add(fi);
      }

      if (fields.length > 5 && fields[5] != null && !fields[5].isEmpty()) {
        fi.type = fields[5].toLowerCase();
      }

      line = fieldInforeader.readLine();
    }
    LOG.warn("No columns available for tables :" + noColTables);

    // start reading the fields from joininfo
    line = joinInforeader.readLine();
    while (line != null) {
      String[] fields = line.split(",");
      //Field 0: seq. no
      //Field 1: field id
      //Field 2: referenced field id
      //Field 3: parent id
      int lhsid = Integer.parseInt(fields[1]);
      int rhsid = Integer.parseInt(fields[2]);
      FieldInfo fi = fieldMap.get(lhsid);
      if (fi != null) {
        fieldMap.get(lhsid).references.add(rhsid);
      } else {
        LOG.warn("Field not found for id:" + lhsid + " in join chain");
      }
      line = joinInforeader.readLine();
    }
    for (Map.Entry<String, List<FieldInfo>> entry : noColTables.entrySet()) {
      Map<String, List<TableReference>> dimReferences = 
          new HashMap<String, List<TableReference>>();     
      for (FieldInfo fi : entry.getValue()) {
        dimReferences.put(fi.name, getDimReferences(fi));
      }
      cubeDimReferences.put(entry.getKey(), dimReferences);
    }
  }

  private List<TableReference> getDimReferences(FieldInfo fi) {
    List<TableReference> references = null;
    if (!fi.references.isEmpty()) {
      references = new ArrayList<TableReference>();
      for (int ref : fi.references) {
        FieldInfo refField = fieldMap.get(ref);
        references.add(new TableReference(refField.tableName, refField.name));
      }
    }
    return references;
  }

  public List<TableReference> getDimensionReferences(String cubeName,
      String dimName) {
    return cubeDimReferences.get(cubeName).get(dimName);
  }

  public void createAllDimensions() throws HiveException {
    for (String tableName : tableFields.keySet()) {
      System.out.println("## DIM TABLE:" + tableName);
      try {
        createDimension(tableName);
      } catch (HiveException exc) {
        System.err.println("@@Error creating table: " + tableName + "  " +  exc.getMessage());
        exc.printStackTrace();
        throw exc;
      }
    }
  }

  public void createDimension(String dimName)
      throws HiveException {
    Map<Integer, FieldInfo> colMap = tableFields.get(dimName);

    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    Map<String, List<TableReference>> dimensionReferences = 
        new HashMap<String, List<TableReference>>();

    for (int i = 1; i <= colMap.size(); i++) {
      FieldInfo fi = colMap.get(i);
      FieldSchema fieldSchema;
      if ("list".equalsIgnoreCase(fi.type)) {
        // Handle List
        fieldSchema = new FieldSchema(fi.name, "array<string>", fi.desc);
      } else if ("map".equalsIgnoreCase(fi.type)) {
        // Handle map type
        fieldSchema = new FieldSchema(fi.name, "map<string, string>", fi.desc);
      } else if (fi.type != null && !fi.type.isEmpty()) {
        fieldSchema = new FieldSchema(fi.name, fi.type, fi.desc);
      } else {
        // Default data type is string
        fieldSchema = new FieldSchema(fi.name, CubeDDL.DIM_TYPE, fi.desc);
      }
      columns.add(fieldSchema);
      List<TableReference> references = getDimReferences(fi);
      if (references != null) {
        dimensionReferences.put(fi.name, references);
      }
    }
    Map<String, UpdatePeriod> snapshotDumpPeriods = 
        new HashMap<String, UpdatePeriod>();
    Map<Storage, StorageTableDesc> storageTables = createStorages(dimName);
    for (Storage storage: storageTables.keySet()) {
      snapshotDumpPeriods.put(storage.getName(), dimension_dump_period);
    }

    Map<String, String> properties = new HashMap<String, String>();
    properties.put(MetastoreConstants.TIMED_DIMENSION, dim_time_part_column);
    System.out.println("## Creating table " + dimName);
    if (Hive.get().getTable(dimName, false) == null) {
      client.createCubeDimensionTable(dimName, columns, Double.valueOf(0.0),
          dimensionReferences,
          snapshotDumpPeriods, properties, storageTables);
      System.out.println("Created dimension:" + dimName);
    } else {
      CubeDimensionTable original = client.getDimensionTable(dimName);
      CubeDimensionTable dimTable = new CubeDimensionTable(dimName, columns,
          Double.valueOf(0.0), snapshotDumpPeriods, dimensionReferences, properties);
      if (original == null) {
        System.out.println(dimName + " is not a dimension table");
      } else if (!original.equals(dimTable)) {
        System.out.println("Altering dimension table original:" + original + " new:" + dimTable);
        client.alterCubeDimensionTable(dimName, dimTable);
      } else {
        System.out.println("Nothing to alter for" + dimTable);
      }
    }
  }

  public Map<Storage, StorageTableDesc> createStorages(String dimName) {
    Map<Storage, StorageTableDesc> storages =  new HashMap<Storage, StorageTableDesc>();
    
    Storage storage = new HDFSStorage(CubeDDL.YODA_STORAGE);
    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> timePartCols = new ArrayList<String>();
    partCols.add(new FieldSchema(dim_time_part_column, "string", "dim part column"));
    timePartCols.add(dim_time_part_column);
    StorageTableDesc sTbl = new StorageTableDesc();
    sTbl.setExternal(true);
    sTbl.setInputFormat(TextInputFormat.class.getCanonicalName());
    sTbl.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    sTbl.setPartCols(partCols);
    sTbl.setTimePartCols(timePartCols);
    storages.put(storage, sTbl);
    return storages;
  }

  public FieldInfo getFieldInfo(String table, String field) {
    Map<String, FieldInfo> tableSchema = tableFieldSchema.get(table);
    return tableSchema == null ? null : tableSchema.get(field);
  }

  public static void main(String[] args) throws Exception {
    HiveConf conf = new HiveConf(DimensionDDL.class);
    SessionState.start(conf);
    DimensionDDL cd = new DimensionDDL(conf);
    if (args.length > 0) {
      if (args[0].equals("-db")) {
        String dbName = args[1];
        Database database = new Database();
        database.setName(dbName);
        Hive.get().createDatabase(database, true);
        SessionState.get().setCurrentDatabase(dbName);
      }
    }
    LOG.info("Creating all dimensions");
    cd.createAllDimensions();
    System.out.println("Created all dimensions");
  }
}
