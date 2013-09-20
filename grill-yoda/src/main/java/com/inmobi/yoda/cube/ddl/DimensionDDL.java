package com.inmobi.yoda.cube.ddl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimension;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.TextInputFormat;


public class DimensionDDL {
  private static final Log LOG = LogFactory.getLog(
      DimensionDDL.class);
  public static final String metadata_field_info_file = "field_info.csv";
  public static final String join_chain_info_file = "join_chain_info.csv";
  public static final UpdatePeriod dimension_dump_period = UpdatePeriod.HOURLY;

  private static class FieldInfo {
    int id; // seq id
    String name;
    String tableName;
    String desc;
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
  private Set<String> noColTables = new HashSet<String>(); 
  private Map<String, Map<Integer, FieldInfo>> tableFields = new HashMap<String,
      Map<Integer, FieldInfo>>();
  private Map<String, List<TableReference>> dimReferences = 
      new HashMap<String, List<TableReference>>();

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
      FieldInfo fi = new FieldInfo();
      fi.id = Integer.parseInt(fields[0]);
      fi.name = fields[1];
      fi.tableName = fields[2];
      fieldMap.put(fi.id, fi);
      if (fields.length > 3 && fields[3] != null) {
        fi.desc = fields[3];
      } else {
        fi.desc = "";
      }
      if (fields.length > 4 && fields[4] != null) {
        int colPos = Integer.parseInt(fields[4]);
        Map<Integer, FieldInfo> colMap = tableFields.get(fi.tableName);
        if (colMap == null) {
          colMap = new HashMap<Integer, FieldInfo>();
          tableFields.put(fi.tableName, colMap);
        }
        colMap.put(colPos, fi);
      } else {
        noColTables.add(fi.tableName);
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
    for (FieldInfo fi : fieldMap.values()) {
      List<TableReference> references = null;
      if (!fi.references.isEmpty()) {
        references = new ArrayList<TableReference>();
        for (int ref : fi.references) {
          FieldInfo refField = fieldMap.get(ref);
          references.add(new TableReference(refField.tableName, refField.name));
        }
      }
      dimReferences.put(fi.name, references);
    }
  }

  public List<TableReference> getDimensionReferences(String name) {
    return dimReferences.get(name);
  }

  public void createAllDimensions() throws HiveException {
    for (String tableName : tableFields.keySet()) {
      createDimension(tableName);
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
      columns.add(new FieldSchema(fi.name, CubeDDL.DIM_TYPE, fi.desc));
      if (dimReferences.get(fi.name) != null) {
        dimensionReferences.put(fi.name, dimReferences.get(fi.name));
        LOG.info("Dimension " + fi.name + " with references:" 
            + dimReferences.get(fi.name));
      }
    }
    Map<Storage, UpdatePeriod> snapshotDumpPeriods = 
        new HashMap<Storage, UpdatePeriod>();
    Set<Storage> storages = createStorages(dimName);
    for (Storage storage: storages) {
      snapshotDumpPeriods.put(storage, dimension_dump_period);
    }

    Map<String, String> properties = new HashMap<String, String>();
    client.createCubeDimensionTable(dimName, columns, Double.valueOf(0.0),
        dimensionReferences,
        snapshotDumpPeriods, properties);
  }

  public Set<Storage> createStorages(String dimName) {
    Set<Storage> storages =  new HashSet<Storage>();
    Storage storage = new HDFSStorage(CubeDDL.YODA_STORAGE,
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName(),
        null, true, null, null, null);
    storages.add(storage);
    return storages;
  }

  public static void main(String[] args) throws IOException, HiveException {
    HiveConf conf = new HiveConf(DimensionDDL.class);
    SessionState.start(conf);
    DimensionDDL cd = new DimensionDDL(conf);
    cd.createAllDimensions();
  }
}
