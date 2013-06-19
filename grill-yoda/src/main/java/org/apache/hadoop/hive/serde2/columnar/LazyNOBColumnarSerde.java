package org.apache.hadoop.hive.serde2.columnar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.serde.serdeConstants.*;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

public class LazyNOBColumnarSerde implements SerDe {
  private static final Log LOG = LogFactory.getLog(LazyNOBColumnarSerde.class);
  LazyNOBColumnarStruct cachedNOBLazyStruct;
  protected ObjectInspector cachedObjectInspector;
  int[] referedCols;

  static Map<Integer, List<String>> fieldGroups;

  static {
    fieldGroups = new TreeMap<Integer, List<String>>();
    Properties fieldProps = new Properties();
    try {
      fieldProps.load(LazyNOBColumnarSerde.class.getClassLoader()
          .getResourceAsStream("field_group.properties"));

      for (Map.Entry<Object, Object> entry : fieldProps.entrySet()) {
        if (!entry.getKey().toString().equals("group.total")) {
          int group = Integer.parseInt(entry.getValue().toString());
          if (!fieldGroups.containsKey(group)) {
            fieldGroups.put(group, new ArrayList<String>());
          }
          fieldGroups.get(group).add(entry.getKey().toString());
        }
      }
    } catch (IOException e) {
      LOG.error("Could not load field group properties");
      throw new RuntimeException("Could not load field group properties", e);
    }
  }

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    String columnNameProperty = tbl.getProperty(LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(LIST_COLUMN_TYPES);
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(
        columnTypeProperty);
    int numColumns = columnNames.size();
    String[] readColStr = conf.getStrings("hive.io.file.readcolumn.ids");
    if (readColStr != null) {
      referedCols = new int[readColStr.length];
      for (int i=0; i< readColStr.length; i++) {
        referedCols[i] = Integer.valueOf(readColStr[i]);
      }
    } else {
      referedCols = new int[fieldGroups.size()];
      for (int i=0; i< fieldGroups.size(); i++) {
        referedCols[i] = i;
      }
    }
    // Create the ObjectInspectors for the fields. 
    List<ObjectInspector> columnObjectInspectors = 
        new ArrayList<ObjectInspector>(numColumns);
    ObjectInspector colObjectInspector;
    for (int col = 0; col < numColumns; col++) {
      colObjectInspector = TypeInfoUtils
          .getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(col));
      columnObjectInspectors.add(colObjectInspector);
    }

    cachedObjectInspector = ObjectInspectorFactory
        .getColumnarStructObjectInspector(columnNames, columnObjectInspectors);
    cachedNOBLazyStruct = new LazyNOBColumnarStruct(cachedObjectInspector,
        numColumns, referedCols);
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    try {
      cachedNOBLazyStruct.init((BytesRefArrayWritable)blob);
      return cachedNOBLazyStruct;
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    throw new UnsupportedOperationException("Serialize method not supported");
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesRefArrayWritable.class;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
