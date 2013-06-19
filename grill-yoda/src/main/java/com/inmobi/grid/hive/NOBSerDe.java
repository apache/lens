package com.inmobi.grid.hive;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.inmobi.dw.yoda.proto.NetworkObject.KeyLessNetworkObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.serde.serdeConstants.*;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.*;

public class NOBSerDe implements SerDe {
  private static final Log LOG = LogFactory.getLog(
      NOBSerDe.class);

  private ObjectInspector rowObjectInspector;
  private Map<Integer, List<String>> fieldGroups;

  boolean logonce = true;
  private static final Map<String, Boolean> fieldTypes = new HashMap<String, Boolean>();
  static {
    fieldTypes.put("rq_ccid", true);
    fieldTypes.put("rq_ccode", true);
    fieldTypes.put("rq_handsetid", true);
    fieldTypes.put("rq_is_3d", true);
    fieldTypes.put("rq_pgreq", true);
    fieldTypes.put("rq_mkvldpgreq", true);
    fieldTypes.put("rq_adreq", true);
    fieldTypes.put("rq_mkvldadreq", true);
    fieldTypes.put("rq_adimp", true);
    fieldTypes.put("rq_pgimp", true);
    fieldTypes.put("rq_mkcpcadserved", true);
    fieldTypes.put("rq_unbilled_cpm_adimp", true);
    fieldTypes.put("rq_overbill_cpm_adimp", true);
    fieldTypes.put("rm_view_count", true);
    fieldTypes.put("rm_expand_count", true);
    fieldTypes.put("rm_dismiss_count", true);
    fieldTypes.put("rm_auto_dismiss_count", true);
    fieldTypes.put("rm_engagement_duration", true);
    fieldTypes.put("rm_dismiss_duration", true);
    fieldTypes.put("rm_auto_dismiss_duration", true);
    fieldTypes.put("rm_interaction_count", true);
    fieldTypes.put("rm_cta_count", true);
    fieldTypes.put("rm_page_view_count", true);
    fieldTypes.put("rm_page_view_duration", true);
    fieldTypes.put("rm_media_play_count", true);
    fieldTypes.put("rm_media_auto_play_count", true);
    fieldTypes.put("rm_media_start_count", true);
    fieldTypes.put("rm_media_q1_count", true);
    fieldTypes.put("rm_media_q2_count", true);
    fieldTypes.put("rm_media_q3_count", true);
    fieldTypes.put("rm_media_end_count", true);
    fieldTypes.put("rm_media_stop_count", true);
    fieldTypes.put("rm_media_duration", true);
    fieldTypes.put("rm_adhoc_value_count", true);
    fieldTypes.put("rm_engagement_count", true);
    fieldTypes.put("cl_overbill_cpc_clicks", true);
    fieldTypes.put("cl_unbilled_cpc_clicks", true);
    fieldTypes.put("cl_terminated_clicks", true);
    fieldTypes.put("cl_fraud_clicks", true);
    fieldTypes.put("bl_billedcount", true); 
    fieldTypes.put("bl_cpc", false);
    fieldTypes.put("bl_agencycpc", false);
    fieldTypes.put("bl_pubcpc", false);
    fieldTypes.put("bl_overbill_revenue", false);
    fieldTypes.put("bl_nofund_burn", false);
    fieldTypes.put("bl_nondiscounted_burn", false);
    fieldTypes.put("dl_matched_count", true);
    fieldTypes.put("dl_joined_count", true);
    fieldTypes.put("dl_unjoined_count", true);
    fieldTypes.put("dl_duplicate_count", true);
    fieldTypes.put("dcp_pgreq", true);
    fieldTypes.put("dcp_mkvldpgreq", true);
    fieldTypes.put("dcp_adreq", true);
    fieldTypes.put("dcp_mkvldadreq", true);
    fieldTypes.put("dcp_pgimp", true);
    fieldTypes.put("dcp_mkcpcadserved", true);
    fieldTypes.put("dcp_total_click_count", true);
    fieldTypes.put("dcp_fraud_clicks", true);
    fieldTypes.put("dcp_billed_cpc_click", true);
    fieldTypes.put("dcp_nonbillable_cpc_click", true);
    fieldTypes.put("dcp_cpm_impression", true);
    fieldTypes.put("dcp_non_billable_cpm_impression", true);
    fieldTypes.put("dcp_spend", false);
    fieldTypes.put("dcp_publisher_revenue", false);
    fieldTypes.put("dcp_agency_revenue", false);
    fieldTypes.put("dcp_cpm_clicks", true);
    fieldTypes.put("dcp_cpc_beacon", true);
    fieldTypes.put("dcp_cpm_beacon", true);
    fieldTypes.put("user_mkvldpgreq", true);
    fieldTypes.put("user_mkvldadreq", true);
    fieldTypes.put("user_adimp", true);
    fieldTypes.put("user_pgimp", true);
    fieldTypes.put("udid_encryption_type", true);
    fieldTypes.put("do_not_use_cl_total_bad_clicks", true);
  }

  private static Map<String, Descriptors.FieldDescriptor> nameToDescriptorMap;

  private static List<FieldDescriptor> fdList = new ArrayList<FieldDescriptor>();

  static {
    KeyLessNetworkObject message = KeyLessNetworkObject.getDefaultInstance();
    nameToDescriptorMap = new HashMap<String, Descriptors.FieldDescriptor>();
    for (Descriptors.FieldDescriptor des : message.getDescriptorForType().getFields()) {
      nameToDescriptorMap.put(des.getName(), des);
    }

    fdList.addAll(nameToDescriptorMap.values());
  }

  @Override
  public void initialize(Configuration conf,
      Properties tbl) throws SerDeException {

    /*
    for (Map.Entry<String,String> entry : conf) {
        System.out.println(entry.getKey() + ": " + entry.getValue());
    }
     */
    String columnNameProperty = tbl.getProperty(LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(LIST_COLUMN_TYPES);


    LOG.info("columnNameProperty " + columnNameProperty + " columnTypeProperty:" + columnTypeProperty);


    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    int numColumns = columnNames.size();


    List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>(columnNames.size());
    ObjectInspector colObjectInspector;
    for (int col = 0; col < numColumns; col++) {
      colObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
          columnTypes.get(col));
      objectInspectors.add(colObjectInspector);
    }
    rowObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, objectInspectors);

    fieldGroups = new TreeMap<Integer, List<String>>();
    Properties fieldProps = new Properties();
    try {
      fieldProps.load(getClass().getResourceAsStream("/field_group.properties"));

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
      throw new SerDeException(e);
    }
    LOG.info("Initialized serde");
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesRefArrayWritable.class;
  }

  @Override
  public Writable serialize(Object obj,
      ObjectInspector objInspector) throws SerDeException {
    throw new UnsupportedOperationException("Serialize method not supported");
  }

  //TODO deserialize need to use LazyStruct.
  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    try {
      KeyLessNetworkObject.Builder nobBuilder = KeyLessNetworkObject.newBuilder();
      BytesRefArrayWritable data = (BytesRefArrayWritable) blob;
      int numGroups = fieldGroups.size();
      boolean[] nobFilled = new boolean[numGroups];
      for (int i = 0; i < numGroups; ++i) {
        BytesRefWritable bytesRef = data.get(i);
        if (bytesRef.getLength() != 0) {
          nobBuilder.mergeFrom(bytesRef.getData(),
              bytesRef.getStart(), bytesRef.getLength());
          nobFilled[i] = true;
        } else {
          nobFilled[i] = false;
        }
      }
      KeyLessNetworkObject nob = nobBuilder.build();
      int i = 0;
      List<Object> objects = new ArrayList<Object>(
          nob.getDescriptorForType().getFields().size());
      for (Descriptors.FieldDescriptor field : nob.getDescriptorForType()
          .getFields()) {
        Object column = null;
        Object nobField = nob.getField(field);
        column = nobField;
        if (nobField instanceof String) {
          if (fieldTypes.containsKey(field.getName())) {
            try {
              if (fieldTypes.get(field.getName())) {
                long val = Long.parseLong((String) nobField);
                column = val;
              } else {
                double val = Double.parseDouble((String) nobField);
                column = val;
              }
            } catch (NumberFormatException e) {
              System.out.println("Error parsing " + field.getName() 
                  + ", value " + nob.getField(field));
            }
          }
        }
        objects.add(column);
        i++;
      }
      if (logonce) {
        StringBuilder str = new StringBuilder();
        for (Object o : objects) {
          str.append("object "+ i + ": " + o);
        }
        LOG.info("Objects:" + str.toString());
        logonce = false;
      }
      return objects;
    } catch (IOException e) {
      e.printStackTrace();
      throw new SerDeException(e);
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
