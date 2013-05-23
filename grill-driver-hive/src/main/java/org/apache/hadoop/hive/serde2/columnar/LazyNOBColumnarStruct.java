package org.apache.hadoop.hive.serde2.columnar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.inmobi.dw.yoda.proto.NetworkObject.KeyLessNetworkObject;

public class LazyNOBColumnarStruct extends ColumnarStructBase {
  private static final Log LOG = LogFactory.getLog(LazyNOBColumnarStruct.class);

  KeyLessNetworkObject.Builder nobBuilder = KeyLessNetworkObject.newBuilder();
  static final Map<String, Boolean> fieldTypes = new HashMap<String, Boolean>();
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

  class NOBFieldInfo {
    Object value = null;
    boolean retrievedData = false;
    FieldDescriptor fd;
    KeyLessNetworkObject nob;

    protected Object getField() {
      if (!retrievedData) {
        if (nob.hasField(fd)) {
          Object nobField = nob.getField(fd);
          value = nobField;
          if (nobField instanceof String) {
            if (fieldTypes.containsKey(fd.getName())) {
              try {
                if (fieldTypes.get(fd.getName())) {
                  long val = Long.parseLong((String) nobField);
                  value = val;
                } else {
                  double val = Double.parseDouble((String) nobField);
                  value = val;
                }
              } catch (NumberFormatException e) {
                LOG.warn("Error parsing " + fd.getName() 
                    + ", value " + nob.getField(fd), e);
              }
            }
          } 
        } else {
          value = null;
        }
        retrievedData = true;
      }
      return value;
    }

    public void init(KeyLessNetworkObject nob,
        FieldDescriptor fieldDescriptor) {
      this.nob = nob;
      this.fd = fieldDescriptor;
      retrievedData  = false;
    }
  }

  private NOBFieldInfo[] fieldInfoList = null;
  private ArrayList<Object> cachedList;

  private int numCols;
  private int[] referedCols;
  public LazyNOBColumnarStruct(ObjectInspector oi, int numCols, int[] referedCols) {
    super(oi, new ArrayList<Integer>());
    this.numCols = numCols;
    fieldInfoList = new NOBFieldInfo[numCols];
    for (int i = 0; i < numCols; i++) {
      fieldInfoList[i] = new NOBFieldInfo();
    }
    this.referedCols = referedCols;
  }

  /**
   * Get one field out of the struct.
   *
   * @param fieldID
   *          The field ID
   * @return The field as a LazyObject
   */
  public Object getField(int fieldID) {
    Object o = fieldInfoList[fieldID].getField();
    return o;
  }


  /**
   * Get the values of the fields as an ArrayList.
   *
   * @return The values of the fields as an ArrayList.
   */
  public ArrayList<Object> getFieldsAsList() {
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < fieldInfoList.length; i++) {
      cachedList.add(fieldInfoList[i].getField());
    }
    return cachedList;
  }

  public void init(BytesRefArrayWritable cols) {
    int numGroups = LazyNOBColumnarSerde.fieldGroups.size();
    nobBuilder.clear();
    for (int i : referedCols) {
      BytesRefWritable bytesRef = cols.get(i);
      if (bytesRef.getLength() != 0) {
        try {
          nobBuilder.mergeFrom(bytesRef.getData(),
              bytesRef.getStart(), bytesRef.getLength());
        } catch (Exception e) {
          throw new RuntimeException("Could not initialize network object", e);
        }
      }
    }
    KeyLessNetworkObject nob = nobBuilder.build();

    List<FieldDescriptor> fdList = new ArrayList<FieldDescriptor>();
    fdList.addAll(nob.getDescriptorForType().getFields());
    for (int i = 0; i < numCols; i++) {
      fieldInfoList[i].init(nob, fdList.get(i));
    }
  }

  @Override
  protected LazyObjectBase createLazyObjectBase(ObjectInspector arg0) {
    return null;
  }

  @Override
  protected int getLength(ObjectInspector arg0, ByteArrayRef arg1, int arg2,
      int arg3) {
    return 0;
  }
}
