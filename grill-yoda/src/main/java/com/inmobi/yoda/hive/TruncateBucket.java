package com.inmobi.yoda.hive;


import com.inmobi.dw.yoda.mr.util.TruncateBucketHelper;
import com.inmobi.dw.yoda.proto.YodaMrSpec;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.text.ParseException;

@Description(
  name = "truncate_bucket",
  value = "_FUNC_(str, ...) - Call Yoda TrucncateBucketHelper.getBucket")
public class TruncateBucket extends UDF {
  static {
    FunctionRegistry.registerTemporaryFunction("truncate_bucket", TruncateBucket.class);
  }


  public String evaluate(String value, String level) throws HiveException {
    YodaMrSpec.Truncate truncateLevel = YodaMrSpec.Truncate.valueOf(level.toUpperCase());
    if (truncateLevel == null) {
      throw new HiveException("Invalid level " + level);
    }

    try {
      return TruncateBucketHelper.getBucket(value, truncateLevel);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}
