package com.inmobi.yoda.hive;

import com.singularsys.jep.EvaluationException;
import com.singularsys.jep.functions.BinaryFunction;
import com.singularsys.jep.functions.NaryFunction;
import com.singularsys.jep.functions.UnaryFunction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.HashMap;

@Description(
  name = "yoda_udf",
  value = "_FUNC_(str, ...) - Call Yoda UDF")
public class ForwardingUDF extends UDF {
  public static final Log LOG = LogFactory.getLog(ForwardingUDF.class);
  HashMap<String, Object> udfObjects;

  public synchronized Object getCachedInstance(String udfName) throws HiveException {
    Object udf = udfObjects.get(udfName);
    if (udf == null)  {
      Class<?> udfClass = null;
      try {
        udfClass = Class.forName(udfName);
      } catch (ClassNotFoundException e) {
        throw new HiveException(e);
      }

      try {
        udf = udfClass.newInstance();
        udfObjects.put(udfName, udf);
      } catch (Exception ex) {
        throw new HiveException("Could not instantiate UDF " + udfName + " class: " + udfClass.getName());
      }
    }

    return udf;
  }

  public ForwardingUDF() {
    udfObjects = new HashMap<String, Object>();
  }


  public String evaluate(String yodaUdfName, Object ... udfArgs) throws HiveException {
    if (yodaUdfName == null || yodaUdfName.isEmpty()) {
      throw new HiveException("UDF name mising");
    }

    Object udf = getCachedInstance(yodaUdfName);

    try {
      Object result;
      if (udf instanceof UnaryFunction) {
        result = ((UnaryFunction) udf).eval(udfArgs[0]);
      } else if (udf instanceof BinaryFunction) {
        result = ((BinaryFunction) udf).eval(udfArgs[0], udfArgs[1]);
      } else if (udf instanceof NaryFunction) {
        result = ((NaryFunction) udf).eval(udfArgs);
      } else {
        throw new HiveException("Unknown UDF type: " + udf.getClass());
      }
      return result == null ? null : result.toString();
    } catch (EvaluationException e) {
      throw new HiveException("Error evaluating UDF " + yodaUdfName, e);
    }
  }

}
