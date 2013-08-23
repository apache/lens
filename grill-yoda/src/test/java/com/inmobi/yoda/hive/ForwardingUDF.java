package com.inmobi.yoda.hive;

import com.singularsys.jep.EvaluationException;
import com.singularsys.jep.functions.NaryFunction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Description(
  name = "yoda_udf",
  value = "_FUNC_(str, ...) - Call Yoda UDF")
public class ForwardingUDF extends UDF {
  static Map<String, Class<? extends NaryFunction>> udfToClass;
  static HashMap<String, NaryFunction> udfObjects;
  static boolean cacheUdfObjects;

  static {
    HiveConf conf = new HiveConf(ForwardingUDF.class);
    try {
      udfToClass = loadUdfToClassMapping(conf);
      cacheUdfObjects = conf.getBoolean("grill.yoda.udf.cache.instance", true);
      udfObjects = new HashMap<String, NaryFunction>(udfToClass.size());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Map<String, Class<? extends NaryFunction>> loadUdfToClassMapping(HiveConf conf) throws IOException {
    Map<String, Class<? extends NaryFunction>> udfToClassMap = new HashMap<String, Class<? extends NaryFunction>>();
    Map<String, String> udfClassNames = conf.getValByRegex("grill\\.yoda\\.udf\\.name\\.");
    for (String key : udfClassNames.keySet()) {
      udfToClassMap.put(key.substring("grill.yoda.udf.name.".length()),
        conf.getClass(key, NaryFunction.class, NaryFunction.class));
    }
    return udfToClassMap;
  }

  public static synchronized NaryFunction getCachedInstance(String udfName) throws HiveException {
    NaryFunction udf = udfObjects.get(udfName);
    if (udf == null)  {
      Class<? extends NaryFunction> udfClass = udfToClass.get(udfName);
      if (udfClass == null) {
        throw new HiveException("Could not find UDF class for name " + udfName);
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

  public Object evaluate(String yodaUdfName, Object ... udfArgs) throws HiveException {
    if (yodaUdfName == null || yodaUdfName.isEmpty()) {
      throw new HiveException("UDF name mising");
    }

    Class<? extends NaryFunction> udfClass = udfToClass.get(yodaUdfName.toLowerCase());
    if (udfClass == null) {
      throw new HiveException("Could not find UDF class for name " + yodaUdfName);
    }

    NaryFunction udf;
    if (cacheUdfObjects) {
      udf = getCachedInstance(yodaUdfName);
    } else {
      try {
        udf = udfClass.newInstance();
      } catch (Exception e) {
        throw new HiveException("Could not instantiate UDF " + yodaUdfName + " class: " + udfClass.getName(), e);
      }
    }

    try {
      return udf.eval(udfArgs);
    } catch (EvaluationException e) {
      throw new HiveException("Error evaluating UDF " + yodaUdfName + " class: " + udfClass.getName(), e);
    }
  }

}
