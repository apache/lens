package com.inmobi.yoda.hive;

import com.singularsys.jep.EvaluationException;
import com.singularsys.jep.functions.BinaryFunction;
import com.singularsys.jep.functions.NaryFunction;
import com.singularsys.jep.functions.UnaryFunction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Description(
  name = "yoda_udf",
  value = "_FUNC_(str, ...) - Call Yoda UDF")
public class ForwardingUDF extends UDF {
  public static final Log LOG = LogFactory.getLog(ForwardingUDF.class);
  static Map<String, Class<?>> udfToClass;
  static boolean isMappingLoaded;
  static HashMap<String, Object> udfObjects;
  static boolean cacheUdfObjects;

  public static Map<String, Class<?>> loadUdfToClassMapping(Configuration conf)
    throws IOException, ClassNotFoundException {
    Map<String, Class<?>> udfToClassMap = new HashMap<String, Class<?>>();
    Iterator<Map.Entry<String, String>> confEntries = conf.iterator();
    while (confEntries.hasNext()) {
      Map.Entry<String, String> entry = confEntries.next();
      String keyStr = entry.getKey();
      if (keyStr.startsWith("grill.yoda.udf.name.")) {
        LOG.info("##@@ " + keyStr + "=" + entry.getValue());
        udfToClassMap.put(keyStr.substring("grill.yoda.udf.name.".length()),
          Class.forName(entry.getValue()));
      }
    }
    return udfToClassMap;
  }

  public static synchronized Object getCachedInstance(String udfName) throws HiveException {
    Object udf = udfObjects.get(udfName);
    if (udf == null)  {
      Class<?> udfClass = udfToClass.get(udfName);
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

  static void loadMapping(Configuration conf) {
    if (!isMappingLoaded) {
      LOG.info("## Loading mapping");
      try {
        udfToClass = loadUdfToClassMapping(conf);
        isMappingLoaded = true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }



  public ForwardingUDF() {
    loadMapping(new HiveConf());
  }


  public String evaluate(String yodaUdfName, Object ... udfArgs) throws HiveException {
    if (yodaUdfName == null || yodaUdfName.isEmpty()) {
      throw new HiveException("UDF name mising");
    }

    Class<?> udfClass = udfToClass.get(yodaUdfName.toLowerCase());
    if (udfClass == null) {
      throw new HiveException("Could not find UDF class for name " + yodaUdfName + " registry="
        + udfToClass.toString());
    }

    Object udf;
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
      throw new HiveException("Error evaluating UDF " + yodaUdfName + " class: " + udfClass.getName(), e);
    }
  }

}
