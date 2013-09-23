package com.inmobi.yoda.hive;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;

public class UdfRegistry {
  /**
   * This has to be called before invoking Yoda UDFs.
   * If this is not called, then each UDF must be registered using equivalent HQL syntax to create temporary functions
   */
  public static void registerUDFS() {
    FunctionRegistry.registerTemporaryFunction("str_yoda_udf", StringForwardingUDF.class);
    FunctionRegistry.registerTemporaryFunction("bool_yoda_udf", BooleanForwardingUDF.class);
    FunctionRegistry.registerTemporaryFunction("int_yoda_udf", IntForwardingUDF.class);
    FunctionRegistry.registerTemporaryFunction("double_yoda_udf", DoubleForwardingUDF.class);
    FunctionRegistry.registerTemporaryFunction("long_yoda_udf", LongForwardingUDF.class);
  }
}
