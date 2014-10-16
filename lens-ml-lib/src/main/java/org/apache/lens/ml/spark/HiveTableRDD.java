package org.apache.lens.ml.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Create a JavaRDD based on a Hive table using HCatInputFormat
 */
public class HiveTableRDD {

  public static JavaPairRDD<WritableComparable,HCatRecord> createHiveTableRDD(
    JavaSparkContext javaSparkContext,
    Configuration conf,
    String db,
    String table,
    String partitionFilter) throws IOException {

    HCatInputFormat.setInput(conf, db, table, partitionFilter);
    JavaPairRDD<WritableComparable, HCatRecord> rdd =
      javaSparkContext.newAPIHadoopRDD(conf,
        HCatInputFormat.class, // Input format class
        WritableComparable.class, // input key class
        HCatRecord.class); // input value class

    return rdd;
  }
}
