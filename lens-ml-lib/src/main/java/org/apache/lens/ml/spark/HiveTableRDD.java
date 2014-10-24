package org.apache.lens.ml.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Create a JavaRDD based on a Hive table using HCatInputFormat.
 */
public class HiveTableRDD {

  /**
   * Creates the hive table rdd.
   *
   * @param javaSparkContext
   *          the java spark context
   * @param conf
   *          the conf
   * @param db
   *          the db
   * @param table
   *          the table
   * @param partitionFilter
   *          the partition filter
   * @return the java pair rdd
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public static JavaPairRDD<WritableComparable, HCatRecord> createHiveTableRDD(JavaSparkContext javaSparkContext,
      Configuration conf, String db, String table, String partitionFilter) throws IOException {

    HCatInputFormat.setInput(conf, db, table, partitionFilter);
    JavaPairRDD<WritableComparable, HCatRecord> rdd = javaSparkContext.newAPIHadoopRDD(conf, HCatInputFormat.class, // Input
                                                                                                                    // format
                                                                                                                    // class
        WritableComparable.class, // input key class
        HCatRecord.class); // input value class

    return rdd;
  }
}
