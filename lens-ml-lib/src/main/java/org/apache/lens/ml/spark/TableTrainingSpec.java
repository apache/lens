package org.apache.lens.ml.spark;

import com.google.common.base.Preconditions;
import org.apache.lens.api.LensException;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class TableTrainingSpec implements Serializable {
  public static final Log LOG = LogFactory.getLog(TableTrainingSpec.class);

  @Getter
  private transient RDD<LabeledPoint> trainingRDD;

  @Getter
  private transient RDD<LabeledPoint> testingRDD;

  @Getter
  private String database;

  @Getter
  private String table;

  @Getter
  private String partitionFilter;

  @Getter
  private List<String> featureColumns;

  @Getter
  private String labelColumn;

  @Getter
  transient private HiveConf conf;

  // By default all samples are considered for training
  private boolean splitTraining;
  private double trainingFraction = 1.0;
  int labelPos;
  int[] featurePositions;
  int numFeatures;

  transient JavaRDD<LabeledPoint> labeledRDD;

  public static TableTrainingSpecBuilder newBuilder() {
    return new TableTrainingSpecBuilder();
  }


  public static class TableTrainingSpecBuilder {
    final TableTrainingSpec spec;

    public TableTrainingSpecBuilder() {
      spec = new TableTrainingSpec();
    }

    public TableTrainingSpecBuilder hiveConf(HiveConf conf) {
      spec.conf = conf;
      return this;
    }

    public TableTrainingSpecBuilder database(String db) {
      spec.database = db;
      return this;
    }

    public TableTrainingSpecBuilder table(String table) {
      spec.table = table;
      return this;
    }

    public TableTrainingSpecBuilder partitionFilter(String partFilter) {
      spec.partitionFilter = partFilter;
      return this;
    }

    public TableTrainingSpecBuilder labelColumn(String labelColumn) {
      spec.labelColumn = labelColumn;
      return this;
    }

    public TableTrainingSpecBuilder featureColumns(List<String> featureColumns) {
      spec.featureColumns = featureColumns;
      return this;
    }

    public TableTrainingSpec build() {
      return spec;
    }

    public TableTrainingSpecBuilder trainingFraction(double trainingFraction) {
      Preconditions.checkArgument(trainingFraction >= 0 && trainingFraction <= 1.0,
        "Training fraction shoule be between 0 and 1");
      spec.trainingFraction = trainingFraction;
      spec.splitTraining = true;
      return this;
    }
  }

  public static class DataSample implements Serializable {
    private final LabeledPoint labeledPoint;
    private final double sample;

    public DataSample(LabeledPoint labeledPoint) {
      sample = Math.random();
      this.labeledPoint = labeledPoint;
    }
  }

  public static class TrainingFilter implements Function<DataSample, Boolean> {
    private double trainingFraction;

    public TrainingFilter(double fraction) {
      trainingFraction = fraction;
    }

    @Override
    public Boolean call(DataSample v1) throws Exception {
      return v1.sample <= trainingFraction;
    }
  }

  public static class TestingFilter implements Function<DataSample, Boolean> {
    private double trainingFraction;

    public TestingFilter(double fraction) {
      trainingFraction = fraction;
    }

    @Override
    public Boolean call(DataSample v1) throws Exception {
      return v1.sample > trainingFraction;
    }
  }

  public static class GetLabeledPoint implements Function<DataSample, LabeledPoint> {
    @Override
    public LabeledPoint call(DataSample v1) throws Exception {
      return v1.labeledPoint;
    }
  }


  boolean validate() {
    List<FieldSchema> columns;
    try {
      Hive metastoreClient = Hive.get(conf);
      Table tbl = (database == null) ? metastoreClient.getTable(table) : metastoreClient.getTable(database, table);
      columns = tbl.getAllCols();
    } catch (HiveException exc) {
      LOG.error("Error getting table info " + toString(), exc);
      return false;
    }

    boolean valid = false;
    if (columns != null && !columns.isEmpty()) {
      // Check labeled column
      List<String> columnNames = new ArrayList<String>();
      for (FieldSchema col : columns) {
        columnNames.add(col.getName());
      }

      // Need at least one feature column and one label column
      valid = columnNames.contains(labelColumn) && columnNames.size() > 1;

      if (valid) {
        labelPos = columnNames.indexOf(labelColumn);

        // Check feature columns
        if (featureColumns == null || featureColumns.isEmpty()) {
          // feature columns are not provided, so all columns except label column are feature columns
          featurePositions = new int[columnNames.size() - 1];
          int p = 0;
          for (int i = 0 ; i < columnNames.size(); i++) {
            if (i == labelPos) {
              continue;
            }
            featurePositions[p++] = i;
          }

          columnNames.remove(labelPos);
          featureColumns = columnNames;
        } else {
          // Feature columns were provided, verify all feature columns are present in the table
          valid = columnNames.containsAll(featureColumns);
          if (valid) {
            // Get feature positions
            featurePositions = new int[featureColumns.size()];
            for (int i = 0; i < featureColumns.size(); i++) {
              featurePositions[i] = columnNames.indexOf(featureColumns.get(i));
            }
          }
        }
        numFeatures = featureColumns.size();
      }
    }

    return valid;
  }

  public void createRDDs(JavaSparkContext sparkContext) throws LensException {
    // Validate the spec
    if (!validate()) {
      throw new LensException("Table spec not valid: " + toString());
    }

    // Get the RDD for table
    JavaPairRDD<WritableComparable, HCatRecord> tableRDD;
    try {
      tableRDD = HiveTableRDD.createHiveTableRDD(sparkContext, conf, database, table, partitionFilter);
    } catch (IOException e) {
      throw new LensException(e);
    }

    // Map into trainable RDD
    // TODO: Figure out a way to use custom value mappers
    FeatureValueMapper[] valueMappers = new FeatureValueMapper[numFeatures];
    final DoubleValueMapper doubleMapper = new DoubleValueMapper();
    for (int i = 0 ; i < numFeatures; i++) {
      valueMappers[i] = doubleMapper;
    }

    ColumnFeatureFunction trainPrepFunction =
      new ColumnFeatureFunction(featurePositions, valueMappers, labelPos, numFeatures, 0);
    labeledRDD = tableRDD.map(trainPrepFunction);

    if (splitTraining) {
      // We have to split the RDD between a training RDD and a testing RDD
      LOG.info("Splitting RDD for table " + database + "." + table + " with split fraction " + trainingFraction);
      JavaRDD<DataSample> sampledRDD = labeledRDD.map(new Function<LabeledPoint, DataSample>() {
        @Override
        public DataSample call(LabeledPoint v1) throws Exception {
          return new DataSample(v1);
        }
      });

      trainingRDD = sampledRDD.filter(new TrainingFilter(trainingFraction)).map(new GetLabeledPoint()).rdd();
      testingRDD = sampledRDD.filter(new TestingFilter(trainingFraction)).map(new GetLabeledPoint()).rdd();
    } else {
      LOG.info("Using same RDD for train and test");
      trainingRDD = testingRDD = labeledRDD.rdd();
    }
    LOG.info("Generated RDDs");
  }

  @Override
  public String toString() {
    return StringUtils.join(new String[]{database, table, partitionFilter, labelColumn}, ",");
  }
}
