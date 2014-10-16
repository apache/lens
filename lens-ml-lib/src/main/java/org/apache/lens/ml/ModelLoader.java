package org.apache.lens.ml;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.ml.MLModel;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Load ML models from a FS location
 */
public class ModelLoader {
  public static final String MODEL_PATH_BASE_DIR = "Lens.ml.model.basedir";
  public static final String MODEL_PATH_BASE_DIR_DEFAULT = "file:///tmp";

  public static final Log LOG = LogFactory.getLog(ModelLoader.class);
  public static final String TEST_REPORT_BASE_DIR = "Lens.ml.test.basedir";
  public static final String TEST_REPORT_BASE_DIR_DEFAULT = "file:///tmp/ml_reports";

  // Model cache settings
  public static final long MODEL_CACHE_SIZE = 10;
  public static final long MODEL_CACHE_TIMEOUT =  3600000L;// one hour
  private static Cache<Path, MLModel> modelCache = CacheBuilder.newBuilder()
    .maximumSize(MODEL_CACHE_SIZE)
    .expireAfterAccess(MODEL_CACHE_TIMEOUT, TimeUnit.MILLISECONDS)
    .build();

  public static Path getModelLocation(Configuration conf, String algorithm, String modelID) {
    String modelDataBaseDir = conf.get(MODEL_PATH_BASE_DIR, MODEL_PATH_BASE_DIR_DEFAULT);
    // Model location format - <modelDataBaseDir>/<algorithm>/modelID
    return new Path(new Path(new Path(modelDataBaseDir), algorithm), modelID);
  }

  public static MLModel loadModel(Configuration conf, String algorithm, String modelID) throws IOException {
    final Path modelPath = getModelLocation(conf, algorithm, modelID);
    LOG.info("Loading model for algorithm: " + algorithm + " modelID: " + modelID
      + " At path: " + modelPath.toUri().toString());
    try {
      return modelCache.get(modelPath, new Callable<MLModel>() {
        @Override
        public MLModel call() throws Exception {
          FileSystem fs = modelPath.getFileSystem(new HiveConf());
          if (!fs.exists(modelPath)) {
            throw new IOException("Model path not found " + modelPath.toString());
          }

          ObjectInputStream ois = null;
          try {
            ois = new ObjectInputStream(fs.open(modelPath));
            MLModel model = (MLModel) ois.readObject();
            LOG.info("Loaded model " + model.getId() + " from location " + modelPath);
            return model;
          } catch (ClassNotFoundException e) {
            throw new IOException(e);
          } finally {
            IOUtils.closeQuietly(ois);
          }
        }
      });
    } catch (ExecutionException exc) {
      throw new IOException(exc);
    }
  }

  public static void clearCache() {
    modelCache.cleanUp();
  }

  public static Path getTestReportPath(Configuration conf, String algorithm, String report) {
    String testReportDir = conf.get(TEST_REPORT_BASE_DIR, TEST_REPORT_BASE_DIR_DEFAULT);
    return new Path(new Path(testReportDir, algorithm), report);
  }

  public static void saveTestReport(Configuration conf, MLTestReport report) throws IOException {
    Path reportDir = new Path(conf.get(TEST_REPORT_BASE_DIR, TEST_REPORT_BASE_DIR_DEFAULT));
    FileSystem fs = reportDir.getFileSystem(conf);

    if (!fs.exists(reportDir)) {
      LOG.info("Creating test report dir " + reportDir.toUri().toString());
      fs.mkdirs(reportDir);
    }

    Path algoDir = new Path(reportDir, report.getAlgorithm());

    if (!fs.exists(algoDir)) {
      LOG.info("Creating algorithm report dir " + algoDir.toUri().toString());
      fs.mkdirs(algoDir);
    }

    ObjectOutputStream reportOutputStream = null;
    Path reportSaveLocation;
    try {
      reportSaveLocation = new Path(algoDir, report.getReportID());
      reportOutputStream = new ObjectOutputStream(fs.create(reportSaveLocation));
      reportOutputStream.writeObject(report);
      reportOutputStream.flush();
    } catch (IOException ioexc) {
      LOG.error("Error saving test report " + report.getReportID(), ioexc);
      throw ioexc;
    } finally {
      IOUtils.closeQuietly(reportOutputStream);
    }
    LOG.info("Saved report " + report.getReportID() + " at location " + reportSaveLocation.toUri());
  }

  public static MLTestReport loadReport(Configuration conf, String algorithm, String reportID) throws IOException {
    Path reportLocation = getTestReportPath(conf, algorithm, reportID);
    FileSystem fs = reportLocation.getFileSystem(conf);
    ObjectInputStream reportStream = null;
    MLTestReport report = null;

    try {
      reportStream = new ObjectInputStream(fs.open(reportLocation));
      report = (MLTestReport) reportStream.readObject();
    } catch (IOException ioex) {
      LOG.error("Error reading report " + reportLocation, ioex);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(reportStream);
    }
    return report;
  }

  public static void deleteModel(HiveConf conf, String algorithm, String modelID) throws IOException {
    Path modelLocation = getModelLocation(conf, algorithm, modelID);
    FileSystem fs = modelLocation.getFileSystem(conf);
    fs.delete(modelLocation, false);
  }

  public static void deleteTestReport(HiveConf conf, String algorithm, String reportID) throws IOException {
    Path reportPath = getTestReportPath(conf, algorithm, reportID);
    reportPath.getFileSystem(conf).delete(reportPath, false);
  }
}
