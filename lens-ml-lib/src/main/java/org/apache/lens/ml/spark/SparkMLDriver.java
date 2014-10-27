/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.ml.spark;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.ml.Algorithms;
import org.apache.lens.ml.MLDriver;
import org.apache.lens.ml.MLTrainer;
import org.apache.lens.ml.spark.trainers.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class SparkMLDriver.
 */
public class SparkMLDriver implements MLDriver {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(SparkMLDriver.class);

  /** The owns spark context. */
  private boolean ownsSparkContext = true;

  /**
   * The Enum SparkMasterMode.
   */
  private enum SparkMasterMode {
    // Embedded mode used in tests
    /** The embedded. */
    EMBEDDED,
    // Yarn client and Yarn cluster modes are used when deploying the app to Yarn cluster
    /** The yarn client. */
    YARN_CLIENT,

    /** The yarn cluster. */
    YARN_CLUSTER
  }

  /** The algorithms. */
  private final Algorithms algorithms = new Algorithms();

  /** The client mode. */
  private SparkMasterMode clientMode = SparkMasterMode.EMBEDDED;

  /** The is started. */
  private boolean isStarted;

  /** The spark conf. */
  private SparkConf sparkConf;

  /** The spark context. */
  private JavaSparkContext sparkContext;

  /**
   * Use spark context.
   *
   * @param jsc
   *          the jsc
   */
  public void useSparkContext(JavaSparkContext jsc) {
    ownsSparkContext = false;
    this.sparkContext = jsc;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLDriver#isTrainerSupported(java.lang.String)
   */
  @Override
  public boolean isTrainerSupported(String name) {
    return algorithms.isAlgoSupported(name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLDriver#getTrainerInstance(java.lang.String)
   */
  @Override
  public MLTrainer getTrainerInstance(String name) throws LensException {
    checkStarted();

    if (!isTrainerSupported(name)) {
      return null;
    }

    MLTrainer trainer = null;
    try {
      trainer = algorithms.getTrainerForName(name);
      if (trainer instanceof BaseSparkTrainer) {
        ((BaseSparkTrainer) trainer).setSparkContext(sparkContext);
      }
    } catch (LensException exc) {
      LOG.error("Error creating trainer object", exc);
    }
    return trainer;
  }

  /**
   * Register trainers.
   */
  private void registerTrainers() {
    algorithms.register(NaiveBayesTrainer.class);
    algorithms.register(SVMTrainer.class);
    algorithms.register(LogisticRegressionTrainer.class);
    algorithms.register(DecisionTreeTrainer.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLDriver#init(org.apache.lens.api.LensConf)
   */
  @Override
  public void init(LensConf conf) throws LensException {
    sparkConf = new SparkConf();
    registerTrainers();
    for (String key : conf.getProperties().keySet()) {
      if (key.startsWith("lens.ml.sparkdriver.")) {
        sparkConf.set(key.substring("lens.ml.sparkdriver.".length()), conf.getProperties().get(key));
      }
    }

    String sparkAppMaster = sparkConf.get("spark.master");
    if ("yarn-client".equalsIgnoreCase(sparkAppMaster)) {
      clientMode = SparkMasterMode.YARN_CLIENT;
    } else if ("yarn-cluster".equalsIgnoreCase(sparkAppMaster)) {
      clientMode = SparkMasterMode.YARN_CLUSTER;
    } else if ("local".equalsIgnoreCase(sparkAppMaster) || StringUtils.isBlank(sparkAppMaster)) {
      clientMode = SparkMasterMode.EMBEDDED;
    } else {
      throw new IllegalArgumentException("Invalid master mode " + sparkAppMaster);
    }

    if (clientMode == SparkMasterMode.YARN_CLIENT || clientMode == SparkMasterMode.YARN_CLUSTER) {
      String sparkHome = System.getenv("SPARK_HOME");
      if (StringUtils.isNotBlank(sparkHome)) {
        sparkConf.setSparkHome(sparkHome);
      }

      // If SPARK_HOME is not set, SparkConf can read from the Lens-site.xml or System properties.
      if (StringUtils.isBlank(sparkConf.get("spark.home"))) {
        throw new IllegalArgumentException("Spark home is not set");
      }

      LOG.info("Spark home is set to " + sparkConf.get("spark.home"));
    }

    sparkConf.setAppName("lens-ml");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLDriver#start()
   */
  @Override
  public void start() throws LensException {
    if (sparkContext == null) {
      sparkContext = new JavaSparkContext(sparkConf);
    }

    // Adding jars to spark context is only required when running in yarn-client mode
    if (clientMode != SparkMasterMode.EMBEDDED) {
      // TODO Figure out only necessary set of JARs to be added for HCatalog
      // Add hcatalog and hive jars
      String hiveLocation = System.getenv("HIVE_HOME");

      if (StringUtils.isBlank(hiveLocation)) {
        throw new LensException("HIVE_HOME is not set");
      }

      LOG.info("HIVE_HOME at " + hiveLocation);

      File hiveLibDir = new File(hiveLocation, "lib");
      FilenameFilter jarFileFilter = new FilenameFilter() {
        @Override
        public boolean accept(File file, String s) {
          return s.endsWith(".jar");
        }
      };

      List<String> jarFiles = new ArrayList<String>();
      // Add hive jars
      for (File jarFile : hiveLibDir.listFiles(jarFileFilter)) {
        jarFiles.add(jarFile.getAbsolutePath());
        LOG.info("Adding HIVE jar " + jarFile.getAbsolutePath());
        sparkContext.addJar(jarFile.getAbsolutePath());
      }

      // Add hcatalog jars
      File hcatalogDir = new File(hiveLocation + "/hcatalog/share/hcatalog");
      for (File jarFile : hcatalogDir.listFiles(jarFileFilter)) {
        jarFiles.add(jarFile.getAbsolutePath());
        LOG.info("Adding HCATALOG jar " + jarFile.getAbsolutePath());
        sparkContext.addJar(jarFile.getAbsolutePath());
      }

      // Add the current jar
      String[] LensSparkLibJars = JavaSparkContext.jarOfClass(SparkMLDriver.class);
      for (String LensSparkJar : LensSparkLibJars) {
        LOG.info("Adding Lens JAR " + LensSparkJar);
        sparkContext.addJar(LensSparkJar);
      }
    }

    isStarted = true;
    LOG.info("Created Spark context for app: '" + sparkContext.appName() + "', Spark master: " + sparkContext.master());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLDriver#stop()
   */
  @Override
  public void stop() throws LensException {
    if (!isStarted) {
      LOG.warn("Spark driver was not started");
      return;
    }
    isStarted = false;
    if (ownsSparkContext) {
      sparkContext.stop();
    }
    LOG.info("Stopped spark context " + this);
  }

  @Override
  public List<String> getTrainerNames() {
    return algorithms.getAlgorithmNames();
  }

  /**
   * Check started.
   *
   * @throws LensException
   *           the lens exception
   */
  public void checkStarted() throws LensException {
    if (!isStarted) {
      throw new LensException("Spark driver is not started yet");
    }
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

}
