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
package org.apache.lens.server;

import static org.apache.lens.server.api.LensConfConstants.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.*;

import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.error.ErrorCollectionFactory;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.util.LensUtil;
import org.apache.lens.server.metrics.MetricsServiceImpl;
import org.apache.lens.server.model.LogSegregationContext;
import org.apache.lens.server.model.MappedDiagnosticLogSegregationContext;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.stats.StatisticsService;
import org.apache.lens.server.user.UserConfigLoaderFactory;
import org.apache.lens.server.user.usergroup.UserGroupLoaderFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manage lifecycle of all Lens services
 */
@Slf4j
public class LensServices extends CompositeService implements ServiceProvider {

  /** The Constant LENS_SERVICES_NAME. */
  public static final String LENS_SERVICES_NAME = "lens_services";

  /** Constant for FileSystem auto close on shutdown config */
  private static final String FS_AUTOMATIC_CLOSE = "fs.automatic.close";
  private static final String FS_IO_FILE_BUFFER_SIZE = "io.file.buffer.size";

  /** The instance. */
  private static LensServices instance = new LensServices(LENS_SERVICES_NAME,
    new MappedDiagnosticLogSegregationContext());

  /** The conf. */
  private HiveConf conf;

  /** The cli service. */
  private CLIService cliService;

  /** The services. */
  private final Map<String, Service> services = new LinkedHashMap<String, Service>();

  /** The lens services. */
  private final List<BaseLensService> lensServices = new ArrayList<BaseLensService>();

  /** The persist dir. */
  private Path persistDir;

  /** The persistence file system. */
  private FileSystem persistenceFS;

  /** The stopping. */
  private boolean stopping = false;

  /**
   * The metrics service.
   */
  private MetricsService metricsService;

  /**
   * The Constant SERVER_STATE_PERSISTENCE_ERRORS.
   */
  public static final String SERVER_STATE_PERSISTENCE_ERRORS = "total-server-state-persistence-errors";

  /**
   * The Constant SERVER_STATE_PERSISTENCE_ERRORS.
   */
  public static final String KDC_LOGIN_ERRORS = "total-kdc-login-errors";

  /** The service mode. */
  @Getter
  @Setter
  private ServiceMode serviceMode;

  /** Scheduled Executor which persists the server state periodically*/
  private ScheduledExecutorService serverSnapshotScheduler;

  /** Scheduled Executor to refresh kerberos tgt*/
  private ScheduledExecutorService kerberosTgtScheduler;

  /* Lock for synchronizing persistence of LensServices state */
  private final Object statePersistenceLock = new Object();

  @Getter
  private ErrorCollection errorCollection;

  private boolean isServerStatePersistenceEnabled;

  private long serverStatePersistenceInterval;

  private long serverKdcLoginInterval;

  @Getter
  private final LogSegregationContext logSegregationContext;

  /**
   * Incr counter.
   *
   * @param counter the counter
   */
  private void incrCounter(String counter) {
    getMetricService().incrCounter(LensServices.class, counter);
  }

  /**
   * Gets counter value.
   *
   * @param counter the counter
   */
  private long getCounter(String counter) {
    return getMetricService().getCounter(LensServices.class, counter);
  }

  public static LensException processLensException(LensException exc) {
    if (exc != null) {
      exc.buildLensErrorTO(get().getErrorCollection());
    }
    return exc;
  }

  /**
   * The Enum ServiceMode.
   */
  public enum ServiceMode {

    /** The read only. */
    READ_ONLY, // All requests on sesssion resource and Only GET requests on all other resources
    /** The metastore readonly. */
    METASTORE_READONLY, // Only GET requests on metastore service and
    // all other requests on other services are accepted
    /** The metastore nodrop. */
    METASTORE_NODROP, // DELETE requests on metastore are not accepted
    /** The open. */
    OPEN // All requests are accepted
  }

  /**
   * Instantiates a new lens services.
   *
   * @param name the name
   */
  public LensServices(String name, @NonNull final LogSegregationContext logSegregationContext) {
    super(name);
    this.logSegregationContext = logSegregationContext;
  }

  // This is only for test, to simulate a restart of the server
  public static void setInstance(LensServices newInstance) {
    instance = newInstance;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.CompositeService#init(org.apache.hadoop.hive.conf.HiveConf)
   */
  @SuppressWarnings("unchecked")
  @Override
  public synchronized void init(HiveConf hiveConf) {

    if (getServiceState() == STATE.NOTINITED) {

      initializeErrorCollection();
      conf = hiveConf;
      conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, LensSessionImpl.class.getCanonicalName());
      serviceMode = conf.getEnum(SERVER_MODE,
        ServiceMode.valueOf(DEFAULT_SERVER_MODE));
      cliService = new CLIService(null);
      UserConfigLoaderFactory.init(conf);
      UserGroupLoaderFactory.init(conf);
      // Add default services
      addService(cliService);
      addService(new EventServiceImpl(LensEventService.NAME));
      addService(new MetricsServiceImpl(MetricsService.NAME));
      addService(new StatisticsService(StatisticsService.STATS_SVC_NAME));

      // Add configured services, these are instances of LensService which need a CLIService instance
      // for session management
      String[] serviceNames = conf.getStrings(SERVICE_NAMES);
      for (String sName : serviceNames) {
        try {

          String serviceClassName = conf.get(getServiceImplConfKey(sName));

          if (StringUtils.isBlank(serviceClassName)) {
            log.warn("Invalid class for service {} class={}", sName, serviceClassName);
            continue;
          }

          Class<?> cls = Class.forName(serviceClassName);

          if (BaseLensService.class.isAssignableFrom(cls)) {
            Class<? extends BaseLensService> serviceClass = (Class<? extends BaseLensService>) cls;
            log.info("Adding {}  service with {}", sName, serviceClass);
            Constructor<?> constructor = serviceClass.getConstructor(CLIService.class);
            BaseLensService service = (BaseLensService) constructor.newInstance(cliService);
            addService(service);
            lensServices.add(service);
          } else if (Service.class.isAssignableFrom(cls)) {
            Class<? extends Service> serviceClass = (Class<? extends Service>) cls;
            // Assuming default constructor
            Service svc = serviceClass.newInstance();
            addService(svc);
          } else {
            log.warn("Unsupported service class {} for service {}", serviceClassName, sName);
          }
        } catch (Exception e) {
          log.warn("Could not add service:{}", sName, e);
          throw new RuntimeException("Could not add service:" + sName, e);
        }
      }

      for (Service svc : getServices()) {
        services.put(svc.getName(), svc);
      }

      // This will init all services in the order they were added
      super.init(conf);

      // setup persisted state
      isServerStatePersistenceEnabled = conf.getBoolean(SERVER_STATE_PERSISTENCE_ENABLED,
        DEFAULT_SERVER_STATE_PERSISTENCE_ENABLED);
      if (isServerStatePersistenceEnabled) {
        String persistPathStr = conf.get(SERVER_STATE_PERSIST_LOCATION,
          DEFAULT_SERVER_STATE_PERSIST_LOCATION);
        persistDir = new Path(persistPathStr);
        try {
          Configuration configuration = new Configuration(conf);
          configuration.setBoolean(FS_AUTOMATIC_CLOSE, false);

          int outStreamBufferSize = conf.getInt(STATE_PERSIST_OUT_STREAM_BUFF_SIZE,
            DEFAULT_STATE_PERSIST_OUT_STREAM_BUFF_SIZE);
          configuration.setInt(FS_IO_FILE_BUFFER_SIZE, outStreamBufferSize);
          log.info("STATE_PERSIST_OUT_STREAM_BUFF_SIZE IN BYTES:{}", outStreamBufferSize);
          persistenceFS = FileSystem.newInstance(persistDir.toUri(), configuration);
          setupPersistedState();
        } catch (Exception e) {
          log.error("Could not recover from persisted state", e);
          throw new RuntimeException("Could not recover from persisted state", e);
        }
        serverStatePersistenceInterval = conf.getLong(SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS,
          DEFAULT_SERVER_STATE_PERSISTENCE_INTERVAL_MILLIS);
      }
      log.info("Initialized services: {}", services.keySet().toString());
    }
  }

  /**
   * Setup KDC logint thread.
   *
   */
  private void enableKDCLoginThread() {

    try {
      LensUtil.refreshLensTGT(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    serverKdcLoginInterval = conf.getInt(LensConfConstants.KDC_LOGIN_SERVICE_INTERVAL_IN_MINUTES,
            LensConfConstants.DEFAULT_KDC_LOGIN_SERVICE_INTERVAL_IN_MINUTES);

    ThreadFactory factory = new BasicThreadFactory.Builder()
            .namingPattern("Lens-server-refresh-tgt-Thread-%d")
            .daemon(true)
            .priority(Thread.NORM_PRIORITY)
            .build();
    kerberosTgtScheduler = Executors.newSingleThreadScheduledExecutor(factory);
    kerberosTgtScheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          final String runId = UUID.randomUUID().toString();
          logSegregationContext.setLogSegregationId(runId);
          LensUtil.refreshLensTGT(conf);
          log.info("KDC login successful for lens.");
        } catch (Exception e) {
          incrCounter(SERVER_STATE_PERSISTENCE_ERRORS);
          log.error("Unable to login to KDC...", e);
        }
      }
    }, 0, serverKdcLoginInterval, TimeUnit.MINUTES);
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.CompositeService#start()
   */
  public synchronized void start() {

    if (cliService.getHiveConf().getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION)
            .equals(HiveAuthFactory.AuthTypes.KERBEROS.toString())) {

      enableKDCLoginThread();
      log.info("Enabled kerberos tgt login at {} minutes interval", serverKdcLoginInterval);
    }

    if (getServiceState() != STATE.STARTED) {
      super.start();
    }

    if (!isServerStatePersistenceEnabled) {
      log.info("Server restart is not enabled. Not persisting lens server state");
    } else {
      ThreadFactory factory = new BasicThreadFactory.Builder()
        .namingPattern("Lens-server-snapshotter-Thread-%d")
        .daemon(true)
        .priority(Thread.NORM_PRIORITY)
        .build();
      serverSnapshotScheduler = Executors.newSingleThreadScheduledExecutor(factory);
      serverSnapshotScheduler.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            final String runId = UUID.randomUUID().toString();
            logSegregationContext.setLogSegregationId(runId);
            persistLensServiceState();
            log.info("SnapShot of Lens Services created");
          } catch (Exception e) {
            incrCounter(SERVER_STATE_PERSISTENCE_ERRORS);
            log.error("Unable to persist lens server state", e);
          }
        }
      }, serverStatePersistenceInterval, serverStatePersistenceInterval, TimeUnit.MILLISECONDS);

      log.info("Enabled periodic persistence of lens server state at {} millis interval",
              serverStatePersistenceInterval);

    }

  }

  /**
   * Setup persisted state.
   *
   * @throws IOException            Signals that an I/O exception has occurred.
   * @throws ClassNotFoundException the class not found exception
   */
  private void setupPersistedState() throws IOException, ClassNotFoundException {
    for (BaseLensService service : lensServices) {
      ObjectInputStream in = null;
      Path path = getServicePersistPath(service);
      try {
        try {
          in = new ObjectInputStream(persistenceFS.open(path));
        } catch (FileNotFoundException fe) {
          log.warn("Persisted state not available for service: {} at: {}", service.getName(), path);
          continue;
        }
        service.readExternal(in);
        log.info("Recovered service {} from persisted state {}", service.getName(), path);
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }
  }

  /**
   * Persist lens service state.
   */
  private void persistLensServiceState() {
    synchronized (statePersistenceLock) {
      log.info("Persisting server state in {}", persistDir);
      String now = "" + System.currentTimeMillis();
      for (BaseLensService service : lensServices) {
        try {
          persistState(service, now);
        } catch (Exception e) {
          incrCounter(SERVER_STATE_PERSISTENCE_ERRORS);
          log.error("Error while persisting state for service {}", service.getName(), e);
        }
      }
    }
  }

  private void persistState(BaseLensService service, String time) throws IOException {
    log.info("Persisting state of service: {}", service.getName());
    Path serviceWritePath = new Path(persistDir, service.getName() + ".out" + "." + time);
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(persistenceFS.create(serviceWritePath));
      service.writeExternal(out);
    } finally {
      if (out != null) {
        out.close();
      }
    }
    Path servicePath = getServicePersistPath(service);
    if (persistenceFS.exists(servicePath)) {
      // delete the destination first, because rename is no-op in HDFS, if destination exists
      if (!persistenceFS.delete(servicePath, true)) {
        throw new IOException("Failed to delete " + servicePath);
      }
    }
    if (!persistenceFS.rename(serviceWritePath, servicePath)) {
      throw new IOException("Failed to rename " + serviceWritePath + " to " + servicePath);
    }
    log.info("Persisted service {} to [{}]", service.getName(), servicePath);
  }

  /**
   * Gets the service persist path.
   *
   * @param service the service
   * @return the service persist path
   */
  private Path getServicePersistPath(BaseLensService service) {
    return new Path(persistDir, service.getName() + ".final");
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.CompositeService#stop()
   */
  public synchronized void stop() {
    if (getServiceState() != STATE.STOPPED) {
      log.info("Stopping lens server");
      stopping = true;
      for (BaseLensService service : lensServices) {
        service.prepareStopping();
      }

      if (isServerStatePersistenceEnabled) {
        try {
          //1. shutdown serverSnapshotScheduler gracefully by allowing already triggered task (if any) to finish
          if (serverSnapshotScheduler != null) {
            serverSnapshotScheduler.shutdown();
            try { //Wait for shutdown. Shutdown should be immediate in case no task is running at this point
              while (!serverSnapshotScheduler.awaitTermination(1, TimeUnit.MINUTES)) {
                log.info("Waiting for Lens-server-snapshotter to shutdown gracefully...");
              }
            } catch (InterruptedException e) {
              log.error("Lens-server-snapshotter interrupted while shutting down", e);
            }
            log.info("Lens-server-snapshotter was shutdown");
          } else {
            log.info("Lens-server-snapshotter wasn't started, so no need to shutdown");
          }
          //2. persist the latest state of all the services
          persistLensServiceState();
        } finally {
          try {
            persistenceFS.close();
            log.info("Persistence File system object close complete");
          } catch (Exception e) {
            log.error("Error while closing Persistence File system object", e);
          }
        }
      }

      super.stop();

    }
  }

  public STATE getServiceState() {
    return super.getServiceState();
  }

  public boolean isStopping() {
    return stopping;
  }

  /**
   * Gets the.
   *
   * @return the lens services
   */
  public static LensServices get() {
    return instance;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.ServiceProvider#getService(java.lang.String)
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T extends Service> T getService(String sName) {
    return (T) services.get(sName);
  }

  public List<BaseLensService> getLensServices() {
    return lensServices;
  }

  private void initializeErrorCollection() {
    try {
      errorCollection = new ErrorCollectionFactory().createErrorCollection();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not create error collection.", e);
    }
  }


  private MetricsService getMetricService() {
    if (metricsService == null) {
      metricsService = LensServices.get().getService(MetricsService.NAME);
      if (metricsService == null) {
        throw new NullPointerException("Could not get metrics service");
      }
    }
    return metricsService;
  }
}
