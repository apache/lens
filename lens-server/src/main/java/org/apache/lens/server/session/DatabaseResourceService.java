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
package org.apache.lens.server.session;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.util.ScannedPaths;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;

import lombok.extern.slf4j.Slf4j;

/**
 * Service to maintain DB specific static jars. This service is managed by HiveSessionService.
 */
@Slf4j
public class DatabaseResourceService extends AbstractService {
  public static final String NAME = "database-resources";
  private Map<String, ClassLoader> classLoaderCache;
  private Map<String, List<LensSessionImpl.ResourceEntry>> dbResEntryMap;

  /**
   * The metrics service.
   */
  private MetricsService metricsService;

  /**
   * The Constant LOAD_RESOURCES_ERRORS.
   */
  public static final String LOAD_RESOURCES_ERRORS = "total-load-resources-errors";

  /**
   * Incr counter.
   *
   * @param counter the counter
   */
  private void incrCounter(String counter) {
    getMetrics().incrCounter(DatabaseResourceService.class, counter);
  }


  public DatabaseResourceService(String name) {
    super(name);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    classLoaderCache = new HashMap<>();
    dbResEntryMap = new HashMap<>();
  }

  @Override
  public synchronized void start() {
    super.start();

    try {
      log.info("Starting loading DB specific resources");
      loadDbResourceEntries();
      loadResources();
    } catch (LensException e) {
      incrCounter(LOAD_RESOURCES_ERRORS);
      log.warn("Failed to load DB resource mapping, resources must be added explicitly to session.");
    }
  }


  @Override
  public synchronized void stop() {
    super.stop();
    classLoaderCache.clear();
    dbResEntryMap.clear();
  }

  private void loadDbResourceEntries() throws LensException {
    // Read list of databases in
    FileSystem serverFs = null;

    try {
      String resTopDir =
        getHiveConf().get(LensConfConstants.DATABASE_RESOURCE_DIR, LensConfConstants.DEFAULT_DATABASE_RESOURCE_DIR);
      log.info("Database specific resources at {}", resTopDir);

      Path resTopDirPath = new Path(resTopDir);
      serverFs = FileSystem.newInstance(resTopDirPath.toUri(), getHiveConf());
      if (!serverFs.exists(resTopDirPath)) {
        incrCounter(LOAD_RESOURCES_ERRORS);
        log.warn("Database resource location does not exist - {}. Database jars will not be available", resTopDir);
        return;
      }

      // Look for db dirs
      for (FileStatus dbDir : serverFs.listStatus(resTopDirPath)) {
        Path dbDirPath = dbDir.getPath();
        if (serverFs.isDirectory(dbDirPath)) {
          String dbName = dbDirPath.getName();
          // Get all resources for that db
          findResourcesInDir(serverFs, dbName, dbDirPath);
        } else {
          log.warn("DB resource DIR is not a directory: {}", dbDirPath);
        }
      }

      log.debug("Found resources {}", dbResEntryMap);
    } catch (IOException io) {
      log.error("Error getting list of dbs to load resources from", io);
      throw new LensException(io);
    } finally {
      if (serverFs != null) {
        try {
          serverFs.close();
        } catch (IOException e) {
          log.error("Error closing file system instance", e);
        }
      }
    }
  }

  private void findResourcesInDir(FileSystem serverFs, String database, Path dbDirPath) throws IOException {
    // Check if order file is present in the directory
    List<String> jars = new ScannedPaths(dbDirPath, "jar").getFinalPaths();
    if (jars != null && !jars.isEmpty()) {
      log.info("{} picking jar in jar_order: {}", database, jars);
      for (String jar : jars) {
        if (StringUtils.isBlank(jar)) {
          // skipping empty lines. usually the last line could be empty
          continue;
        }
        Path jarFilePath = new Path(dbDirPath, jar);
        if (!jar.endsWith(".jar") || !serverFs.exists(jarFilePath)) {
          log.info("Resource skipped {} for db {}", jarFilePath, database);
          continue;
        }
        addResourceEntry(new LensSessionImpl.ResourceEntry("jar", jarFilePath.toUri().toString()), database);
      }
    } else {
      log.info("{} picking jars in file list order", database);
      for (FileStatus dbResFile : serverFs.listStatus(dbDirPath)) {
        // Skip subdirectories
        if (serverFs.isDirectory(dbResFile.getPath())) {
          continue;
        }

        String dbResName = dbResFile.getPath().getName();
        String dbResUri = dbResFile.getPath().toUri().toString();

        if (dbResName.endsWith(".jar")) {
          addResourceEntry(new LensSessionImpl.ResourceEntry("jar", dbResUri), database);
        } else {
          log.info("Resource skipped {} for db {}", dbResFile.getPath(), database);
        }
      }
    }
  }

  private void addResourceEntry(LensSessionImpl.ResourceEntry entry, String dbName) {
    log.info("Adding resource entry {} for {}", entry.getUri(), dbName);
    synchronized (dbResEntryMap) {
      List<LensSessionImpl.ResourceEntry> dbEntryList = dbResEntryMap.get(dbName);
      if (dbEntryList == null) {
        dbEntryList = new ArrayList<>();
        dbResEntryMap.put(dbName, dbEntryList);
      }
      dbEntryList.add(entry);
    }
  }

  /**
   * Load DB specific resources
   */
  public void loadResources() {
    for (String db : dbResEntryMap.keySet()) {
      try {
        createClassLoader(db);
        loadDBJars(db, dbResEntryMap.get(db), true);
        log.info("Loaded resources for db {} resources: {}", db, dbResEntryMap.get(db));
      } catch (LensException exc) {
        incrCounter(LOAD_RESOURCES_ERRORS);
        log.warn("Failed to load resources for db {}", db, exc);
        classLoaderCache.remove(db);
      }
    }
  }

  protected void createClassLoader(String database) throws LensException {
    classLoaderCache.put(database, this.getClass().getClassLoader());
  }

  /**
   * Add a resource to the specified database. Update class loader of the database if required.
   * @param database database name
   * @param resources resources which need to be added to the database
   * @param useUri if set to true, use URI from resourceEntry to load in classLoader and update class loader of the
   *               database in the class loader cache
   * @return class loader updated as a result of adding any JARs
   */
  private synchronized ClassLoader loadDBJars(String database, Collection<LensSessionImpl.ResourceEntry> resources,
                                             boolean useUri) {
    ClassLoader classLoader = classLoaderCache.get(database);
    if (classLoader == null) {
      // No change since there are no static resources to be added
      return null;
    }

    if (resources == null || resources.isEmpty()) {
      // Return DB class loader directly since no resources have to be merged.
      return classLoader;
    }

    // Get URLs of the class loader
    if (classLoader instanceof URLClassLoader) {
      URLClassLoader urlLoader = (URLClassLoader) classLoader;
      URL[] preUrls = urlLoader.getURLs();

      // Add to set to remove duplicate additions
      Set<URL> newUrls = new LinkedHashSet<>();
      // New class loader = URLs of DB jars + argument jars
      Collections.addAll(newUrls, preUrls);

      for (LensSessionImpl.ResourceEntry res : resources) {
        try {
          if (useUri) {
            newUrls.add(new URL(res.getUri()));
          } else {
            newUrls.add(new URL(res.getLocation()));
          }
        } catch (MalformedURLException e) {
          incrCounter(LOAD_RESOURCES_ERRORS);
          log.error("Invalid URL {} with location: {} adding to db {}", res.getUri(), res.getLocation(), database, e);
        }
      }

      URLClassLoader newClassLoader = new URLClassLoader(newUrls.toArray(new URL[newUrls.size()]),
        DatabaseResourceService.class.getClassLoader());
      if (useUri) {
        classLoaderCache.put(database, newClassLoader);
      }

      return newClassLoader;
    } else {
      log.warn("Only URL class loader supported");
      return Thread.currentThread().getContextClassLoader();
    }
  }

  /**
   * Add a resource to the specified database, return class loader with resources added.
   * This call does not update the class loader cache
   * @param database database name
   * @param resources resources which need to be added to the database
   * @return class loader updated as a result of adding any JARs
   */
  protected ClassLoader loadDBJars(String database, Collection<LensSessionImpl.ResourceEntry> resources) {
    return loadDBJars(database, resources, false);
  }


  /**
   * Get class loader of a database added with database specific jars
   * @param database database
   * @return class loader from cache of classloaders for each db
   * @throws LensException
   */
  protected ClassLoader getClassLoader(String database) throws LensException {
    return classLoaderCache.get(database);
  }

  /**
   * Get resources added statically to the database
   * @param database db
   * @return resources added to the database, or null if no resources are noted for this database
   */
  public Collection<LensSessionImpl.ResourceEntry> getResourcesForDatabase(String database) {
    return dbResEntryMap.get(database);
  }

  private MetricsService getMetrics() {
    if (metricsService == null) {
      metricsService = LensServices.get().getService(MetricsService.NAME);
      if (metricsService == null) {
        throw new NullPointerException("Could not get metrics service");
      }
    }
    return metricsService;
  }
}
