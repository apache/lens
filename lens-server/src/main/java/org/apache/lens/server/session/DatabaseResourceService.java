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

import java.io.File;
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
  private List<LensSessionImpl.ResourceEntry> commonResMap;

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

  private String resTopDir = null;

  @Override
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    classLoaderCache = new HashMap<>();
    dbResEntryMap = new HashMap<>();
    commonResMap = new LinkedList<>();

    resTopDir =
      hiveConf.get(LensConfConstants.DATABASE_RESOURCE_DIR, LensConfConstants.DEFAULT_DATABASE_RESOURCE_DIR);
  }

  @Override
  public synchronized void start() {
    super.start();

    try {
      log.info("Starting loading DB specific resources");

      // If the base folder itself is not present, then return as we can't load any jars.
      FileSystem serverFs = null;
      try {
        Path resTopDirPath = new Path(resTopDir);
        serverFs = FileSystem.newInstance(resTopDirPath.toUri(), getHiveConf());
        if (!serverFs.exists(resTopDirPath)) {
          incrCounter(LOAD_RESOURCES_ERRORS);
          log.warn("DB base location does not exist.", resTopDir);
          return;
        }
      } catch (Exception io) {
        log.error("Error locating base directory", io);
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


      // Map common jars available for all DB folders
      mapCommonResourceEntries();

      // Map DB specific jar
      mapDbResourceEntries();

      // Load all the jars for all the DB's mapped in previous steps
      loadMappedResources();


    } catch (LensException e) {
      incrCounter(LOAD_RESOURCES_ERRORS);
      log.error("Failed to load DB resource mapping, resources must be added explicitly to session.", e);
    }
  }


  @Override
  public synchronized void stop() {
    super.stop();
    classLoaderCache.clear();
    dbResEntryMap.clear();
    commonResMap.clear();
  }

  private void mapCommonResourceEntries() {
    // Check if order file is present in the directory
    List<String> jars = new ScannedPaths(new Path(resTopDir), "jar", false).getFinalPaths();
    for (String resPath : jars) {
      Path jarFilePath = new Path(resPath);
      commonResMap.add(new LensSessionImpl.ResourceEntry("jar", jarFilePath.toUri().toString()));
    }

  }

  private void mapDbResourceEntries(String dbName) throws LensException {
    // Read list of databases in
    FileSystem serverFs = null;

    try {

      Path resDirPath = new Path(resTopDir, dbName);
      serverFs = FileSystem.newInstance(resDirPath.toUri(), getHiveConf());
      resDirPath = serverFs.resolvePath(resDirPath);
      if (!serverFs.exists(resDirPath)) {
        incrCounter(LOAD_RESOURCES_ERRORS);
        log.warn("Database resource location does not exist - {}. Database jars will not be available", resDirPath);

        // remove the db entry
        dbResEntryMap.remove(dbName);
        classLoaderCache.remove(dbName);
        return;
      }

      findResourcesInDir(serverFs, dbName, resDirPath);
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

  private void mapDbResourceEntries() throws LensException {
    // Read list of databases in
    FileSystem serverFs = null;

    try {
      String baseDir =
        getHiveConf().get(LensConfConstants.DATABASE_RESOURCE_DIR, LensConfConstants.DEFAULT_DATABASE_RESOURCE_DIR);
      log.info("Database specific resources at {}", baseDir);

      Path resTopDirPath = new Path(baseDir);
      serverFs = FileSystem.newInstance(resTopDirPath.toUri(), getHiveConf());
      resTopDirPath = serverFs.resolvePath(resTopDirPath);
      if (!serverFs.exists(resTopDirPath)) {
        incrCounter(LOAD_RESOURCES_ERRORS);
        log.warn("DB base location does not exist.", baseDir);
        return;
      }

      // Look for db dirs
      for (FileStatus dbDir : serverFs.listStatus(resTopDirPath)) {
        Path dbDirPath = dbDir.getPath();
        if (serverFs.isDirectory(dbDirPath)) {
          String dbName = dbDirPath.getName();

          Path dbJarOrderPath = new Path(baseDir, dbName + File.separator + "jar_order");
          if (serverFs.exists(dbJarOrderPath)) {
            // old flow
            mapDbResourceEntries(dbName);
          } else {
            // new flow
            mapDbSpecificJar(resTopDirPath, dbName);
          }
        }
      }

      log.debug("Found resources {}", dbResEntryMap);
    } catch (IOException io) {
      log.error("Error getting list of dbs to load resources from", io);
      throw new LensException(io);
    } catch (Exception e) {
      log.error("Exception : ", e);
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

  private void mapDbSpecificJar(Path baseDir, String dbName) throws Exception {
    FileSystem serverFs = null;
    try {
      int lastIndex = 0;


      Path dbFolderPath = new Path(baseDir, dbName);

      serverFs = FileSystem.newInstance(dbFolderPath.toUri(), getHiveConf());
      FileStatus[] existingFiles = serverFs.listStatus(dbFolderPath);
      for (FileStatus fs : existingFiles) {
        String fPath = fs.getPath().getName();
        String[] tokens = fPath.split("_");

        if (tokens.length > 1) {
          int fIndex = Integer.parseInt(tokens[tokens.length - 1].substring(0, 1));
          if (fIndex > lastIndex) {
            lastIndex = fIndex;
          }
        }
      }

      if (lastIndex > 0) {
        // latest jar
        Path latestJarPath = new Path(baseDir, dbName + File.separator + dbName + "_" + lastIndex + ".jar");
        addResourceEntry(new LensSessionImpl.ResourceEntry("jar", latestJarPath.toUri().toString()), dbName);
      }


      // add common jars
      for (LensSessionImpl.ResourceEntry jar : commonResMap) {
        addResourceEntry(jar, dbName);
      }

    } catch (Exception io) {
      log.error("Error getting db specific resource ", io);
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
    log.info("Adding resource entry {} for {}", entry.getLocation(), dbName);
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
  public void loadMappedResources() {
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
   * @param addToCache if set to true, update class loader of the database in the class loader cache
   * @return class loader updated as a result of adding any JARs
   */
  protected synchronized ClassLoader loadDBJars(String database, Collection<LensSessionImpl.ResourceEntry> resources,
    boolean addToCache) {
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
          newUrls.add(new URL(res.getLocation()));
        } catch (MalformedURLException e) {
          incrCounter(LOAD_RESOURCES_ERRORS);
          log.error("Invalid URL {} adding to db {}", res.getLocation(), database, e);
        }
      }

      URLClassLoader newClassLoader = new URLClassLoader(newUrls.toArray(new URL[newUrls.size()]),
        DatabaseResourceService.class.getClassLoader());
      if (addToCache) {
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
