package com.inmobi.grill.es.cube.join;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;


public class DimJoinerFactory {
  public static final ESLogger LOG = Loggers.getLogger(DimJoinerFactory.class);
  
  private static volatile boolean factorySetup = false;
  public static final String JOIN_CONFIG_FILE = "join_config.yaml";
  public static final String DIM_LOADER_PFX = "cube.dimloader.indexDir.";
  
  public static boolean isSetup() {
    return factorySetup;
  }
  
  private static Map<String, IndexReader> indexReaders;
  private static Map<String, Directory> luceneDirectories;
  private static Map<String, DimLoader> loaderCache;
  
  public synchronized static void setup() throws IOException {
    if (factorySetup) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting up joiner factory");
    }
    // Setup loaders as well as joiners for the main table.
    Settings settings = ImmutableSettings.settingsBuilder().loadFromClasspath(JOIN_CONFIG_FILE).build();
    /*
     * cube.dimloader.indexDir.<tablename> = /path/to/lucene/index/of/dimtable 
     */
    Map<String, String> settingsMap = settings.getAsMap();
    luceneDirectories = new HashMap<String, Directory>();
    
    for (String key : settingsMap.keySet()) {
      LOG.info("Join config for {} INDEX_DIR = {}" , key, settingsMap.get(key));
      if (key.startsWith(DIM_LOADER_PFX)) {
        String table = key.replace(DIM_LOADER_PFX, "");
        String path = settingsMap.get(key);
        if (path != null) {
          File indexDirFile = new File(path);
          if (!indexDirFile.exists() && !indexDirFile.isDirectory()) {
            throw new IOException("Index dir for table " +  table + " does not exist or is not a directory");
          }
          Directory dir = new MMapDirectory(indexDirFile);
          luceneDirectories.put(table, dir);
          LOG.info("Loaded lucene directory for table {}", table);
        }
      }
    }
    
    indexReaders = new HashMap<String, IndexReader>();
    loaderCache = new HashMap<String, DimLoader>();
    // Avoid re-opening dirs
    factorySetup = true;
  }
  
  public synchronized static DimJoiner getJoinerForConfig(JoinConfig config) throws IOException {
    // Parse create the condition object
    JoinCondition condition = JoinConditionFactory.fromString(config.joinConditionStr);
    IndexReader reader = null;

    // Get index reader
    reader = indexReaders.get(config.table);
    if (reader == null) {
      reader = IndexReader.open(luceneDirectories.get(config.table));
      indexReaders.put(config.table, reader);
      LOG.info("Opened index reader for table {}", config.table);
    }

    // Create the lookup object for this field.
    DimLoader loader = loaderCache.get(config.table + config.joinedColumn);
    if (loader == null) {
      TermDocsLoader tloader = new TermDocsLoader(config.joinedColumn);
      tloader.setIndexReader(reader);
      loaderCache.put(config.table + config.joinedColumn, tloader);
      loader = tloader;
    }

    DimJoiner joiner = new DimJoiner(config, condition, loader);
    return joiner;
  }
}
