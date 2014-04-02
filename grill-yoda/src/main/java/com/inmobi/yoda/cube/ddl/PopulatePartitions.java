package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.StoragePartitionDesc;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.BasicConfigurator;

/**
 * This class populates the partitions for a given range for a cube.
 * 
 * This is mainly used for dev and qa testing. In production, partitions will be
 * added as soon as there are available.
 *
 */
public class PopulatePartitions {
  public static final Log LOG = LogFactory.getLog(PopulatePartitions.class);
  private final HiveConf conf;
  private PathFilter filter;

  public PopulatePartitions(HiveConf conf) {
    this.conf = conf;
    filter = createPathFilter();
  }

  public void populateAllDimParts(Path basePath, DateFormat pathDateFormat,
      Date partitionTimestamp, boolean checkExist)
          throws HiveException, IOException {
    final CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);
    List<CubeDimensionTable> dims = client.getAllDimensionTables();

    // basePath = /user/yoda/warehouse/metadata/
    for (CubeDimensionTable dim : dims) {
      Path partPath = new Path(new Path(basePath, dim.getName()),
          "rq_dt=" + pathDateFormat.format(partitionTimestamp));
      if (checkExist) {
        FileSystem fs = partPath.getFileSystem(conf);
        if (!fs.exists(partPath)) {
          System.out.println("Path" + partPath +" does not exist");          
          continue;
        }

      }
      System.out.println("Adding partition at Path" + partPath);
      Map<String, Date> timeParts = new HashMap<String, Date>();
      timeParts.put(DimensionDDL.dim_time_part_column, partitionTimestamp);
      StoragePartitionDesc partSpec = new StoragePartitionDesc(dim.getName(),
          timeParts, null, UpdatePeriod.HOURLY);
      partSpec.setLocation(partPath.toString());
      try {
        client.addPartition(partSpec, CubeDDL.YODA_STORAGE);
      } catch (HiveException exc) {
        LOG.error("Error adding dim partition for : " + dim.getName(), exc);
        System.out.println("Failed to add partition for" + partPath);
      }
    }
  }

  public void populateCubeParts(String cubeName, Date start, Date end,
      UpdatePeriod updatePeriod, Path basePath, DateFormat dateFormat,
      String summaries, boolean checkExist)
          throws HiveException, IOException {
    final CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);

    Cube cube = client.getCube(CubeDDL.CUBE_NAME_PFX + cubeName);
    FileSystem fs = basePath.getFileSystem(conf);

    boolean pieStorage = false;
    String sumPath = cube.getProperties().get(
        "cube." + cubeName + ".summaries.path");
    Path piePath = null;
    if (cube.getProperties().get("cube." + cubeName + ".summaries.pie.path") != null) {
      piePath = new Path(cube.getProperties().get("cube." + cubeName + ".summaries.pie.path"));
    }
    if (new Path(basePath.toUri().getPath()).equals(piePath)) {
      System.out.println("Its a PIE storage!");
      pieStorage = true;
    }

    boolean populateOnlyRaw = false;
    boolean populateAll = false;
    List<String> summaryList = Arrays.asList(StringUtils.split(
        summaries.toLowerCase(), ","));
    if (summaryList.size() == 1) {
      if (summaryList.get(0).equalsIgnoreCase(CubeDDL.RAW_FACT_NAME)) {
        populateOnlyRaw = true;
      }
      if (summaryList.get(0).equalsIgnoreCase("ALL")) {
        populateAll = true;
      }
    }
    // cube.request.path=rrcube/transformationoutput2
    //cube.request.colo.path=/user/yoda/warehouse/localdc/rrcube/transformationoutput2
    Path rawFactPath = null;
    if (!pieStorage) {
      rawFactPath = new Path(basePath, cube.getProperties().get(
          "cube." + cubeName + ".path"));
    } else {
      rawFactPath = new Path(cube.getProperties().get(
          "cube." + cubeName + ".colo.path"));
    }
    //cube.request.summaries.path=rrcube
    Path summariesPath = null;
    if (!populateOnlyRaw) {
      if (sumPath != null) {
        summariesPath = new Path(basePath, cube.getProperties().get(
            "cube." + cubeName + ".summaries.path"));
      } else {
        System.out.println("summaries.path not available, populating only raw");
        populateOnlyRaw = true;
      }
    }

    List<CubeFactTable> facts = client.getAllFactTables(cube);
    System.out.println("All facts:" + facts);
    Calendar cal = Calendar.getInstance();
    cal.setTime(start);
    Date dt = cal.getTime();
    while (!dt.after(end)) {
      // for each fact add the partition for dt
      for (CubeFactTable fact : facts) {
        if (!populateAll && !summaryList.contains(fact.getName())) {
          continue; 
        }
        if (populateOnlyRaw && (!fact.getName().endsWith(
            CubeDDL.RAW_FACT_NAME))) {
          continue;
        }
        for (Map.Entry<String, Set<UpdatePeriod>> entry : 
          fact.getUpdatePeriods().entrySet()) {
          if (!entry.getValue().contains(updatePeriod)) {
            continue;
          }

          Path partPath;
          if (!pieStorage) {
            if (entry.getKey().equalsIgnoreCase(CubeDDL.YODA_STORAGE)) {
              if (fact.getName().endsWith(CubeDDL.RAW_FACT_NAME)) {
                partPath = new Path(rawFactPath, dateFormat.format(dt));
              } else {
                partPath = new Path(new Path(new Path(summariesPath, fact.getName()),
                    updatePeriod.name().toLowerCase()), dateFormat.format(dt));
              }
              if (checkExist && !fs.exists(partPath)) {
                System.out.println("Path" + partPath +" does not exist");
                continue;
              }
              Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
              partitionTimestamps.put(CubeDDL.PART_KEY_IT, dt);
              System.out.println("Adding partitions for Path" + partPath);
              StoragePartitionDesc partSpec = new StoragePartitionDesc(
                  fact.getName(), partitionTimestamps, null, updatePeriod);
              partSpec.setLocation(partPath.toString());
              try {
                client.addPartition(partSpec, entry.getKey());
              } catch (HiveException exc) {
                LOG.error("Error adding cube partition", exc);
                System.out.println("Failed to add partition for" + partPath);
              }
            }
          } else {
            if (entry.getKey().equalsIgnoreCase(CubeDDL.YODA_PIE_STORAGE)) {
              if (fact.getName().endsWith(CubeDDL.RAW_FACT_NAME)) {
                partPath = new Path(rawFactPath, dateFormat.format(dt));
                Date it = dt;
                FileStatus[] cStats = fs.listStatus(partPath, filter);
                for (FileStatus cstat : cStats) {
                  String colo = cstat.getPath().getName();
                  Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
                  Map<String, String> nonTimepartSpec = new HashMap<String, String>();
                  partitionTimestamps.put(CubeDDL.PART_KEY_IT, it);
                  nonTimepartSpec.put(CubeDDL.PART_KEY_COLO, colo); 
                  System.out.println("Adding partitions for Path" + cstat.getPath());
                  StoragePartitionDesc partSpec = new StoragePartitionDesc(
                      fact.getName(), partitionTimestamps, nonTimepartSpec, updatePeriod);
                  partSpec.setLocation(cstat.getPath().toString());
                  try {
                    client.addPartition(partSpec, entry.getKey());
                  } catch (HiveException exc) {
                    LOG.error("Error adding cube partition", exc);
                    System.out.println("Failed to add partition for" + cstat.getPath());
                  }
                }
              } else {
                partPath = new Path(new Path(new Path(summariesPath, fact.getName()),
                    updatePeriod.name().toLowerCase()), dateFormat.format(dt));
                if (checkExist && !fs.exists(partPath)) {
                  System.out.println("Path" + partPath +" does not exist");
                  continue;
                }
                Date pt = dt;
                FileStatus[] iStats = fs.listStatus(partPath, filter);
                for (FileStatus istat : iStats) {
                  Date it = null;
                  try {
                    it = dateFormat.parse(istat.getPath().getName());
                  } catch (ParseException e) {
                    // ignore
                  }
                  FileStatus[] eStats = fs.listStatus(istat.getPath(), filter);
                  for (FileStatus estat : eStats) {
                    Date et = null;
                    try {
                      et = dateFormat.parse(estat.getPath().getName());
                    } catch (ParseException e) {
                      //ignore
                    }
                    Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
                    Map<String, String> nonTimePartSpec = new HashMap<String, String>();
                    partitionTimestamps.put(CubeDDL.PART_KEY_PT, pt);
                    if (it != null) {
                      partitionTimestamps.put(CubeDDL.PART_KEY_IT, it);
                    } else {
                      nonTimePartSpec.put(CubeDDL.PART_KEY_IT, istat.getPath().getName()); 
                    }

                    if (et != null) {
                      partitionTimestamps.put(CubeDDL.PART_KEY_ET, et);
                    } else {
                      nonTimePartSpec.put(CubeDDL.PART_KEY_ET, estat.getPath().getName()); 
                    }
                    System.out.println("Adding partitions for Path" + estat.getPath());
                    StoragePartitionDesc partSpec = new StoragePartitionDesc(
                        fact.getName(), partitionTimestamps, nonTimePartSpec, updatePeriod);
                    partSpec.setLocation(estat.getPath().toString());
                    try {
                      client.addPartition(partSpec, entry.getKey());
                    } catch (HiveException exc) {
                      LOG.error("Error adding cube partition", exc);
                      System.out.println("Failed to add partition for" + estat.getPath());
                    }
                  }
                }
              }
            }
          }
        }
      }
      cal.add(updatePeriod.calendarField(), 1);
      dt = cal.getTime();
    }    
  }

  private PathFilter createPathFilter() {
    return new PathFilter() {
      @Override
      public boolean accept(Path p) {
        if (p.getName().startsWith("_")) {
          return false;
        }
        return true;
      }
    };
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length < 4) {
      System.out.println("Usage:" +
          "\t [ [-db dbName] -dims basepath timestamp pathDateFormat]\n" +
          "\t [ [-db dbName] cubeName startPartition endPartition" +
          " UpdatePeriod basePath pathDateFormat (summarylist|raw|all) ]");
      return;
    }
    BasicConfigurator.configure();
    HiveConf conf = new HiveConf(PopulatePartitions.class);
    SessionState.start(conf);
    PopulatePartitions pp = new PopulatePartitions(conf);
    int startIndex = 0;
    if (args.length > 0) {
      if (args[0].equals("-db")) {
        String dbName = args[1];
        Database database = new Database();
        database.setName(dbName);
        Hive.get().createDatabase(database, true);
        SessionState.get().setCurrentDatabase(dbName);
        startIndex = 2;
      }
    }
    if (args[startIndex].equalsIgnoreCase("-dims")) {
      String baseDimPath = args[startIndex + 1];
      String dimTS = args[startIndex + 2];
      String pathDateFormat = args[startIndex + 3];
      SimpleDateFormat dateFormat = new SimpleDateFormat(pathDateFormat);
      pp.populateAllDimParts(new Path(baseDimPath), dateFormat,
          dateFormat.parse(dimTS), true);
    } else {
      String cubeName = args[startIndex];
      String startPos = args[startIndex + 1];
      String endPos = args[startIndex + 2];
      String updatePeriod = args[startIndex + 3];
      String basePath = args[startIndex + 4];
      String pathDateFormat = args[startIndex + 5];
      String summaries = args[startIndex + 6];

      UpdatePeriod p = UpdatePeriod.valueOf(updatePeriod.toUpperCase());
      SimpleDateFormat dateFormat = new SimpleDateFormat(pathDateFormat);
      Date start = dateFormat.parse(startPos);
      Date end = dateFormat.parse(endPos);

      pp.populateCubeParts(cubeName, start, end, p, new Path(basePath),
          dateFormat, summaries, true);
    }
  }
}
