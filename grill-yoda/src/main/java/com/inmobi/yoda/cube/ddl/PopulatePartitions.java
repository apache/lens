package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.columnar.LazyNOBColumnarSerde;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * This class populates the partitions for a given range for a cube.
 * 
 * This is mainly used for dev and qa testing. In production, partitions will be
 * added as soon as there are available.
 *
 */
public class PopulatePartitions {

  private final HiveConf conf;
  private PathFilter filter;

  public PopulatePartitions(HiveConf conf) {
    this.conf = conf;
    filter = createPathFilter();
  }

  public void populateAllDimParts(Path basePath, SimpleDateFormat pathDateFormat,
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
      HDFSStorage storage = new HDFSStorage(CubeDDL.YODA_STORAGE,
          TextInputFormat.class.getCanonicalName(),
          HiveIgnoreKeyTextOutputFormat.class.getCanonicalName(),
          null, true, null, null, null);
      storage.setPartLocation(partPath);
      client.addPartition(dim, storage, partitionTimestamp);
    }
  }

  public void populateCubeParts(String cubeName, Date start, Date end,
      UpdatePeriod updatePeriod, Path basePath, SimpleDateFormat dateFormat,
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
              HDFSStorage storage = new HDFSStorage(entry.getKey(),
                  RCFileInputFormat.class.getCanonicalName(),
                  RCFileOutputFormat.class.getCanonicalName(),
                  LazyNOBColumnarSerde.class.getCanonicalName(), true, null, null,
                  null);
              storage.setPartLocation(partPath);
              Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
              partitionTimestamps.put(CubeDDL.PART_KEY_IT, dt);
              System.out.println("Adding partitions for Path" + partPath);
              client.addPartition(fact, storage, updatePeriod,
                  partitionTimestamps, CubeDDL.PART_KEY_IT);
            }
          } else {
            if (entry.getKey().equalsIgnoreCase(CubeDDL.YODA_PIE_STORAGE)) {
              if (fact.getName().endsWith(CubeDDL.RAW_FACT_NAME)) {
                partPath = new Path(rawFactPath, dateFormat.format(dt));
                Date it = dt;
                FileStatus[] cStats = fs.listStatus(partPath, filter);
                for (FileStatus cstat : cStats) {
                  String colo = cstat.getPath().getName();
                  HDFSStorage storage = new HDFSStorage(entry.getKey(),
                      RCFileInputFormat.class.getCanonicalName(),
                      RCFileOutputFormat.class.getCanonicalName(),
                      LazyNOBColumnarSerde.class.getCanonicalName(), true, null, null,
                      null);
                  storage.setPartLocation(cstat.getPath());
                  Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
                  Map<String, String> partSpec = new HashMap<String, String>();
                  partitionTimestamps.put(CubeDDL.PART_KEY_IT, it);
                  partSpec.put(CubeDDL.PART_KEY_COLO, colo); 
                  System.out.println("Adding partitions for Path" + cstat.getPath());
                  client.addPartition(fact, storage, updatePeriod,
                      partitionTimestamps, partSpec, CubeDDL.PART_KEY_IT);
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
                    HDFSStorage storage = new HDFSStorage(entry.getKey(),
                        RCFileInputFormat.class.getCanonicalName(),
                        RCFileOutputFormat.class.getCanonicalName(),
                        LazyNOBColumnarSerde.class.getCanonicalName(), true, null, null,
                        null);
                    storage.setPartLocation(estat.getPath());
                    Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
                    Map<String, String> partSpec = new HashMap<String, String>();
                    partitionTimestamps.put(CubeDDL.PART_KEY_PT, pt);
                    if (it != null) {
                      partitionTimestamps.put(CubeDDL.PART_KEY_IT, it);
                    } else {
                      partSpec.put(CubeDDL.PART_KEY_IT, istat.getPath().getName()); 
                    }

                    if (et != null) {
                      partitionTimestamps.put(CubeDDL.PART_KEY_ET, et);
                    } else {
                      partSpec.put(CubeDDL.PART_KEY_ET, estat.getPath().getName()); 
                    }
                    System.out.println("Adding partitions for Path" + estat.getPath());
                    client.addPartition(fact, storage, updatePeriod,
                        partitionTimestamps, partSpec, CubeDDL.PART_KEY_PT);
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
      throws HiveException, ParseException, IOException {
    if (args.length < 4) {
      System.out.println("Usage:" +
          "\t [-dims basepath timestamp pathDateFormat]\n" +
          "\t [cubeName startPartition endPartition" +
          " UpdatePeriod basePath pathDateFormat (summarylist|raw|all) ]");
      return;
    }
    HiveConf conf = new HiveConf(PopulatePartitions.class);
    SessionState.start(conf);
    PopulatePartitions pp = new PopulatePartitions(conf);
    if (args[0].equalsIgnoreCase("-dims")) {
      String baseDimPath = args[1];
      String dimTS = args[2];
      String pathDateFormat = args[3];
      SimpleDateFormat dateFormat = new SimpleDateFormat(pathDateFormat);
      pp.populateAllDimParts(new Path(baseDimPath), dateFormat,
          dateFormat.parse(dimTS), true);
    } else {
      String cubeName = args[0];
      String startPos = args[1];
      String endPos = args[2];
      String updatePeriod = args[3];
      String basePath = args[4];
      String pathDateFormat = args[5];
      String summaries = args[6];

      UpdatePeriod p = UpdatePeriod.valueOf(updatePeriod.toUpperCase());
      SimpleDateFormat dateFormat = new SimpleDateFormat(pathDateFormat);
      Date start = dateFormat.parse(startPos);
      Date end = dateFormat.parse(endPos);

      pp.populateCubeParts(cubeName, start, end, p, new Path(basePath),
          dateFormat, summaries, true);
    }
  }
}
