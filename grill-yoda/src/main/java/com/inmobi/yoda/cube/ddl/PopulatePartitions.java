package com.inmobi.yoda.cube.ddl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.columnar.LazyNOBColumnarSerde;

public class PopulatePartitions {

  private final String cubeName;
  private final Date start;
  private final Date end;
  private final UpdatePeriod updatePeriod;
  private final HiveConf conf;
  private final Path basePath;
  private final SimpleDateFormat dateFormat;

  PopulatePartitions(String cubeName, Date startPos, Date endPos,
      UpdatePeriod updatePeriod, HiveConf conf, Path basePath,
      SimpleDateFormat dateFormat) {
    this.cubeName = cubeName;
    this.start = startPos;
    this.end = endPos;
    this.updatePeriod = updatePeriod;
    this.conf = conf;
    this.basePath = basePath;
    this.dateFormat = dateFormat;
  }

  public void run() throws HiveException {
    final CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);

    Cube cube = client.getCube(CubeDDL.CUBE_NAME_PFX + cubeName);
    List<CubeFactTable> facts = client.getAllFactTables(cube);
    Calendar cal = Calendar.getInstance();
    cal.setTime(start);
    Date dt = cal.getTime();
    while (!dt.after(end)) {
      // for each fact add the partition for dt
      for (CubeFactTable fact : facts) {
        for (Map.Entry<String, List<UpdatePeriod>> entry : 
            fact.getUpdatePeriods().entrySet()) {
          if (!entry.getValue().contains(updatePeriod)) {
            continue;
          }
          String factPathName = fact.getName().substring(cubeName.length() + 1);
          Path partPath = new Path(new Path(new Path(basePath, factPathName),
              updatePeriod.name().toLowerCase()), dateFormat.format(dt));
          System.out.println("Adding partition at Path" + partPath);
          HDFSStorage storage = new HDFSStorage(entry.getKey(),
              RCFileInputFormat.class.getCanonicalName(),
              RCFileOutputFormat.class.getCanonicalName(),
              LazyNOBColumnarSerde.class.getCanonicalName(), true, null, null,
              null);
          storage.setPartLocation(partPath);
          Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
          partitionTimestamps.put(Storage.getDatePartitionKey(), dt);
          client.addPartition(fact, storage, updatePeriod, partitionTimestamps);
        }
      }
        cal.add(updatePeriod.calendarField(), 1);
        dt = cal.getTime();
    }    
  }

  public static void main(String[] args) throws HiveException, ParseException {
    if (args.length < 4) {
      System.out.println("Usage: cubeName startPartition endPartition" +
          " UpdatePeriod basePath pathDateFormat");
      return;
    }
    String cubeName = args[0];
    String startPos = args[1];
    String endPos = args[2];
    String updatePeriod = args[3];
    String basePath = args[4];
    String pathDateFormat = args[5];
    HiveConf conf = new HiveConf(PopulatePartitions.class);

    UpdatePeriod p = UpdatePeriod.valueOf(updatePeriod.toUpperCase());
    SimpleDateFormat dateFormat = new SimpleDateFormat(pathDateFormat);
    Date start = dateFormat.parse(startPos);
    Date end = dateFormat.parse(endPos);

    PopulatePartitions pp = new PopulatePartitions(cubeName, start, end, p,
        conf, new Path(basePath), dateFormat);
    pp.run();
  }
}
