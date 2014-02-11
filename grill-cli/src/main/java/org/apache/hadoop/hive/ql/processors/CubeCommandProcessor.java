package org.apache.hadoop.hive.ql.processors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.driver.cube.RewriteUtil;

public class CubeCommandProcessor extends Driver {

  public static final Log LOG = LogFactory.getLog(
      CubeCommandProcessor.class.getName());
  public static final LogHelper console = new LogHelper(LOG);

  private CubeGrillDriver engine;
  private Configuration conf;
  public CubeCommandProcessor(HiveConf conf) {
    super(conf);
    init(conf);
  }

  public CubeCommandProcessor() {
    super();
    init(new HiveConf(new Configuration(), CubeCommandProcessor.class));
  }

  public void init(Configuration conf) {
    this.conf = conf;
    try {
      engine = new CubeGrillDriver(conf);
    } catch (GrillException e) {
      throw new RuntimeException(e);
    }
  }

  public CommandProcessorResponse run(String command)
      throws CommandNeedRetryException {
    if (RewriteUtil.isCubeQuery(command)) {
      int ret = 0;
      try {
        engine.execute(command, conf);
      } catch (GrillException e) {
        console.printError("Command failed with exception : " + StringUtils
            .stringifyException(e));
        return new CommandProcessorResponse(1, e.getMessage(), null);
      }
      return new CommandProcessorResponse(ret);
    } else {
      return super.run(command);
    }
  }
}
