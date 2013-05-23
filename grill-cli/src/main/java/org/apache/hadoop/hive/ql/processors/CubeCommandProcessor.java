package org.apache.hadoop.hive.ql.processors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;

import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.exception.GrillException;

public class CubeCommandProcessor implements CommandProcessor {

  public static final Log LOG = LogFactory.getLog(CubeCommandProcessor.class.getName());
  public static final LogHelper console = new LogHelper(LOG);

  CubeGrillDriver engine;
  public CubeCommandProcessor(Configuration conf) {
    try {
      engine = new CubeGrillDriver(conf);
    } catch (GrillException e) {
      throw new RuntimeException(e);
    }
  }

  public void init() {  
  }

  public CommandProcessorResponse run(String command)
      throws CommandNeedRetryException {
    int ret = 0;
    try {
      engine.execute(command, null);
    } catch (GrillException e) {
      console.printError("Command failed with exception : " + StringUtils
          .stringifyException(e));
      return new CommandProcessorResponse(1, e.getMessage(), null);
    }
    return new CommandProcessorResponse(ret);
  }

}
