package con.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;

public class HiveDriver implements GrillDriver {

  private List<String> storages = new ArrayList<String>();
  HiveConf hiveConf;

  public HiveDriver() {
  }

  @Override
  public QueryPlan explain(String query, Configuration conf) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GrillResultSet execute(String query, Configuration conf) throws GrillException {
    Driver qlDriver = new Driver(hiveConf);
    CommandProcessorResponse response;
    try {
      response = qlDriver.run(query);
    } catch (CommandNeedRetryException e) {
      e.printStackTrace();
      throw new GrillException("Error executing query", e);
    }
    return new HiveResultSet(null, response);
  }

  public void configure(Configuration conf) throws GrillException {
    this.hiveConf = new HiveConf(conf, SessionState.class);
  }

  @Override
  public List<String> getSupportedStorages() {
    return storages;
  }

  @Override
  public QueryHandle executeAsync(String breezeQueryContext, Configuration conf) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GrillResultSet fetchResultSet(QueryHandle handle) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) {
    // TODO Auto-generated method stub
    return false;
  }

}
