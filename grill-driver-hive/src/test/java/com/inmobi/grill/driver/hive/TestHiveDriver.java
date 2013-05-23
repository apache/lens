package com.inmobi.grill.driver.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.annotations.Test;

import com.inmobi.grill.exception.GrillException;

import con.inmobi.grill.driver.hive.HiveDriver;

public class TestHiveDriver {

  //@Test
  public void testHiveDriver() throws GrillException {
    Configuration conf = new Configuration();
    HiveConf hiveConf = new HiveConf(conf, SessionState.class);
    CliSessionState ss = new CliSessionState(hiveConf);
    assert ss != null;

    SessionState.start(ss);
    HiveDriver driver = new HiveDriver();
    driver.configure(new Configuration());
    driver.execute("drop table table_name", null);
    driver.execute("create table table_name (id int, dtDontQuery string, " +
    		"name string) partitioned by (dt string)", null);
  }
}
