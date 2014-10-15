package org.apache.lens.server;

import org.apache.hadoop.hive.conf.HiveConf;

public class LensServerConf {

  public static HiveConf conf;

  public static HiveConf get() {
    if(conf == null) {
      synchronized (LensServerConf.class) {
        if (conf == null) {
          conf = new HiveConf();
          conf.addResource("lensserver-default.xml");
          conf.addResource("lens-site.xml");
        }
      }
    }
    return conf;
  }
}
