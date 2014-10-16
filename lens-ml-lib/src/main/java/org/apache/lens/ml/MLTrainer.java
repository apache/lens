package org.apache.lens.ml;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;


public interface MLTrainer {
  public String getName();
  public String getDescription();
  public void configure(LensConf configuration);
  public LensConf getConf();
  public MLModel train(LensConf conf, String db, String table, String modelId, String ... params) throws LensException;
}
