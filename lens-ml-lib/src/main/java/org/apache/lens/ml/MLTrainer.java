package org.apache.lens.ml;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;

/**
 * The Interface MLTrainer.
 */
public interface MLTrainer {
  public String getName();

  public String getDescription();

  /**
   * Configure.
   *
   * @param configuration
   *          the configuration
   */
  public void configure(LensConf configuration);

  public LensConf getConf();

  /**
   * Train.
   *
   * @param conf
   *          the conf
   * @param db
   *          the db
   * @param table
   *          the table
   * @param modelId
   *          the model id
   * @param params
   *          the params
   * @return the ML model
   * @throws LensException
   *           the lens exception
   */
  public MLModel train(LensConf conf, String db, String table, String modelId, String... params) throws LensException;
}
