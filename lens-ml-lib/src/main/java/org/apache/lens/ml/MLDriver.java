package org.apache.lens.ml;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;

import java.util.List;

/**
 * The Interface MLDriver.
 */
public interface MLDriver {

  /**
   * Checks if is trainer supported.
   *
   * @param trainer
   *          the trainer
   * @return true, if is trainer supported
   */
  public boolean isTrainerSupported(String trainer);

  /**
   * Gets the trainer instance.
   *
   * @param trainer
   *          the trainer
   * @return the trainer instance
   * @throws LensException
   *           the lens exception
   */
  public MLTrainer getTrainerInstance(String trainer) throws LensException;

  /**
   * Inits the.
   *
   * @param conf
   *          the conf
   * @throws LensException
   *           the lens exception
   */
  public void init(LensConf conf) throws LensException;

  /**
   * Start.
   *
   * @throws LensException
   *           the lens exception
   */
  public void start() throws LensException;

  /**
   * Stop.
   *
   * @throws LensException
   *           the lens exception
   */
  public void stop() throws LensException;

  public List<String> getTrainerNames();
}
