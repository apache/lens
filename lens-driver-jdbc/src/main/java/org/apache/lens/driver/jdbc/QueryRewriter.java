package org.apache.lens.driver.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensException;

/**
 * The Interface QueryRewriter.
 */
public interface QueryRewriter {

  /**
   * Rewrite.
   *
   * @param conf
   *          the conf
   * @param query
   *          the query
   * @return the string
   * @throws LensException
   *           the lens exception
   */
  public String rewrite(Configuration conf, String query) throws LensException;
}
