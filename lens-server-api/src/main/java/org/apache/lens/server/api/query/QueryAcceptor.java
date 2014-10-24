package org.apache.lens.server.api.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.SubmitOp;

/**
 * The Interface QueryAcceptor.
 */
public interface QueryAcceptor {

  /**
   * Whether to accept the query or not.
   *
   * @param query
   *          The query
   * @param conf
   *          The configuration of the query
   * @param submitOp
   *          the submit op
   * @return null if query should be accepted, rejection cause otherwise
   * @throws LensException
   *           the lens exception
   */
  public String accept(String query, Configuration conf, SubmitOp submitOp) throws LensException;

}
