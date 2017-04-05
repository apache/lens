package org.apache.lens.cube.parse;

import org.apache.lens.server.api.error.LensException;

/**
 * Created on 03/04/17.
 */
public interface QueryWriterContext {
  void addAutoJoinDims() throws LensException;
  void addExpressionDims() throws LensException;
  void addDenormDims() throws LensException;
  void updateDimFilterWithFactFilter() throws LensException;
  QueryAST getQueryAst();
  void updateFromString() throws LensException;
  QueryWriter toQueryWriter() throws LensException;
}
