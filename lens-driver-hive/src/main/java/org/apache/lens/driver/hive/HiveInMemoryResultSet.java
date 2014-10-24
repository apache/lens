package org.apache.lens.driver.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.InMemoryResultSet;

/**
 * The Class HiveInMemoryResultSet.
 */
public class HiveInMemoryResultSet extends InMemoryResultSet {

  /** The client. */
  private final CLIServiceClient client;

  /** The op handle. */
  private final OperationHandle opHandle;

  /** The metadata. */
  private TableSchema metadata;

  /** The row set. */
  private RowSet rowSet;

  /** The fetch size. */
  private int fetchSize = 100;

  /** The fetched rows itr. */
  private Iterator<Object[]> fetchedRowsItr;

  /** The no more results. */
  private boolean noMoreResults;

  /** The close after fecth. */
  private boolean closeAfterFecth;

  /** The num columns. */
  int numColumns;

  /**
   * Instantiates a new hive in memory result set.
   *
   * @param hiveHandle
   *          the hive handle
   * @param client
   *          the client
   * @param closeAfterFecth
   *          the close after fecth
   * @throws HiveSQLException
   *           the hive sql exception
   */
  public HiveInMemoryResultSet(OperationHandle hiveHandle, CLIServiceClient client, boolean closeAfterFecth)
      throws HiveSQLException {
    this.client = client;
    this.opHandle = hiveHandle;
    this.closeAfterFecth = closeAfterFecth;
    this.metadata = client.getResultSetMetadata(opHandle);
    this.numColumns = metadata.getColumnDescriptors().size();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensResultSet#size()
   */
  @Override
  public int size() throws LensException {
    return -1;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    // Removed Anonymous inner class and changed it to concrete class
    // for serialization to JSON
    HiveResultSetMetadata hrsMeta = new HiveResultSetMetadata();
    hrsMeta.setColumns(metadata.getColumnDescriptors());
    return hrsMeta;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.InMemoryResultSet#hasNext()
   */
  @Override
  public boolean hasNext() throws LensException {
    if (fetchedRowsItr == null || !fetchedRowsItr.hasNext()) {
      try {
        rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, fetchSize);
        noMoreResults = rowSet.numRows() == 0;
        if (noMoreResults) {
          if (closeAfterFecth) {
            HiveDriver.LOG.info("No more results closing the query");
            client.closeOperation(opHandle);
          }
          return false;
        }
        fetchedRowsItr = rowSet.iterator();
      } catch (Exception e) {
        throw new LensException(e);
      }
    }
    return fetchedRowsItr.hasNext();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.InMemoryResultSet#next()
   */
  @Override
  public ResultRow next() throws LensException {
    List<Object> results = new ArrayList<Object>(numColumns);
    Object[] row = fetchedRowsItr.next();
    results.addAll(Arrays.asList(row));

    return new ResultRow(results);
  }

  @Override
  public void setFetchSize(int size) throws LensException {
    assert size >= 0;
    fetchSize = size == 0 ? Integer.MAX_VALUE : size;
  }
}
