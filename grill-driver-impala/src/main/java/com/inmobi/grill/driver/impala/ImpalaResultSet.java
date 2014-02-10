package com.inmobi.grill.driver.impala;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.cloudera.beeswax.api.BeeswaxException;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryNotFoundException;
import com.cloudera.beeswax.api.Results;
import com.cloudera.impala.thrift.ImpalaService.Client;
import com.inmobi.grill.driver.api.GrillResultSetMetadata;
import com.inmobi.grill.driver.api.InMemoryResultSet;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.query.QueryResult;
import com.inmobi.grill.query.ResultRow;

public class ImpalaResultSet extends InMemoryResultSet {

  private Logger logger = Logger.getLogger(ImpalaResultSet.class);
  private Client client;
  private Queue<List<Object>> a = new LinkedList<List<Object>>();
  private QueryHandle queryHandle;
  private boolean hasMoreData = true;
  private int size =0;

  public ImpalaResultSet(Client client, QueryHandle queryHandle) {
    this.client = client;
    this.queryHandle = queryHandle;

  }

  @Override
  public int size() {
    return this.size;

  }

  @Override
  public boolean hasNext() throws GrillException {
    return (this.hasMoreData || this.a.size() != 0);
  }

  @Override
  public ResultRow next() throws GrillException {

    Results resultSet = null;

    try {
      if (this.a.size() == 0) {
        if (this.hasMoreData) {

          resultSet = client.fetch(queryHandle, false, -1);
          List<String> results = resultSet.getData();
          size+=results.size();
          this.a.addAll(convert(results));
          if (!resultSet.isHas_more()) {
            this.hasMoreData = false;
            client.close(queryHandle);
          }
        }
        if (a.size() == 0) {
          logger.error("No more rows" );
          throw new GrillException("No more rows ");
        } else {
          return new ResultRow(this.a.remove());
        }
      } else {
        return new ResultRow(this.a.remove());
      }
    } catch (QueryNotFoundException e) {
      logger.error(e.getMessage() , e);
      throw new GrillException(e.getMessage(), e);
    } catch (BeeswaxException e) {
      logger.error(e.getMessage() , e);
      throw new GrillException(e.getMessage(), e);
    } catch (TException e) {
      logger.error(e.getMessage() , e);
      throw new GrillException(e.getMessage(), e);
    }

  }
  /**
   * converts the impala output to Breeze resultset format
   * @param inputList
   * @return
   */
  private List<List<Object>> convert(List<String> inputList) {
    List<List<Object>> returnList = new ArrayList<List<Object>>();
    for (String eachElement : inputList) {
      List<Object> rowSet = new ArrayList<Object>();
      StringTokenizer st = new StringTokenizer(eachElement);
      while (st.hasMoreElements()) {
        rowSet.add(st.nextToken());
      }
      returnList.add(rowSet);

    }
    return returnList;
  }

  @Override
  public GrillResultSetMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setFetchSize(int size) {
    // TODO Auto-generated method stub

  }
}
