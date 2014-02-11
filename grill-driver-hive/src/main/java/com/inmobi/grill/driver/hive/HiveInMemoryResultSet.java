package com.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.InMemoryResultSet;
import com.inmobi.grill.exception.GrillException;

public class HiveInMemoryResultSet implements InMemoryResultSet {
	private final CLIServiceClient client;
	private final OperationHandle opHandle;
	private TableSchema metadata;
	private TRowSet rowSet;
	private int fetchSize = 100;
	private Iterator<TRow> rowItr;
	private boolean noMoreResults;
	
	public HiveInMemoryResultSet(OperationHandle hiveHandle, CLIServiceClient client) {
		this.client = client;
		this.opHandle = hiveHandle;
	}
	
	private TableSchema getTableSchema() throws GrillException {
		if (metadata == null) {
			try {
				metadata = client.getResultSetMetadata(opHandle);
			} catch (HiveSQLException e) {
				throw new GrillException(e);
			}
		}
		return metadata;
	}
	
	@Override
	public int size() throws GrillException {
		return getTableSchema().getSize();
	}

	@Override
	public GrillResultSetMetadata getMetadata() throws GrillException {
		return new GrillResultSetMetadata() {

			@Override
			public List<Column> getColumns() {
				try {
					List<ColumnDescriptor> descriptors = getTableSchema().getColumnDescriptors();
					
					if (descriptors == null) {
						return null;
					}
					
					List<Column> columns = new ArrayList<Column>(descriptors.size());
					
					for (ColumnDescriptor desc : descriptors) {
						columns.add(new Column(desc.getName(), desc.getTypeName()));
					}
					
					return columns;
				} catch (GrillException e) {
					return null;
				}
			}
		};
	}

	@Override
	public boolean hasNext() throws GrillException {
		if (noMoreResults) {
			return false;
		}
		
		if (rowItr == null || !rowItr.hasNext()) {
			try {
				rowSet = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, fetchSize).toTRowSet();
				noMoreResults = rowSet.getRowsSize() == 0;
			} catch (Exception e) {
				throw new GrillException(e);
			}
			rowItr = rowSet.getRowsIterator();
		}
		return rowItr.hasNext();
	}

	@Override
	public List<Object> next() throws GrillException {
		TRow row = rowItr.next();
		
		List<Object> results = new ArrayList<Object>(row.getColValsSize());
    List<TColumnValue> thriftRow = row.getColVals();
		for (int i = 0 ; i < row.getColValsSize(); i++) {
			results.add(thriftRow.get(i).getFieldValue());
		}
		
		return results;
	}

	@Override
	public void setFetchSize(int size) throws GrillException {
		assert size >= 0;
		fetchSize = size == 0 ? Integer.MAX_VALUE : size;
	}

}
