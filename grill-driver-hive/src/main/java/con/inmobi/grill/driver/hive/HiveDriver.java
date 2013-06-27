package con.inmobi.grill.driver.hive;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;

public class HiveDriver implements GrillDriver {

	@Override
	public List<String> getSupportedStorages() {
		return null;
	}

	@Override
	public void configure(Configuration conf) throws GrillException {
		
	}

	@Override
	public QueryPlan explain(String query, Configuration conf) throws GrillException {
		return null;
	}

	@Override
	public GrillResultSet execute(String query, Configuration conf) throws GrillException {
		return null;
	}

	@Override
	public QueryHandle executeAsync(String query, Configuration conf) throws GrillException {
		return null;
	}

	@Override
	public QueryStatus getStatus(QueryHandle handle)  throws GrillException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GrillResultSet fetchResultSet(QueryHandle handle)  throws GrillException {
		return null;
	}

	@Override
	public boolean cancelQuery(QueryHandle handle)  throws GrillException {
		return false;
	}

  

}
