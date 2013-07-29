package con.inmobi.grill.driver.hive;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import com.inmobi.grill.api.QueryCost;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.exception.GrillException;

public class HiveQueryPlan extends QueryPlan {
	private Query plan;
	private String jsonPlan;
	public HiveQueryPlan(String planJson) throws GrillException {
		this.jsonPlan = planJson;
		
		// Read the object back from JSON
		TMemoryBuffer tmb = null;
		try {
	    tmb = new TMemoryBuffer(planJson.length());
	    byte[] buf = planJson.getBytes("UTF-8");
	    tmb.write(buf, 0, buf.length);
	    TJSONProtocol oprot = new TJSONProtocol(tmb);
	    plan = new Query();
	    plan.read(oprot);
		} catch (TException e) {
			throw new GrillException("Error reading hive query plan", e);
		} catch (UnsupportedEncodingException e) {
			throw new GrillException("Error reading hive query plan", e);
		} finally {
			if (tmb != null) {
				tmb.close();
			}
		}
		
		setExecMode(ExecMode.BATCH);
		// Partitions should be counted as partial scan
		setScanMode(ScanMode.PARTIAL_SCAN);
		extractPlanDetails(plan);
	}

	private void extractPlanDetails(Query plan) {
		// TODO set the weights and integer fields in QueryPlan based on Hive's query plan object
	}

	@Override
	public String getPlan() {
		return jsonPlan;
	}

	@Override
	public QueryCost getCost() {
		return null;
	}

}
