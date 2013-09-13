package com.inmobi.grill.driver.hive;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.api.Operator;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.Task;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import com.inmobi.grill.api.QueryCost;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.exception.GrillException;

public class HiveQueryPlan extends QueryPlan {
  enum ParserState {
    BEGIN,
    FILE_OUTPUT_OPERATOR,
    TABLE_SCAN,
    JOIN,
  };

  public HiveQueryPlan(List<String> explainOutput, QueryHandle queryHandle) {
    tablesQueried = new ArrayList<String>();
    tableWeights = new HashMap<String, Double>();
    setHandle(queryHandle);
    setExecMode(ExecMode.BATCH);
    setScanMode(ScanMode.PARTIAL_SCAN);
    extractPlanDetails(explainOutput);
  }

  private void extractPlanDetails(List<String> explainOutput) {
    ParserState state = ParserState.BEGIN;

    for (String line : explainOutput) {
      System.out.println("@@" + line);
      String tr = line.trim();
      state = findState(tr, state);
      System.out.println("@@############################################ STATE " + state);
      switch (state) {
        case FILE_OUTPUT_OPERATOR:
          if (tr.startsWith("directory:")) {
            String outputPath = tr.replace("directory:", "").trim();
            resultDestination = outputPath;
          }
          break;
        case TABLE_SCAN:
          if (tr.startsWith("alias:")) {
            String tableName = tr.replace("alias:", "").trim();
            tablesQueried.add(tableName);
            tableWeights.put(tableName, 1d);
          }
          break;
        case JOIN:
          numJoins++;
          break;
      }

    }
	}

  private ParserState findState(String tr, ParserState state) {
    if (tr.equals("File Output Operator")) {
      return ParserState.FILE_OUTPUT_OPERATOR;
    } else if (tr.equals("TableScan")) {
      return ParserState.TABLE_SCAN;
    } else if (tr.equals("Map Join Operator")) {
      return ParserState.JOIN;
    }

    return state;
  }

  @Override
	public String getPlan() {
		return "";
	}

	@Override
	public QueryCost getCost() {
		return null;
	}

}
