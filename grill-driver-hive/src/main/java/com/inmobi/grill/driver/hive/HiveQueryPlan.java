package com.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.inmobi.grill.api.QueryCost;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;

public class HiveQueryPlan extends QueryPlan {
  enum ParserState {
    BEGIN,
    FILE_OUTPUT_OPERATOR,
    TABLE_SCAN,
    JOIN,
    SELECT,
    GROUPBY,
    GROUPBY_KEYS,
    GROUPBY_EXPRS,
    MOVE,
    MAP_REDUCE,
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
    ParserState prevState = state;
    ArrayList<ParserState> states = new ArrayList<ParserState>();

    for (String line : explainOutput) {
      //System.out.println("@@ " + states + " [" +state + "] " + line);
      String tr = line.trim();
      prevState = state;
      state = nextState(tr, state);

      if (prevState != state) {
        states.add(prevState);
      }

      switch (state) {
        case MOVE:
          if (tr.startsWith("destination:")) {
            String outputPath = tr.replace("destination:", "").trim();
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
          if (tr.equals("condition map:")) {
            numJoins++;
          }
          break;
        case SELECT:
          if (tr.startsWith("expr:") && states.get(states.size() - 1) == ParserState.TABLE_SCAN) {
            numSels++;
          }
          break;
        case GROUPBY_EXPRS:
          if (tr.startsWith("expr:")) {
            numDefaultAggrExprs++;
          }
          break;
        case GROUPBY_KEYS:
          if (tr.startsWith("expr:")) {
            numGbys++;
          }
          break;
      }
    }
	}

  private ParserState nextState(String tr, ParserState state) {
    if (tr.equals("File Output Operator")) {
      return ParserState.FILE_OUTPUT_OPERATOR;
    } else if (tr.equals("Map Reduce")) {
      return ParserState.MAP_REDUCE;
    } else if (tr.equals("Move Operator")) {
      return ParserState.MOVE;
    } else if (tr.equals("TableScan")) {
      return ParserState.TABLE_SCAN;
    } else if (tr.equals("Map Join Operator")) {
      return ParserState.JOIN;
    } else if (tr.equals("Select Operator")) {
      return ParserState.SELECT;
    } else if (tr.equals("Group By Operator")) {
      return ParserState.GROUPBY;
    } else if (tr.startsWith("aggregations:") && state == ParserState.GROUPBY) {
      return ParserState.GROUPBY_EXPRS;
    } else if (tr.startsWith("keys:") && state == ParserState.GROUPBY_EXPRS) {
      return ParserState.GROUPBY_KEYS;
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
