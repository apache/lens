package com.inmobi.yoda.hive;

import com.inmobi.dw.yoda.mr.aggregators.Aggregator;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class YodaUDAFWrapper extends UDAF {
  public static class UDAFState {
    Aggregator yodaAggregator;
  }

  public static class YodaUDAFEvaluator implements UDAFEvaluator {
    UDAFState state;

    public YodaUDAFEvaluator() {
      super();
      state = new UDAFState();
    }

    @Override
    public void init() {
    }
  }
}
