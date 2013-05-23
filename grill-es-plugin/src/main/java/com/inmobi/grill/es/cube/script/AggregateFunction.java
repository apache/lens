package com.inmobi.grill.es.cube.script;

public abstract class AggregateFunction {
  public AggregateFunction() {}
  public abstract void update(String... newval);
  public abstract void update(double... newval);
  public abstract double get();
  public abstract void set(double value);
  public abstract String name();
  
  @Override
  public String toString() {
    return new StringBuilder(name()).append(':').append(get()).toString();
  }
}


class SumDouble extends AggregateFunction {
  double value = 0;
  @Override
  public void update(String... newval) {
    for (String d : newval) {
      value += Double.parseDouble(d);
    }
  }

  @Override
  public void update(double... newval) {
    for (double d : newval) {
      value += d;
    }
  }

  @Override
  public double get() {
    return value;
  }

  @Override
  public void set(double val) {
    this.value = val;
  }

  @Override
  public String name() {
    return "sum";
  }
}


class MaxDouble extends AggregateFunction {
  double max = 0d;

  @Override
  public void update(String... newval) {
    for (String s : newval) {
      double d = Double.parseDouble(s);
      if (max  < d) {
        max = d;
      }
    }
  }

  @Override
  public void update(double... newval) {
    for (double d : newval) {
      if (max < d) {
        max = d;
      }
    }
  }

  @Override
  public double get() {
    return max;
  }

  @Override
  public void set(double val) {
    this.max = val;
  }

  @Override
  public String name() {
    return "max";
  }
}

class CountDouble extends AggregateFunction {
  private int count = 0;
  @Override
  public void update(String... newval) {
    count++;
  }

  @Override
  public void update(double... newval) {
    count++;
  }

  @Override
  public double get() {
    return count;
  }

  @Override
  public void set(double value) {
    this.count = (int) value;
  }

  @Override
  public String name() {
    return "count";
  }
}


class AvgDouble extends AggregateFunction {
  double sum = 0;
  int count = 0;
  @Override
  public void update(String... newval) {
    for (String sval : newval) {
      count++;
      sum += Double.parseDouble(sval);
    }
  }

  @Override
  public void update(double... newval) {
    for (double d : newval) {
      count++;
      sum += d;
    }
  }

  @Override
  public double get() {
    return count == 0 ? 0 : sum/count;
  }

  @Override
  public void set(double value) {
    this.sum = sum;
  }

  @Override
  public String name() {
    return "avg";
  }
  
}