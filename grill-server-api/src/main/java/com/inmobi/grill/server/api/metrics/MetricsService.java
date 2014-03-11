package com.inmobi.grill.server.api.metrics;


public interface MetricsService {
  public static final String NAME = "metrics";
  
  /**
   * Increment a counter with the given name
   * Actual name of the counter will be <pre>MetricRegistry.name(MetricsService.class, counter)<pre>
   * @param counter
   */
  public void incrCounter(String counter);
  
  /**
   * Increment a counter with the name constructed using given class and counter name
   * Actual name of the counter will be <pre>MetricRegistry.name(cls, counter)<pre>
   * @param counter
   * @param cls Class of the counter for namespacing the counter
   */
  public void incrCounter(Class<?> cls, String counter);
  
  /**
   * Decrement a counter with the name costructed using given class and counter name
   * Actual name of the counter will be <pre>MetricRegistry.name(cls, counter)<pre>
   * @param cls Class of the counter for namespacing of counters
   * @param counter
   */
  public void decrCounter(Class<?> cls, String counter);
  
  /**
   * Decrement a counter with the given name
   * Actual name of the counter will be <pre>MetricRegistry.name(MetricsService.class, counter)<pre>
   * @param counter
   */
  public void decrCounter(String counter);
  
  /**
   * Get current value of the counter
   */
  public long getCounter(String counter);
  
  /**
   * Get current value of the counter
   */
  public long getCounter(Class<?> cls, String counter);
  
  /**
   * Query engine counter names
   */
  public static final String QUEUED_QUERIES = "queued-queries";
  public static final String RUNNING_QUERIES = "running-queries";
  public static final String FINISHED_QUERIES = "finished-queries";
  public static final String ACCEPTED_QUERIES = "accepted-queries";
  public static final String CANCELLED_QUERIES = "cancelled-queries";
  public static final String FAILED_QUERIES = "failed-queries";
  
  /**
   * Get value of queueud query metric
   * @return number of queued queries
   */
  public long getQueuedQueries();
  
  /**
   * Get value of running query metric
   * @return number of running queries
   */
  public long getRunningQueries();
  
  /**
   * Get value of finished query metric
   * @return number of finished queries
   */
  public long getFinishedQueries();
  
  /**
   * Get value of accepted query metric
   * @return number of accepted queries
   */
  public long getAcceptedQueries();
  
  /**
   * Get value of cancelled query metric
   * @return number of cancelled queries
   */
  public long getCancelledQueries();
  
  /**
   * Get value of failed query metric
   * @return number of failed queries
   */
  public long getFailedQueries();
}
