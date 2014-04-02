package com.inmobi.grill.api;

@SuppressWarnings("serial")
public class GrillException extends Exception {
  public GrillException(String msg) {
    super(msg);
  }
  
  
  public GrillException(String msg, Throwable th){
    super(msg, th);
  }
  
  public GrillException() {
    super();
  }
  
  public GrillException(Throwable th) {
  	super(th);
  }
}
