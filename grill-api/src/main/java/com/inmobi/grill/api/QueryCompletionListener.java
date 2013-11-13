package com.inmobi.grill.api;

public interface QueryCompletionListener {

  public void onCompletion(QueryHandle handle);

  public void onError(QueryHandle handle, String error);

}
