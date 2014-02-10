package com.inmobi.grill.driver.api;

import com.inmobi.grill.query.QueryHandle;

public interface QueryCompletionListener {

  public void onCompletion(QueryHandle handle);

  public void onError(QueryHandle handle, String error);

}
