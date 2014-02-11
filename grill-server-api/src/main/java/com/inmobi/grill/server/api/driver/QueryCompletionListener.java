package com.inmobi.grill.server.api.driver;

import com.inmobi.grill.api.query.QueryHandle;

public interface QueryCompletionListener {

  public void onCompletion(QueryHandle handle);

  public void onError(QueryHandle handle, String error);

}
