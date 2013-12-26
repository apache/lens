package com.inmobi.grill.server.api.events;


import com.inmobi.grill.exception.GrillException;

public interface QueryEventListener<T extends QueryEvent> {
  public void onQueryEvent(T change) throws GrillException;
}