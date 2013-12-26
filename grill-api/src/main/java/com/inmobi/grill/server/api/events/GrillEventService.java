package com.inmobi.grill.server.api.events;


import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.GrillService;

import java.util.Collection;

public interface GrillEventService extends GrillService {
  public void addListener(QueryEventListener listener);
  public void removeListener(QueryEventListener listener);
  public void handleEvent(QueryEvent change) throws GrillException;
  public Collection<QueryEventListener> getListeners(Class<? extends QueryEvent> changeType);
}
