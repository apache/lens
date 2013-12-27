package com.inmobi.grill.query.service;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.events.QueryEvent;
import com.inmobi.grill.server.api.events.QueryEventListener;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.*;

public class EventServiceImpl implements GrillEventService {
  public static final Logger LOG = Logger.getLogger(EventServiceImpl.class);
  final Map<Class<? extends QueryEvent>, List<QueryEventListener>> eventListeners;
  private volatile boolean running;

  public EventServiceImpl() {
    eventListeners = new HashMap<Class<? extends QueryEvent>, List<QueryEventListener>>();
  }

  @SuppressWarnings("unchecked")
  protected final Class<? extends QueryEvent> getListenerType(QueryEventListener listener) {
    for (Method m : listener.getClass().getMethods()) {
      if (QueryEventListener.HANDLER_METHOD_NAME.equals(m.getName())) {
        // Found handler method
        return  (Class<? extends QueryEvent>) m.getParameterTypes()[0];
      }
    }
    return null;
  }

  @Override
  public void addListener(QueryEventListener listener) {
    Class<? extends QueryEvent> listenerEventType = getListenerType(listener);
    synchronized (eventListeners) {
      List<QueryEventListener> listeners = eventListeners.get(listenerEventType);
      if (listeners == null) {
        listeners = new ArrayList<QueryEventListener>();
        eventListeners.put(listenerEventType, listeners);
      }
      listeners.add(listener);
      LOG.info("Added listener " + listener);
    }
  }

  @Override
  public void removeListener(QueryEventListener listener) {
    synchronized (eventListeners) {
      List<QueryEventListener> listeners = eventListeners.get(getListenerType(listener));
      if (listeners != null) {
        if (listeners.remove(listener)) {
          LOG.info("Removed listener " + listener);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void handleEvent(List<QueryEventListener> listeners, QueryEvent evt) {
    if (listeners != null && !listeners.isEmpty()) {
      for (QueryEventListener listener : listeners) {
        try {
          listener.onQueryEvent(evt);
        } catch (Exception exc) {
          LOG.error("Error in handling event: " + evt.getQueryHandle() + "//" + evt.getId()
            + " for listener " + listener, exc);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handleEvent(QueryEvent evt) throws GrillException {
    if (!running || evt == null) {
      return;
    }

    Class<? extends QueryEvent> evtClass = evt.getClass();
    handleEvent(eventListeners.get(evtClass), evt);
    Class<?> superClass =  evtClass.getSuperclass();

    while (QueryEvent.class.isAssignableFrom(superClass)) {
      if (eventListeners.containsKey(superClass)) {
        handleEvent(eventListeners.get(superClass), evt);
      }
      superClass = superClass.getSuperclass();
    }
  }

  @Override
  public Collection<QueryEventListener> getListeners(Class<? extends QueryEvent> changeType) {
    return eventListeners.get(changeType);
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public void init() throws GrillException {
  }

  @Override
  public void start() throws GrillException {
    running = true;
    LOG.info("Event listener service started");
  }

  @Override
  public void stop() throws GrillException {
    running = false;
    LOG.info("Event listener service stopped");
  }
}
