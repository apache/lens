package com.inmobi.grill.query.service;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.events.GrillEvent;
import com.inmobi.grill.server.api.events.GrillEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.*;

public class EventServiceImpl implements GrillEventService {
  public static final Logger LOG = Logger.getLogger(EventServiceImpl.class);
  final Map<Class<? extends GrillEvent>, List<GrillEventListener>> eventListeners;
  private volatile boolean running;

  public EventServiceImpl() {
    eventListeners = new HashMap<Class<? extends GrillEvent>, List<GrillEventListener>>();
  }

  @SuppressWarnings("unchecked")
  protected final Class<? extends GrillEvent> getListenerType(GrillEventListener listener) {
    for (Method m : listener.getClass().getMethods()) {
      if (GrillEventListener.HANDLER_METHOD_NAME.equals(m.getName())) {
        // Found handler method
        return  (Class<? extends GrillEvent>) m.getParameterTypes()[0];
      }
    }
    return null;
  }

  @Override
  public void addListener(GrillEventListener listener) {
    Class<? extends GrillEvent> listenerEventType = getListenerType(listener);
    synchronized (eventListeners) {
      List<GrillEventListener> listeners = eventListeners.get(listenerEventType);
      if (listeners == null) {
        listeners = new ArrayList<GrillEventListener>();
        eventListeners.put(listenerEventType, listeners);
      }
      listeners.add(listener);
      LOG.info("Added listener " + listener);
    }
  }

  @Override
  public void removeListener(GrillEventListener listener) {
    synchronized (eventListeners) {
      List<GrillEventListener> listeners = eventListeners.get(getListenerType(listener));
      if (listeners != null) {
        if (listeners.remove(listener)) {
          LOG.info("Removed listener " + listener);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void handleEvent(List<GrillEventListener> listeners, GrillEvent evt) {
    if (listeners != null && !listeners.isEmpty()) {
      for (GrillEventListener listener : listeners) {
        try {
          listener.onEvent(evt);
        } catch (Exception exc) {
          LOG.error("Error in handling event" + evt.getEventId() + " for listener " + listener, exc);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handleEvent(GrillEvent evt) throws GrillException {
    if (!running || evt == null) {
      return;
    }

    Class<? extends GrillEvent> evtClass = evt.getClass();
    handleEvent(eventListeners.get(evtClass), evt);
    Class<?> superClass =  evtClass.getSuperclass();

    while (GrillEvent.class.isAssignableFrom(superClass)) {
      if (eventListeners.containsKey(superClass)) {
        handleEvent(eventListeners.get(superClass), evt);
      }
      superClass = superClass.getSuperclass();
    }
  }

  @Override
  public Collection<GrillEventListener> getListeners(Class<? extends GrillEvent> eventType) {
    return Collections.unmodifiableList(eventListeners.get(eventType));
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
