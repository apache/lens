package com.inmobi.grill.query.service;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.events.GrillEvent;
import com.inmobi.grill.server.api.events.GrillEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EventServiceImpl implements GrillEventService {
  public static final Logger LOG = Logger.getLogger(EventServiceImpl.class);
  final Map<Class<? extends GrillEvent>, List<GrillEventListener>> eventListeners;
  private volatile boolean running;
  private ExecutorService eventHandlerPool;

  public EventServiceImpl() {
    eventListeners = new HashMap<Class<? extends GrillEvent>, List<GrillEventListener>>();
    eventHandlerPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
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

  private final class EventHandler implements Runnable {
    final GrillEvent event;

    EventHandler(GrillEvent event) {
      this.event = event;
    }

    public void run() {
      Class<? extends GrillEvent> evtClass = event.getClass();
      // Call listeners directly listening for this event type
      handleEvent(eventListeners.get(evtClass), event);
      Class<?> superClass =  evtClass.getSuperclass();

      // Call listeners which listen of super types of this event type
      while (GrillEvent.class.isAssignableFrom(superClass)) {
        if (eventListeners.containsKey(superClass)) {
          handleEvent(eventListeners.get(superClass), event);
        }
        superClass = superClass.getSuperclass();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handleEvent(final GrillEvent evt) throws GrillException {
    if (!running || evt == null) {
      return;
    }
    eventHandlerPool.submit(new EventHandler(evt));
  }

  @Override
  public Collection<GrillEventListener> getListeners(Class<? extends GrillEvent> eventType) {
    return Collections.unmodifiableList(eventListeners.get(eventType));
  }

  public void start() throws GrillException {
    running = true;
    LOG.info("Event listener service started");
  }

  public void stop() throws GrillException {
    running = false;
    List<Runnable> pending = eventHandlerPool.shutdownNow();
    if (pending != null && !pending.isEmpty()) {
      StringBuilder pendingMsg = new StringBuilder("Pending Events:");
      for (Runnable handler : pending) {
        if (handler instanceof EventHandler) {
          pendingMsg.append(((EventHandler) handler).event.getEventId()).append(",");
        }
      }
      LOG.info("Event listener service stopped while " + pending.size() + " events still pending");
      LOG.info(pendingMsg.toString());
    }
    LOG.info("Event service stopped");
  }
}
