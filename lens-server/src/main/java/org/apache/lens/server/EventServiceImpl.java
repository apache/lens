package org.apache.lens.server;

/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.lens.api.LensException;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.events.LensEvent;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.events.LensEventService;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EventServiceImpl extends AbstractService implements LensEventService {
  public static final Log LOG = LogFactory.getLog(EventServiceImpl.class);
  private final Map<Class<? extends LensEvent>, List<LensEventListener>> eventListeners =
    new HashMap<Class<? extends LensEvent>, List<LensEventListener>>();
  private ExecutorService eventHandlerPool;

  public EventServiceImpl(String name) {
    super(name);
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    int numProcs = Runtime.getRuntime().availableProcessors();
    eventHandlerPool =
      Executors.newFixedThreadPool(hiveConf.getInt(LensConfConstants.EVENT_SERVICE_THREAD_POOL_SIZE, numProcs));
    super.init(hiveConf);
  }

  @SuppressWarnings("unchecked")
  protected final Class<? extends LensEvent> getListenerType(LensEventListener listener) {
    for (Method m : listener.getClass().getMethods()) {
      if (LensEventListener.HANDLER_METHOD_NAME.equals(m.getName())) {
        // Found handler method
        return  (Class<? extends LensEvent>) m.getParameterTypes()[0];
      }
    }
    return null;
  }

  @Override
  public void addListener(LensEventListener listener) {
    Class<? extends LensEvent> listenerEventType = getListenerType(listener);
    synchronized (eventListeners) {
      List<LensEventListener> listeners = eventListeners.get(listenerEventType);
      if (listeners == null) {
        listeners = new ArrayList<LensEventListener>();
        eventListeners.put(listenerEventType, listeners);
      }
      listeners.add(listener);
    }
    LOG.info("Added listener " + listener);
  }

  @Override
  public void removeListener(LensEventListener listener) {
    synchronized (eventListeners) {
      for (List<LensEventListener> listeners : eventListeners.values()) {
        if (listeners.remove(listener)) {
          LOG.info("Removed listener " + listener);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void handleEvent(List<LensEventListener> listeners, LensEvent evt) {
    if (listeners != null && !listeners.isEmpty()) {
      for (LensEventListener listener : listeners) {
        try {
          listener.onEvent(evt);
        } catch (Exception exc) {
          LOG.error("Error in handling event" + evt.getEventId() + " for listener " + listener, exc);
        }
      }
    }
  }

  private final class EventHandler implements Runnable {
    final LensEvent event;

    EventHandler(LensEvent event) {
      this.event = event;
    }

    public void run() {
      Class<? extends LensEvent> evtClass = event.getClass();
      // Call listeners directly listening for this event type
      handleEvent(eventListeners.get(evtClass), event);
      Class<?> superClass =  evtClass.getSuperclass();

      // Call listeners which listen of super types of this event type
      while (LensEvent.class.isAssignableFrom(superClass)) {
        if (eventListeners.containsKey(superClass)) {
          handleEvent(eventListeners.get(superClass), event);
        }
        superClass = superClass.getSuperclass();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void notifyEvent(final LensEvent evt) throws LensException {
    if (getServiceState() != STATE.STARTED) {
      throw new LensException("Event service is not in STARTED state. Current state is " + getServiceState());
    }

    if (evt == null) {
      return;
    }
    eventHandlerPool.submit(new EventHandler(evt));
  }

  @Override
  public Collection<LensEventListener> getListeners(Class<? extends LensEvent> eventType) {
    return Collections.unmodifiableList(eventListeners.get(eventType));
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public void stop() {
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
    super.stop();
    LOG.info("Event service stopped");
  }

  public Map<Class<? extends LensEvent>, List<LensEventListener>> getEventListeners() {
    return eventListeners;
  }

  @Override
  public void addListenerForType(LensEventListener listener, Class<? extends LensEvent> eventType) {
    synchronized (eventListeners) {
      List<LensEventListener> listeners = eventListeners.get(eventType);
      if (listeners == null) {
        listeners = new ArrayList<LensEventListener>();
        eventListeners.put(eventType, listeners);
      }
      listeners.add(listener);
    }
    LOG.info("Added listener " + listener + " for type:" + eventType.getName());
  }

  @Override
  public void removeListenerForType(LensEventListener listener,
      Class<? extends LensEvent> eventType) {
    synchronized (eventListeners) {
      List<LensEventListener> listeners = eventListeners.get(eventType);
      if (listeners != null) {
        if (listeners.remove(listener)) {
          LOG.info("Removed listener " + listener);
        }
      }
    }
  }
}
