/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.lens.api.LensException;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.events.LensEvent;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.events.LensEventService;

/**
 * Implementation of LensEventService
 */
public class EventServiceImpl extends AbstractService implements LensEventService {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(EventServiceImpl.class);

  /** The event listeners. */
  private final Map<Class<? extends LensEvent>, List<LensEventListener>> eventListeners = new HashMap<Class<? extends LensEvent>, List<LensEventListener>>();

  /** The event handler pool. */
  private ExecutorService eventHandlerPool;

  /**
   * Instantiates a new event service impl.
   *
   * @param name
   *          the name
   */
  public EventServiceImpl(String name) {
    super(name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.AbstractService#init(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    int numProcs = Runtime.getRuntime().availableProcessors();
    eventHandlerPool = Executors.newFixedThreadPool(hiveConf.getInt(LensConfConstants.EVENT_SERVICE_THREAD_POOL_SIZE,
        numProcs));
    super.init(hiveConf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.events.LensEventService#removeListener(org.apache.lens.server.api.events.LensEventListener
   * )
   */
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

  /**
   * Handle event.
   *
   * @param listeners
   *          the listeners
   * @param evt
   *          the evt
   */
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

  /**
   * The Class EventHandler.
   */
  private final class EventHandler implements Runnable {

    /** The event. */
    final LensEvent event;

    /**
     * Instantiates a new event handler.
     *
     * @param event
     *          the event
     */
    EventHandler(LensEvent event) {
      this.event = event;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
      Class<? extends LensEvent> evtClass = event.getClass();
      // Call listeners directly listening for this event type
      handleEvent(eventListeners.get(evtClass), event);
      Class<?> superClass = evtClass.getSuperclass();

      // Call listeners which listen of super types of this event type
      while (LensEvent.class.isAssignableFrom(superClass)) {
        if (eventListeners.containsKey(superClass)) {
          handleEvent(eventListeners.get(superClass), event);
        }
        superClass = superClass.getSuperclass();
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.LensEventService#notifyEvent(org.apache.lens.server.api.events.LensEvent)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.LensEventService#getListeners(java.lang.Class)
   */
  @Override
  public Collection<LensEventListener> getListeners(Class<? extends LensEvent> eventType) {
    return Collections.unmodifiableList(eventListeners.get(eventType));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.AbstractService#start()
   */
  @Override
  public synchronized void start() {
    super.start();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.AbstractService#stop()
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.LensEventService#addListenerForType(org.apache.lens.server.api.events.
   * LensEventListener, java.lang.Class)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.LensEventService#removeListenerForType(org.apache.lens.server.api.events.
   * LensEventListener, java.lang.Class)
   */
  @Override
  public void removeListenerForType(LensEventListener listener, Class<? extends LensEvent> eventType) {
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
