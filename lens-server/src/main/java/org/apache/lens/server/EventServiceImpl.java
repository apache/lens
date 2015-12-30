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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEvent;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.health.HealthStatus;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;

import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of LensEventService
 */
@Slf4j
public class EventServiceImpl extends AbstractService implements LensEventService {

  /** The event listeners. */
  private final Map<Class<? extends LensEvent>, List<LensEventListener>> eventListeners
    = new HashMap<Class<? extends LensEvent>, List<LensEventListener>>();

  /** The event handler pool. */
  private ExecutorService eventHandlerPool;

  /**
   * Instantiates a new event service impl.
   *
   * @param name the name
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
    ThreadFactory factory = new BasicThreadFactory.Builder()
      .namingPattern("Event_Service_Thread-%d")
      .daemon(false)
      .priority(Thread.NORM_PRIORITY)
      .build();
    eventHandlerPool = Executors.newFixedThreadPool(hiveConf.getInt(LensConfConstants.EVENT_SERVICE_THREAD_POOL_SIZE,
      numProcs), factory);
    super.init(hiveConf);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.lens.server.api.events.LensEventService#removeListener
   * (org.apache.lens.server.api.events.LensEventListener)
   */
  @Override
  public void removeListener(LensEventListener listener) {
    synchronized (eventListeners) {
      for (List<LensEventListener> listeners : eventListeners.values()) {
        if (listeners.remove(listener)) {
          log.info("Removed listener {}", listener);
        }
      }
    }
  }

  /**
   * Handle event.
   *
   * @param listeners the listeners
   * @param evt       the evt
   */
  @SuppressWarnings("unchecked")
  private void handleEvent(List<LensEventListener> listeners, LensEvent evt) {
    if (listeners != null && !listeners.isEmpty()) {
      for (LensEventListener listener : listeners) {
        try {
          listener.onEvent(evt);
        } catch (Exception exc) {
          log.error("Error in handling event {} for listener {}", evt.getEventId(), listener, exc);
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
     * @param event the event
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
  public <T extends LensEvent> Collection<LensEventListener> getListeners(Class<T> eventType) {
    return Collections.unmodifiableList(eventListeners.get(eventType));
  }

  @Override
  public HealthStatus getHealthStatus() {
    return (this.getServiceState().equals(STATE.STARTED)
        && !eventHandlerPool.isShutdown()
        && !eventHandlerPool.isTerminated())
        ? new HealthStatus(true, "Event service is healthy.")
        : new HealthStatus(false, "Event service is unhealthy.");
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
    if (eventHandlerPool != null) {
      List<Runnable> pending = eventHandlerPool.shutdownNow();
      if (pending != null && !pending.isEmpty()) {
        StringBuilder pendingMsg = new StringBuilder("Pending Events:");
        for (Runnable handler : pending) {
          if (handler instanceof EventHandler) {
            pendingMsg.append(((EventHandler) handler).event.getEventId()).append(",");
          }
        }
        log.info("Event listener service stopped while {} events still pending", pending.size());
        log.info(pendingMsg.toString());
      }
    }
    log.info("Event service stopped");
    super.stop();
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
  public <T extends LensEvent> void addListenerForType(LensEventListener<? super T> listener, Class<T> eventType) {
    synchronized (eventListeners) {
      List<LensEventListener> listeners = eventListeners.get(eventType);
      if (listeners == null) {
        listeners = new ArrayList<LensEventListener>();
        eventListeners.put(eventType, listeners);
      }
      listeners.add(listener);
    }
    log.info("Added listener {} for type:{}", listener, eventType.getName());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.events.LensEventService#removeListenerForType(org.apache.lens.server.api.events.
   * LensEventListener, java.lang.Class)
   */
  @Override
  public <T extends LensEvent> void removeListenerForType(LensEventListener<? super T> listener, Class<T> eventType) {
    synchronized (eventListeners) {
      List<LensEventListener> listeners = eventListeners.get(eventType);
      if (listeners != null) {
        if (listeners.remove(listener)) {
          log.info("Removed listener {}", listener);
        }
      }
    }
  }
}
