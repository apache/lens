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
package org.apache.lens.server.api.events;

import java.util.concurrent.*;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
/**
 * Event listeners should implement this class if they wish to process events asynchronously. This should be used when
 * event processing can block, or is computationally intensive.
 *
 * @param <T> the generic type
 */
@Slf4j
public abstract class AsyncEventListener<T extends LensEvent> implements LensEventListener<T> {

  /**
   * The processor.
   */
  protected final ThreadPoolExecutor processor;

  /**
   * The event queue.
   */
  protected final BlockingQueue<Runnable> eventQueue;

  /**
   * Name of this Asynchronous Event Listener. Will be used for logging and to name the threads in thread pool that
   * allow asynchronous handling of events. If required, Sub Classes can override <code>getName</code> method to
   * provide more appropriate name.
   *
   * Default value is the class Name (Example QueryEndNotifier, ResultFormatter, etc)
   */
  @Getter(AccessLevel.PROTECTED)
  private final String name = this.getClass().getSimpleName();

  /**
   * Create a single threaded event listener with an unbounded queue, with daemon threads.
   */
  public AsyncEventListener() {
    this(1, 1);
  }

  /**
   * Create a event listener with poolSize threads with an unbounded queue and daemon threads.
   *
   * @param poolSize the pool size
   * @param maxPoolSize the max pool size
   */
  public AsyncEventListener(int poolSize, int maxPoolSize) {
    this(poolSize, maxPoolSize, -1, 10, true);
  }

  /**
   * Create an asynchronous event listener which uses a thread poool to process events.
   *
   * @param poolSize       size of the event processing pool
   * @param maxPoolSize    the max pool size
   * @param maxQueueSize   max size of the event queue, if this is non positive, then the queue is unbounded
   * @param timeOutSeconds time out in seconds when an idle thread is destroyed
   * @param isDaemon       if the threads used to process should be daemon threads,
   *                       if false, then implementation should call stop()
   *                       to stop the thread pool
   */
  public AsyncEventListener(int poolSize, int maxPoolSize, int maxQueueSize, long timeOutSeconds,
      final boolean isDaemon) {
    if (maxQueueSize <= 0) {
      eventQueue = new LinkedBlockingQueue<Runnable>();
    } else {
      eventQueue = new ArrayBlockingQueue<Runnable>(maxQueueSize);
    }

    ThreadFactory factory = new BasicThreadFactory.Builder()
      .namingPattern(getName()+"_AsyncThread-%d")
      .daemon(isDaemon)
      .priority(Thread.NORM_PRIORITY)
      .build();
    processor = new ThreadPoolExecutor(poolSize, maxPoolSize, timeOutSeconds, TimeUnit.SECONDS, eventQueue, factory);
  }

  /**
   * Creates a new runnable and calls the process method in it.
   *
   * @param event the event
   * @throws LensException the lens exception
   */
  @Override
  public void onEvent(final T event) throws LensException {
    try {
      processor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            process(event);
          } catch (Throwable e) {
            log.error("{} Failed to process event {}", getName(), event, e);
          }
        }
      });
    } catch (RejectedExecutionException rejected) {
      throw new LensException(rejected);
    }
  }

  /**
   * Should implement the actual event handling.
   *
   * @param event the event
   */
  public abstract void process(T event);

  /**
   * Should be called to stop the event processor thread.
   */
  public void stop() {
    processor.shutdownNow();
  }

  public BlockingQueue<Runnable> getEventQueue() {
    return eventQueue;
  }
}
