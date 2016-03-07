/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import lombok.NonNull;

/**
 *
 * A priority blocking queue in which an element is not allowed to be removed from head while a bulk addAll operation
 * is ongoing. This allows all elements added through addAll to get equal chance in scheduling.
 *
 */
public class FairPriorityBlockingQueue<E> {

  /* PriorityBlockingQueue#DEFAULT_INITIAL_CAPACITY is 11 */

  private static final int DEFAULT_INITIAL_CAPACITY = 11;

  private final PriorityBlockingQueue<E> priorityBlockingQueue;
  private final Object fairnessLock = new Object();
  private final ReentrantLock conditionalWaitLock = new ReentrantLock();
  private final Condition notEmpty = conditionalWaitLock.newCondition();

  public FairPriorityBlockingQueue(@NonNull final Comparator<? super E> comparator) {
    this.priorityBlockingQueue = new PriorityBlockingQueue<E>(DEFAULT_INITIAL_CAPACITY, comparator);
  }
  /**
   *
   * take is implemented by using poll and a fairnessLock to synchronize removal from head of queue with bulk addAll
   * operation.
   *
   * @return
   * @throws InterruptedException
   */
  public E take() throws InterruptedException {

    E e;

    for (;;) {

      synchronized (fairnessLock) {
        e = priorityBlockingQueue.poll();
      }

      if (e != null) {
        return e;
      }

      waitUntilNotEmpty();
    }
  }

  public boolean remove(final Object o) {
    return priorityBlockingQueue.remove(o);
  }

  public void addAll(final Collection<? extends E> c) {
    synchronized (fairnessLock) {
      priorityBlockingQueue.addAll(c);
    }
    signalNotEmpty();
  }

  public boolean add(final E e) {

    boolean modified = priorityBlockingQueue.add(e);
    signalNotEmpty();
    return modified;
  }

  public int size() {
    return priorityBlockingQueue.size();
  }

  private void waitUntilNotEmpty() throws InterruptedException {

    conditionalWaitLock.lock();
    try {
      while (priorityBlockingQueue.size() < 1) {
        notEmpty.await();
      }
    } finally {
      conditionalWaitLock.unlock();
    }
  }

  private void signalNotEmpty() {

    conditionalWaitLock.lock();
    try {
      notEmpty.signal();
    } finally {
      conditionalWaitLock.unlock();
    }
  }
}
