package org.apache.lens.server.query;

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

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.events.*;
import org.apache.lens.server.api.query.QueryEnded;
import org.apache.lens.server.api.query.QueryFailed;
import org.apache.lens.server.api.query.QuerySuccess;
import org.apache.lens.server.api.query.QueuePositionChange;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

@Test(groups="unit-test")
public class TestEventService {
  public static final Logger LOG = Logger.getLogger(TestEventService.class);
  EventServiceImpl service;
  GenericEventListener genericEventListener;
  MockFailedListener failedListener;
  MockQueuePositionChange queuePositionChangeListener;
  MockEndedListener endedListener;
  CountDownLatch latch;

  class GenericEventListener extends AsyncEventListener<LensEvent> {
    boolean processed = false;
    @Override
    public void process(LensEvent event) {
      processed = true;
      latch.countDown();
      LOG.info("LensEvent:" + event.getEventId());
    }
  }

  class MockFailedListener implements LensEventListener<QueryFailed> {
    boolean processed = false;
    @Override
    public void onEvent(QueryFailed change) throws LensException {
      processed = true;
      latch.countDown();
      LOG.info("Query Failed event: " + change);
    }
  }

  class MockEndedListener implements LensEventListener<QueryEnded> {
    boolean processed = false;
    @Override
    public void onEvent(QueryEnded change) throws LensException {
      processed = true;
      latch.countDown();
      LOG.info("Query ended event: " + change);
    }
  }

  class MockQueuePositionChange implements LensEventListener<QueuePositionChange> {
    boolean processed = false;
    @Override
    public void onEvent(QueuePositionChange change) throws LensException {
      processed = true;
      latch.countDown();
      LOG.info("Query position changed: " + change);
    }
  }


  @BeforeTest
  public void setup() throws Exception {
    LensServices.get().init(LensServerConf.get());
    LensServices.get().start();
    service = LensServices.get().getService(LensEventService.NAME);
    assertNotNull(service);
    LOG.info("Service started " + service) ;
  }

  @Test
  public void testAddListener() {
    int listenersBefore = ((EventServiceImpl) service).getEventListeners().keySet().size();
    genericEventListener = new GenericEventListener();
    service.addListener(genericEventListener);
    endedListener = new MockEndedListener();
    service.addListener(endedListener);
    failedListener = new MockFailedListener();
    service.addListener(failedListener);
    queuePositionChangeListener = new MockQueuePositionChange();
    service.addListener(queuePositionChangeListener);

    assertTrue(service.getListeners(LensEvent.class).contains(genericEventListener));
    assertTrue(service.getListeners(QueryFailed.class).contains(failedListener));
    assertTrue(service.getListeners(QueryEnded.class).contains(endedListener));
    assertTrue(service.getListeners(QueuePositionChange.class).contains(queuePositionChangeListener));
  }

  @Test
  public void testRemoveListener() {
    MockFailedListener toRemove = new MockFailedListener();
    service.addListener(toRemove);
    assertEquals(service.getListeners(QueryFailed.class).size(), 2);
    service.removeListener(toRemove);
    assertEquals(service.getListeners(QueryFailed.class).size(), 1);
  }

  private void resetListeners() {
    genericEventListener.processed = false;
    endedListener.processed = false;
    failedListener.processed = false;
    queuePositionChangeListener.processed = false;
  }

  @Test
  public void testHandleEvent() throws Exception{
    QueryHandle query = new QueryHandle(UUID.randomUUID());
    String user = "user";
    long now = System.currentTimeMillis();
    QueryFailed failed = new QueryFailed(now, QueryStatus.Status.RUNNING, QueryStatus.Status.FAILED, query, user, null);
    QuerySuccess success = new QuerySuccess(now, QueryStatus.Status.RUNNING, QueryStatus.Status.SUCCESSFUL, query);
    QueuePositionChange positionChange = new QueuePositionChange(now, 1, 0, query);

    try {
      latch = new CountDownLatch(3);
      LOG.info("Sending event: " + failed);
      service.notifyEvent(failed);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertTrue(endedListener.processed);
      assertTrue(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      latch = new CountDownLatch(2);
      LOG.info("Sending event : " + success);
      service.notifyEvent(success);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertTrue(endedListener.processed);
      assertFalse(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      latch = new CountDownLatch(2);
      LOG.info("Sending event: " + positionChange);
      service.notifyEvent(positionChange);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertFalse(endedListener.processed);
      assertFalse(failedListener.processed);
      assertTrue(queuePositionChangeListener.processed);
      resetListeners();

      LensEvent genEvent = new LensEvent(now) {
        @Override
        public String getEventId() {
          return "TEST_EVENT";
        }
      };

      latch = new CountDownLatch(1);
      LOG.info("Sending generic event " + genEvent.getEventId());
      service.notifyEvent(genEvent);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertFalse(endedListener.processed);
      assertFalse(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
    } catch (LensException e) {
      fail(e.getMessage());
    }
  }
}
