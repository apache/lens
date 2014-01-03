package com.inmobi.grill.query.service;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.events.*;
import com.inmobi.grill.server.api.events.query.QueryEnded;
import com.inmobi.grill.server.api.events.query.QueryFailed;
import com.inmobi.grill.server.api.events.query.QuerySuccess;
import com.inmobi.grill.server.api.events.query.QueuePositionChange;
import com.inmobi.grill.service.GrillServices;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class TestEventService {
  public static final Logger LOG = Logger.getLogger(TestEventService.class);
  EventServiceImpl service;
  GenericEventListener genericEventListener;
  MockFailedListener failedListener;
  MockQueuePositionChange queuePositionChangeListener;
  MockEndedListener endedListener;
  CountDownLatch latch;

  class GenericEventListener extends AsyncEventListener<GrillEvent> {
    boolean processed = false;
    @Override
    public void process(GrillEvent event) {
      processed = true;
      latch.countDown();
      LOG.info("GrillEvent:" + event.getEventId());
    }
  }

  class MockFailedListener implements GrillEventListener<QueryFailed> {
    boolean processed = false;
    @Override
    public void onEvent(QueryFailed change) throws GrillException {
      processed = true;
      latch.countDown();
      LOG.info("Query Failed event: " + change);
    }
  }

  class MockEndedListener implements GrillEventListener<QueryEnded> {
    boolean processed = false;
    @Override
    public void onEvent(QueryEnded change) throws GrillException {
      processed = true;
      latch.countDown();
      LOG.info("Query ended event: " + change);
    }
  }

  class MockQueuePositionChange implements GrillEventListener<QueuePositionChange> {
    boolean processed = false;
    @Override
    public void onEvent(QueuePositionChange change) throws GrillException {
      processed = true;
      latch.countDown();
      LOG.info("Query position changed: " + change);
    }
  }


  @BeforeTest
  public void setup() throws Exception {
    GrillServices.get().init(new HiveConf());
    GrillServices.get().start();
    service = GrillServices.get().getService(GrillEventService.NAME);
    assertNotNull(service);
    LOG.info("Service started " + service) ;
  }

  @AfterTest
  public void cleanup() throws Exception {
    service.stop();
  }

  @Test
  public void testAddListener() {
    int listenersBefore = ((EventServiceImpl) service).eventListeners.keySet().size();
    genericEventListener = new GenericEventListener();
    service.addListener(genericEventListener);
    endedListener = new MockEndedListener();
    service.addListener(endedListener);
    failedListener = new MockFailedListener();
    service.addListener(failedListener);
    queuePositionChangeListener = new MockQueuePositionChange();
    service.addListener(queuePositionChangeListener);

    assertEquals(((EventServiceImpl) service).eventListeners.keySet().size() - listenersBefore, 4);
    assertEquals(service.getListeners(QueryFailed.class).size(), 1);
    assertEquals(service.getListeners(QueryEnded.class).size(), 1);
    assertEquals(service.getListeners(QueuePositionChange.class).size(), 1);
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

      GrillEvent genEvent = new GrillEvent(now) {
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
    } catch (GrillException e) {
      fail(e.getMessage());
    }
  }
}
