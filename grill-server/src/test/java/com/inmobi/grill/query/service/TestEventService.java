package com.inmobi.grill.query.service;

import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.events.*;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.*;

public class TestEventService {
  public static final Logger LOG = Logger.getLogger(TestEventService.class);
  GrillEventService service;
  MockFailedListener failedListener;
  MockQueuePositionChange queuePositionChangeListener;
  MockEndedListener endedListener;

  class MockFailedListener implements QueryEventListener<QueryFailed> {
    boolean processed = false;
    @Override
    public void onQueryEvent(QueryFailed change) throws GrillException {
      processed = true;
      LOG.info("Query Failed event: " + change);
    }
  }

  class MockEndedListener implements QueryEventListener<QueryEnded> {
    boolean processed = false;
    @Override
    public void onQueryEvent(QueryEnded change) throws GrillException {
      processed = true;
      LOG.info("Query ended event: " + change);
    }
  }

  class MockQueuePositionChange implements QueryEventListener<QueuePositionChange> {
    boolean processed = false;
    @Override
    public void onQueryEvent(QueuePositionChange change) throws GrillException {
      processed = true;
      LOG.info("Query position changed: " + change);
    }
  }


  @BeforeTest
  public void setup() throws Exception {
    service = new EventServiceImpl();
    service.init();
    service.start();
    LOG.info("Service started");
  }

  @AfterTest
  public void cleanup() throws Exception {
    service.stop();
  }

  @Test
  public void testAddListener() {
    endedListener = new MockEndedListener();
    service.addListener(endedListener);
    failedListener = new MockFailedListener();
    service.addListener(failedListener);
    queuePositionChangeListener = new MockQueuePositionChange();
    service.addListener(queuePositionChangeListener);

    assertEquals(((EventServiceImpl) service).eventListeners.keySet().size(), 3);
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
    endedListener.processed = false;
    failedListener.processed = false;
    queuePositionChangeListener.processed = false;
  }

  @Test
  public void testHandleEvent() {
    QueryHandle query = new QueryHandle(UUID.randomUUID());
    Exception cause = new Exception("Failure cause");
    String user = "user";
    QueryFailed failed = new QueryFailed(QueryStatus.Status.RUNNING, QueryStatus.Status.FAILED, query, user, cause);
    QuerySuccess success = new QuerySuccess(QueryStatus.Status.RUNNING, QueryStatus.Status.SUCCESSFUL, query);
    QueuePositionChange positionChange = new QueuePositionChange(1, 0, query);

    try {
      service.handleEvent(failed);
      assertTrue(endedListener.processed);
      assertTrue(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      service.handleEvent(success);
      assertTrue(endedListener.processed);
      assertFalse(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      service.handleEvent(positionChange);
      assertFalse(endedListener.processed);
      assertFalse(failedListener.processed);
      assertTrue(queuePositionChangeListener.processed);

    } catch (GrillException e) {
      fail(e.getMessage());
    }
  }
}
