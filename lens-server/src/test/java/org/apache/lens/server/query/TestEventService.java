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
package org.apache.lens.server.query;

import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.LensEvent;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.query.*;
import org.apache.lens.server.api.session.SessionClosed;
import org.apache.lens.server.api.session.SessionExpired;
import org.apache.lens.server.api.session.SessionOpened;
import org.apache.lens.server.api.session.SessionRestored;
import org.apache.lens.server.query.QueryExecutionServiceImpl.QueryStatusLogger;
import org.apache.lens.server.stats.event.query.QueryExecutionStatistics;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestEventService.
 */
@Test(groups = "unit-test")
@Slf4j
public class TestEventService {

  /** The service. */
  EventServiceImpl service;

  /** The generic event listener. */
  GenericEventListener genericEventListener;

  /** The failed listener. */
  MockFailedListener failedListener;

  /** The queue position change listener. */
  MockQueuePositionChange queuePositionChangeListener;

  /** The ended listener. */
  MockEndedListener endedListener;

  /** the session opened listener */
  MockerSessionOpened sessionOpenedListener;

  /** the session closed listener */
  MockerSessionClosed sessionClosedListener;

  /** the session expired listener */
  MockerSessionExpired sessionExpiredListner;

  /** the session restored listener */
  MockerSessionRestored sessionRestoredListener;

  /** The latch. */
  CountDownLatch latch;

  /**
   * The listener interface for receiving genericEvent events. The class that is interested in processing a genericEvent
   * event implements this interface, and the object created with that class is registered with a component using the
   * component's <code>addGenericEventListener<code> method. When
   * the genericEvent event occurs, that object's appropriate
   * method is invoked.
   *
   */
  class GenericEventListener extends AsyncEventListener<LensEvent> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void process(LensEvent event) {
      processed = true;
      latch.countDown();
      log.info("LensEvent: {}", event.getEventId());
    }
  }

  /**
   * The listener interface for receiving mockFailed events. The class that is interested in processing a mockFailed
   * event implements this interface, and the object created with that class is registered with a component using the
   * component's <code>addMockFailedListener<code> method. When
   * the mockFailed event occurs, that object's appropriate
   * method is invoked.
   *
   */
  class MockFailedListener implements LensEventListener<QueryFailed> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(QueryFailed change) throws LensException {
      processed = true;
      latch.countDown();
      log.info("Query Failed event: {}", change);
    }
  }

  /**
   * The listener interface for receiving mockEnded events. The class that is interested in processing a mockEnded event
   * implements this interface, and the object created with that class is registered with a component using the
   * component's <code>addMockEndedListener<code> method. When
   * the mockEnded event occurs, that object's appropriate
   * method is invoked.
   *
   */
  class MockEndedListener implements LensEventListener<QueryEnded> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(QueryEnded change) throws LensException {
      processed = true;
      latch.countDown();
      log.info("Query ended event: {}", change);
    }
  }

  /**
   * The Class MockQueuePositionChange.
   */
  class MockQueuePositionChange implements LensEventListener<QueuePositionChange> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(QueuePositionChange change) throws LensException {
      processed = true;
      latch.countDown();
      log.info("Query position changed: {}", change);
    }
  }

  /**
   * The Class MockerSessionOpened.
   */
  class MockerSessionOpened implements LensEventListener<SessionOpened> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(SessionOpened event) throws LensException {
      processed = true;
      latch.countDown();
      log.info("Session opened: {}", event);
    }
  }

  /**
   * The Class MockerSessionClosed.
   */
  class MockerSessionClosed implements LensEventListener<SessionClosed> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(SessionClosed event) throws LensException {
      processed = true;
      latch.countDown();
      log.info("Session closed: {}", event);
    }
  }

  /**
   * The Class MockerSessionExpired.
   */
  class MockerSessionExpired implements LensEventListener<SessionExpired> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(SessionExpired event) throws LensException {
      processed = true;
      latch.countDown();
      log.info("Session expired: {}", event);
    }
  }

  /**
   * The Class MockerSessionRestored.
   */
  class MockerSessionRestored implements LensEventListener<SessionRestored> {

    /** The processed. */
    boolean processed = false;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(SessionRestored event) throws LensException {
      processed = true;
      latch.countDown();
      log.info("Session restored: {}", event);
    }
  }

  /**
   * Setup.
   *
   * @throws Exception the exception
   */
  @BeforeClass
  public void setup() throws Exception {
    System.setProperty(LensConfConstants.CONFIG_LOCATION, "target/test-classes/");
    LensServices.get().init(LensServerConf.getHiveConf());
    LensServices.get().start();
    service = LensServices.get().getService(LensEventService.NAME);
    assertNotNull(service);
    log.info("Service started {}", service);
  }

  /**
   * Test add listener.
   */
  @Test
  public void testAddListener() {
    int listenersBefore = service.getEventListeners().keySet().size();
    genericEventListener = new GenericEventListener();
    service.addListenerForType(genericEventListener, LensEvent.class);
    endedListener = new MockEndedListener();
    service.addListenerForType(endedListener, QueryEnded.class);
    failedListener = new MockFailedListener();
    service.addListenerForType(failedListener, QueryFailed.class);
    queuePositionChangeListener = new MockQueuePositionChange();
    service.addListenerForType(queuePositionChangeListener, QueuePositionChange.class);
    sessionOpenedListener = new MockerSessionOpened();
    service.addListenerForType(sessionOpenedListener, SessionOpened.class);
    sessionClosedListener = new MockerSessionClosed();
    service.addListenerForType(sessionClosedListener, SessionClosed.class);
    sessionExpiredListner = new MockerSessionExpired();
    service.addListenerForType(sessionExpiredListner, SessionExpired.class);
    sessionRestoredListener = new MockerSessionRestored();
    service.addListenerForType(sessionRestoredListener, SessionRestored.class);

    assertTrue(service.getListeners(LensEvent.class).contains(genericEventListener));
    assertTrue(service.getListeners(QueryFailed.class).contains(failedListener));
    assertTrue(service.getListeners(QueryEnded.class).contains(endedListener));
    assertTrue(service.getListeners(QueuePositionChange.class).contains(queuePositionChangeListener));
    assertTrue(service.getListeners(SessionOpened.class).contains(sessionOpenedListener));
    assertTrue(service.getListeners(SessionClosed.class).contains(sessionClosedListener));
  }

  /**
   * Test remove listener.
   */
  @Test
  public void testRemoveListener() {
    MockFailedListener toRemove = new MockFailedListener();
    service.addListenerForType(toRemove, QueryFailed.class);
    assertEquals(service.getListeners(QueryFailed.class).size(), 2);
    service.removeListener(toRemove);
    assertEquals(service.getListeners(QueryFailed.class).size(), 1);
  }

  /**
   * Reset listeners.
   */
  private void resetListeners() {
    genericEventListener.processed = false;
    endedListener.processed = false;
    failedListener.processed = false;
    queuePositionChangeListener.processed = false;
  }

  /**
   * Reset session listeners.
   */
  private void resetSessionListeners() {
    genericEventListener.processed = false;
    sessionOpenedListener.processed = false;
    sessionClosedListener.processed = false;
    sessionExpiredListner.processed = false;
    sessionRestoredListener.processed = false;
  }

  /**
   * Test handle event.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testSesionHandleEvent() throws Exception {
    LensSessionHandle sessionHandle = new LensSessionHandle(UUID.randomUUID(), UUID.randomUUID());
    String user = "user";
    long now = System.currentTimeMillis();
    SessionOpened sessionOpenedEvent = new SessionOpened(now, sessionHandle, user);
    SessionClosed sessionClosedEvent = new SessionClosed(now, sessionHandle);
    SessionRestored sessionRestored = new SessionRestored(now, sessionHandle);
    SessionExpired sessionExpired = new SessionExpired(now, sessionHandle);

    try {
      latch = new CountDownLatch(3);
      log.info("Sending session opened  event: {}", sessionOpenedEvent);
      service.notifyEvent(sessionOpenedEvent);
      log.info("Sending session restored event: {}", sessionRestored);
      service.notifyEvent(sessionRestored);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertTrue(sessionOpenedListener.processed);
      assertTrue(sessionRestoredListener.processed);
      resetSessionListeners();

      LensEvent genEvent = new LensEvent(now) {
        @Override
        public String getEventId() {
          return "TEST_EVENT";
        }
      };

      latch = new CountDownLatch(2);
      log.info("Sending generic event {}", genEvent.getEventId());
      service.notifyEvent(genEvent);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      resetSessionListeners();

      latch = new CountDownLatch(3);
      log.info("Sending session closed event {}", sessionClosedEvent);
      service.notifyEvent(sessionClosedEvent);
      log.info("Sending session expired event {}", sessionExpired);
      service.notifyEvent(sessionExpired);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(sessionClosedListener.processed);
      assertTrue(sessionExpiredListner.processed);
      assertFalse(sessionOpenedListener.processed);
      assertFalse(sessionRestoredListener.processed);

    } catch (LensException e) {
      fail(e.getMessage());
    }
  }

  /**
   * Test handle event.
   *
   * @throws Exception the exception
   */
  @Test
  public void testHandleEvent() throws Exception {
    QueryHandle query = new QueryHandle(UUID.randomUUID());
    String user = "user";
    long now = System.currentTimeMillis();
    QueryFailed failed
      = new QueryFailed(null, now, QueryStatus.Status.RUNNING, QueryStatus.Status.FAILED, query, user, null);
    QuerySuccess success
      = new QuerySuccess(null, now, QueryStatus.Status.RUNNING, QueryStatus.Status.SUCCESSFUL, query);
    QueuePositionChange positionChange = new QueuePositionChange(now, 1, 0, query);

    try {
      latch = new CountDownLatch(3);
      log.info("Sending event: {}", failed);
      service.notifyEvent(failed);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertTrue(endedListener.processed);
      assertTrue(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      latch = new CountDownLatch(2);
      log.info("Sending event : {}", success);
      service.notifyEvent(success);
      latch.await(5, TimeUnit.SECONDS);
      assertTrue(genericEventListener.processed);
      assertTrue(endedListener.processed);
      assertFalse(failedListener.processed);
      assertFalse(queuePositionChangeListener.processed);
      resetListeners();

      latch = new CountDownLatch(2);
      log.info("Sending event: {}", positionChange);
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
      log.info("Sending generic event {}", genEvent.getEventId());
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

  @Test
  public void testQueryStausLogger() throws Exception {
    System.out.println("@@@ testQueryStatusLogger");
    QueryStatusLogger logger = new QueryStatusLogger();
    service.addListenerForType(logger, StatusChange.class);

    // Catch all listener just to make sure that the query accepted and
    // query exec stat events get through
    final CountDownLatch latch = new CountDownLatch(2);
    LensEventListener<LensEvent> eventListener = queryEventListener(latch);
    service.addListenerForType(eventListener, LensEvent.class);

    QueryHandle queryHandle = new QueryHandle(UUID.randomUUID());
    QueryAccepted queryAccepted = new QueryAccepted(System.currentTimeMillis(), "beforeAccept", "afterAccept",
      queryHandle);

    QueryExecutionStatistics queryExecStats = new QueryExecutionStatistics(System.currentTimeMillis());

    service.notifyEvent(queryAccepted);
    service.notifyEvent(queryExecStats);

    latch.await();
    service.removeListener(eventListener);
  }

  private LensEventListener<LensEvent> queryEventListener(final CountDownLatch latch) {
    return new LensEventListener<LensEvent>() {
      @Override
      public void onEvent(LensEvent event) throws LensException {
        System.out.println("@@@@ Got Event: Type= " + event.getClass().getName() + " obj = " + event);
        latch.countDown();
      }
    };
  }

  @Test
  public void testAysncEventListenerPoolThreads(){
    AsyncEventListener<QuerySuccess> ayncListener = new DummyAsncEventListener();
    for(int i=0; i<10; i++){
      try {
        //A pool thread is created each time an event is submitted until core pool size is reached which is 5
        //for this test case.  @see org.apache.lens.server.api.events.AsyncEventListener.processor
        ayncListener.onEvent(null);
      } catch (LensException e) {
        assert(false); //Not Expected
      }
    }

    //Verify the core pool Threads after the events have been fired
    ThreadGroup currentTG = Thread.currentThread().getThreadGroup();
    int count = currentTG.activeCount();
    Thread[] threads = new Thread[count];
    currentTG.enumerate(threads);
    Set<String> aysncThreadNames = new HashSet<String>();
    for(Thread t : threads){
      if (t.getName().contains("DummyAsncEventListener_AsyncThread")){
        aysncThreadNames.add(t.getName());
      }
    }
    assertTrue(aysncThreadNames.containsAll(Arrays.asList(
      "DummyAsncEventListener_AsyncThread-1",
      "DummyAsncEventListener_AsyncThread-2",
      "DummyAsncEventListener_AsyncThread-3",
      "DummyAsncEventListener_AsyncThread-4",
      "DummyAsncEventListener_AsyncThread-5")));
  }

  /**
   * Test synchronous events
   * @throws Exception
   */
  @Test
  public void testNotifySync() throws Exception {
    service.addListenerForType(new TestEventHandler(), TestEvent.class);
    TestEvent testEvent = new TestEvent("ID");
    service.notifyEventSync(testEvent);
    assertTrue(testEvent.processed);
  }

  private static class TestEvent extends LensEvent{
    String id;
    boolean processed = false;
    public TestEvent(String id) {
      super(System.currentTimeMillis());
      this.id = id;
    }
    @Override
    public String getEventId() {
      return id;
    }
  }

  private static class TestEventHandler implements LensEventListener<TestEvent> {

    @Override
    public void onEvent(TestEvent event) throws LensException {
      event.processed = true;
    }
  }
  private static class DummyAsncEventListener extends AsyncEventListener<QuerySuccess> {
    public DummyAsncEventListener(){
      super(5); //core pool = 5
    }
    @Override
    public void process(QuerySuccess event) {
      throw new RuntimeException("Simulated Exception");
    }
  }
}
