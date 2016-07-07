/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.scheduler.state;

import org.apache.lens.api.scheduler.SchedulerJobStatus;
import org.apache.lens.server.api.error.InvalidStateTransitionException;
import org.apache.lens.server.api.scheduler.StateMachine;

import lombok.Getter;
import lombok.Setter;

/**
 * This class represents current state of a SchedulerJob and provides helper methods
 * for handling different events and lifecycle transition for a SchedulerJob.
 */
public class SchedulerJobState {

  public SchedulerJobState(SchedulerJobStatus status) {
    this.currentStatus = status;
  }
  public SchedulerJobState() {
    this.currentStatus = INITIAL_STATUS;
  }

  @Getter @Setter
  private SchedulerJobStatus currentStatus;

  private static final SchedulerJobStatus INITIAL_STATUS = SchedulerJobStatus.NEW;

  public SchedulerJobStatus nextTransition(EVENT event) throws InvalidStateTransitionException {
    STATE currentState = STATE.valueOf(currentStatus.name());
    return SchedulerJobStatus.valueOf(currentState.nextTransition(event).name());
  }

  private enum STATE implements StateMachine<STATE, EVENT> {
    // repeating same operation will return the same state to ensure idempotent behavior.
    NEW {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_SUBMIT:
          return this;
        case ON_SCHEDULE:
          return STATE.SCHEDULED;
        case ON_EXPIRE:
          return STATE.EXPIRED;
        case ON_DELETE:
          return STATE.DELETED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    SCHEDULED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_SCHEDULE:
          return this;
        case ON_SUSPEND:
          return STATE.SUSPENDED;
        case ON_EXPIRE:
          return STATE.EXPIRED;
        case ON_DELETE:
          return STATE.DELETED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    SUSPENDED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_SUSPEND:
          return this;
        case ON_RESUME:
          return STATE.SCHEDULED;
        case ON_EXPIRE:
          return STATE.EXPIRED;
        case ON_DELETE:
          return STATE.DELETED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    EXPIRED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_EXPIRE:
          return this;
        case ON_DELETE:
          return STATE.DELETED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    DELETED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_DELETE:
          return this;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    }
  }

  /**
   * All events(actions) which can happen on a Scheduler Job.
   */
  public enum EVENT {
    ON_SUBMIT,
    ON_SCHEDULE,
    ON_SUSPEND,
    ON_RESUME,
    ON_EXPIRE,
    ON_DELETE
  }
}
