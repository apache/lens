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

import org.apache.lens.api.scheduler.SchedulerJobInstanceStatus;
import org.apache.lens.server.api.error.InvalidStateTransitionException;
import org.apache.lens.server.api.scheduler.StateMachine;

import lombok.Getter;
import lombok.Setter;

/**
 * State machine for transitions on Scheduler Jobs.
 */
public class SchedulerJobInstanceState {

  public SchedulerJobInstanceState(SchedulerJobInstanceStatus status) {
    this.currentStatus = status;
  }

  public SchedulerJobInstanceState() {
    this.currentStatus = INITIAL_STATUS;
  }

  @Getter @Setter
  private SchedulerJobInstanceStatus currentStatus;

  private static final SchedulerJobInstanceStatus INITIAL_STATUS = SchedulerJobInstanceStatus.WAITING;

  public SchedulerJobInstanceStatus nextTransition(EVENT event) throws InvalidStateTransitionException {
    STATE currentState = STATE.valueOf(currentStatus.name());
    return SchedulerJobInstanceStatus.valueOf(currentState.nextTransition(event).name());
  }

  public enum STATE implements StateMachine<STATE, EVENT> {
    // repeating same operation will return the same state to ensure idempotent behavior.
    WAITING {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_CREATION:
          return this;
        case ON_CONDITIONS_MET:
          return STATE.LAUNCHED;
        case ON_TIME_OUT:
          return STATE.TIMED_OUT;
        case ON_RUN:
          return STATE.RUNNING;
        case ON_SUCCESS:
          return STATE.SUCCEEDED;
        case ON_FAILURE:
          return STATE.FAILED;
        case ON_KILL:
          return STATE.KILLED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    LAUNCHED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_CONDITIONS_MET:
          return this;
        case ON_RUN:
          return STATE.RUNNING;
        case ON_SUCCESS:
          return STATE.SUCCEEDED;
        case ON_FAILURE:
          return STATE.FAILED;
        case ON_KILL:
          return STATE.KILLED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    RUNNING {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_RUN:
          return this;
        case ON_SUCCESS:
          return STATE.SUCCEEDED;
        case ON_FAILURE:
          return STATE.FAILED;
        case ON_KILL:
          return STATE.KILLED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    FAILED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_FAILURE:
          return this;
        case ON_RERUN:
          return STATE.LAUNCHED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    SUCCEEDED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_SUCCESS:
          return this;
        case ON_RERUN:
          return STATE.LAUNCHED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },


    TIMED_OUT {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_TIME_OUT:
          return this;
        case ON_RERUN:
          return STATE.WAITING;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    },

    KILLED {
      @Override
      public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        switch (event) {
        case ON_KILL:
          return this;
        case ON_RERUN:
          return STATE.LAUNCHED;
        default:
          throw new InvalidStateTransitionException("Event: " + event.name() + " is not a valid event for state: "
            + this.name());
        }
      }
    }
  }

  /**
   * All events(actions) which can happen on an instance of <code>SchedulerJob</code>.
   */
  public enum EVENT {
    ON_CREATION, // an instance is first considered by the scheduler.
    ON_TIME_OUT,
    ON_CONDITIONS_MET,
    ON_RUN,
    ON_SUCCESS,
    ON_FAILURE,
    ON_RERUN,
    ON_KILL
  }
}
