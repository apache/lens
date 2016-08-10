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
package org.apache.lens.api.scheduler;

import javax.xml.bind.annotation.*;

import org.apache.lens.api.error.InvalidStateTransitionException;

@XmlRootElement
public enum SchedulerJobState implements StateTransitioner<SchedulerJobState, SchedulerJobEvent> {
  // repeating same operation will return the same state to ensure idempotent behavior.
  NEW {
    @Override
    public SchedulerJobState nextTransition(SchedulerJobEvent event) throws InvalidStateTransitionException {
      switch (event) {
      case ON_SUBMIT:
        return this;
      case ON_SCHEDULE:
        return SchedulerJobState.SCHEDULED;
      case ON_EXPIRE:
        return SchedulerJobState.EXPIRED;
      case ON_DELETE:
        return SchedulerJobState.DELETED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  SCHEDULED {
    @Override
    public SchedulerJobState nextTransition(SchedulerJobEvent event) throws InvalidStateTransitionException {
      switch (event) {
      case ON_SCHEDULE:
        return this;
      case ON_SUSPEND:
        return SchedulerJobState.SUSPENDED;
      case ON_EXPIRE:
        return SchedulerJobState.EXPIRED;
      case ON_DELETE:
        return SchedulerJobState.DELETED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  SUSPENDED {
    @Override
    public SchedulerJobState nextTransition(SchedulerJobEvent event) throws InvalidStateTransitionException {
      switch (event) {
      case ON_SUSPEND:
        return this;
      case ON_RESUME:
        return SchedulerJobState.SCHEDULED;
      case ON_EXPIRE:
        return SchedulerJobState.EXPIRED;
      case ON_DELETE:
        return SchedulerJobState.DELETED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  EXPIRED {
    @Override
    public SchedulerJobState nextTransition(SchedulerJobEvent event) throws InvalidStateTransitionException {
      switch (event) {
      case ON_EXPIRE:
        return this;
      case ON_DELETE:
        return SchedulerJobState.DELETED;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  },

  DELETED {
    @Override
    public SchedulerJobState nextTransition(SchedulerJobEvent event) throws InvalidStateTransitionException {
      switch (event) {
      case ON_DELETE:
        return this;
      default:
        throw new InvalidStateTransitionException(
            "SchedulerJobEvent: " + event.name() + " is not a valid event for state: " + this.name());
      }
    }
  }
}
