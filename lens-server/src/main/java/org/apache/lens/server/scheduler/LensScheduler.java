/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lens.server.scheduler;

import org.apache.lens.api.scheduler.SchedulerJobHandle;
import org.apache.lens.api.scheduler.XJob;

/**
 * Core work engine for the Lens Scheduler.
 */
public final class LensScheduler {

  private static final LensScheduler INSTANCE = new LensScheduler();

  // private constructor to ensure single instance.
  private LensScheduler(){}

  /**
   *
   * @return the singleton instance of the scheduler.
   */
  public static LensScheduler get(){
    return INSTANCE;
  }

  public void schedule(SchedulerJobHandle handle, XJob job) {

  }



}
