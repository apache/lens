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
package org.apache.lens.server.api.metrics;


import static com.codahale.metrics.MetricRegistry.name;

import java.util.HashMap;
import java.util.Map;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.model.LensContainerRequest;
import org.apache.lens.server.model.LensResourceMethod;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.ResourceMethod;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import lombok.Getter;
import lombok.NonNull;

/**
 * Factory for creating MethodMetrics objects on demand.
 */
public class MethodMetricsFactory {
  public static final Logger LOG = Logger.getLogger(MethodMetricsFactory.class);

  private final MetricRegistry metricRegistry;

  public MethodMetricsFactory(@NonNull MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  /** factory helper */
  @Getter
  private volatile Map<String, MethodMetrics> methodMetricsMap = new HashMap<String, MethodMetrics>();

  /**
   * This is a factory method for getting a MethodMetrics instance. First a unique name is determined for the arguments
   * and then function with same name is called with that unique name.
   *
   * @param method
   * @param containerRequest
   * @return
   * @see #get(String)
   * @see #getUniqueName(org.glassfish.jersey.server.model.ResourceMethod, org.glassfish.jersey.server.ContainerRequest)
   */
  public MethodMetrics get(@NonNull ResourceMethod method, @NonNull ContainerRequest containerRequest) {
    return get(getUniqueName(method, containerRequest));
  }

  /**
   * Returns MethodMetrics object corresponding to the given name. If doesn't exist yet, one will be created.
   *
   * @param name
   * @return
   */
  public MethodMetrics get(@NonNull final String name) {
    //SUSPEND CHECKSTYLE CHECK DoubleCheckedLockingCheck
    MethodMetrics result = methodMetricsMap.get(name);
    if (result == null) {
      synchronized (this) {
        result = methodMetricsMap.get(name);
        LOG.info("Creating MethodMetrics of name: " + name);
        result = new MethodMetrics(
          metricRegistry.meter(name(name, "meter")),
          metricRegistry.timer(name(name, "timer")),
          metricRegistry.timer(name(name, "exception.timer")));
        methodMetricsMap.put(name, result);
      }
    }
    //RESUME CHECKSTYLE CHECK DoubleCheckedLockingCheck
    return result;
  }

  /**
   * Wrapper method. Creates Lens model wrappers: objects of {@link org.apache.lens.server.model.LensResourceMethod} and
   * {@link org.apache.lens.server.model.LensContainerRequest}. And calls function of same name with those objects as
   * arguments.
   *
   * @param method
   * @param containerRequest
   * @return unique name of MethodMetrics object to be returned by #get
   * @see #getUniqueName
   * @see #get(org.glassfish.jersey.server.model.ResourceMethod, org.glassfish.jersey.server.ContainerRequest)
   */
  private String getUniqueName(ResourceMethod method, ContainerRequest containerRequest) {
    return getUniqueName(new LensResourceMethod(method), new LensContainerRequest(containerRequest));
  }

  /**
   * Extracts base name from lensResourceMethod#name. Checks if the resource method was annotated with {@link
   * org.apache.lens.server.api.annotations.MultiPurposeResource}. If not, returns the base name If yes, extracts the
   * multi purpose form param from the resource method, gets the value of that param from lensContainerRequest object
   * and appends that to baseName. if value is not passed, gets the default value of that argument from
   * lensContainerRequest and appends that to baseName. Returns the final string constructed
   *
   * @param lensResourceMethod
   * @param lensContainerRequest
   * @return Unique name of the MethodMetrics object associated with given arguments.
   * @see org.apache.lens.server.model.LensResourceMethod#name()
   * @see org.apache.lens.server.api.annotations.MultiPurposeResource
   */
  private String getUniqueName(final LensResourceMethod lensResourceMethod, LensContainerRequest lensContainerRequest) {
    StringBuilder sb = new StringBuilder();
    sb.append(lensResourceMethod.name());
    final Optional<String> multiPurposeFormParam = lensResourceMethod.getMultiPurposeFormParam();
    if (multiPurposeFormParam.isPresent()) {
      sb.append(".");
      String value = lensContainerRequest.getFormDataFieldValue(multiPurposeFormParam.get()).or(
        new Supplier<String>() {
          @Override
          public String get() {
            return lensResourceMethod.getDefaultValueForParam(multiPurposeFormParam.get());
          }
        });
      sb.append(value.toUpperCase());
    }
    return sb.toString();
  }

  /**
   * Remove all meters/timers/... created when MethodMetrics objects were constructed using this factory.
   */
  public void clear() {
    synchronized (this) {
      LOG.info("clearing factory");
      for (Map.Entry<String, MethodMetrics> entry : methodMetricsMap.entrySet()) {
        metricRegistry.remove(name(entry.getKey(), "meter"));
        metricRegistry.remove(name(entry.getKey(), "timer"));
        metricRegistry.remove(name(entry.getKey(), "exception.timer"));
      }
      methodMetricsMap.clear();
    }
  }

  /**
   * Get query metric gauge name.
   *
   * @param conf
   * @param appendToStackName
   * @param gaugeSuffix
   * @return
   */
  public static MethodMetricsContext createMethodGauge(@NonNull Configuration conf, boolean appendToStackName,
    String gaugeSuffix) {
    String uid = conf.get(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY);
    if (StringUtils.isBlank(uid)) {
      return DisabledMethodMetricsContext.getInstance();
    }
    LOG.info("query metricid:" + uid);
    StringBuilder metricName = new StringBuilder();
    if (appendToStackName) {
      String stackName = conf.get(LensConfConstants.QUERY_METRIC_DRIVER_STACK_NAME);
      LOG.info("query metric stackname:" + stackName);
      metricName.append(stackName);
      metricName.append("-");
    } else {
      metricName.append(uid);
      metricName.append("-");
    }
    metricName.append(gaugeSuffix);
    String metricGaugeName = metricName.toString();
    MethodMetricGauge mg = new MethodMetricGauge(LensMetricsRegistry.getStaticRegistry(), metricGaugeName);
    return mg;
  }
}
