/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.shared;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public interface Metrics
{
    List<String> getNames();

    long getCounter(String name);
    Map<String, Long> getCounters(Predicate<String> filter);

    double getHistogram(String name, MetricValue value);
    Map<String, Double> getHistograms(Predicate<String> filter, MetricValue value);

    Object getGauge(String name);
    Map<String, Object> getGauges(Predicate<String> filter);

    double getMeter(String name, Rate value);
    Map<String, Double> getMeters(Predicate<String> filter, Rate value);

    double getTimer(String name, MetricValue value);
    Map<String, Double> getTimers(Predicate<String> filter, MetricValue value);

    enum MetricValue
    {
        COUNT,
        MEDIAN, P75, P95, P98, P99, P999,
        MAX, MEAN, MIN, STDDEV
    }

    enum Rate
    {
        RATE15_MIN,
        RATE5_MIN,
        RATE1_MIN,
        RATE_MEAN
    }
}
