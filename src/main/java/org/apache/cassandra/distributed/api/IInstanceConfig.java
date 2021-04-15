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

package org.apache.cassandra.distributed.api;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import com.vdurmont.semver4j.Semver;

import org.apache.cassandra.distributed.shared.NetworkTopology;

public interface IInstanceConfig
{
    IInstanceConfig with(Feature featureFlag);

    IInstanceConfig with(Feature... flags);

    int num();

    UUID hostId();

    InetSocketAddress broadcastAddress();

    NetworkTopology networkTopology();

    String localRack();

    String localDatacenter();

    /**
     * write the specified parameters to the Config object; we do not specify Config as the type to support a Config
     * from any ClassLoader; the implementation must not directly access any fields of the Object, or cast it, but
     * must use the reflection API to modify the state
     */
    void propagate(Object writeToConfig, Map<Class<?>, Function<Object, Object>> executor);

    /**
     * Validates whether the config properties are within range of accepted values.
     */
    void validate();

    IInstanceConfig set(String fieldName, Object value);

    default IInstanceConfig forceSet(String fieldName, Object value) { throw new UnsupportedOperationException(); }

    Object get(String fieldName);

    String getString(String fieldName);

    int getInt(String fieldName);

    boolean has(Feature featureFlag);

    IInstanceConfig forVersion(Semver series);

    Map<String, Object> getParams();

    public static class ParameterizedClass
    {
        public static final String CLASS_NAME = "class_name";
        public static final String PARAMETERS = "parameters";

        public String class_name;
        public Map<String, String> parameters;

        public ParameterizedClass(String class_name, Map<String, String> parameters)
        {
            this.class_name = class_name;
            this.parameters = parameters;
        }

        @SuppressWarnings("unchecked")
        public ParameterizedClass(Map<String, ?> p)
        {
            this((String) p.get(CLASS_NAME),
                 p.containsKey(PARAMETERS) ? (Map<String, String>) ((List<?>) p.get(PARAMETERS)).get(0) : null);
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof ParameterizedClass && equals((ParameterizedClass) that);
        }

        public boolean equals(ParameterizedClass that)
        {
            return Objects.equals(class_name, that.class_name) && Objects.equals(parameters, that.parameters);
        }

        @Override
        public String toString()
        {
            return class_name + (parameters == null ? "" : parameters.toString());
        }
    }
}
