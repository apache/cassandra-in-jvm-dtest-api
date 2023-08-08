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

import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.IInstanceConfig;

public final class JMXUtil
{
    private JMXUtil()
    {
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(JMXUtil.class);

    public static final String JMX_SERVICE_URL_FMT = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    /**
     * Create an instance of a {@link JMXConnector} to an in-jvm instance based on the input configuration.
     * This overload uses 5 as the default number of retries which has been shown to be adequate in testing,
     * and passes a null environment map to the connect call.
     * @param config The instance configuration to use to get the necessary parameters to connect
     * @return A JMXConnector instance which can communicate with the specified instance via JMX
     */
    public static JMXConnector getJmxConnector(IInstanceConfig config) {
        return getJmxConnector(config, 5, null);
    }

    /**
     * Create an instance of a {@link JMXConnector} to an in-jvm instance based on the input configuration.
     * This overload uses 5 as the default number of retries which has been shown to be adequate in testing.
     * @param config The instance configuration to use to get the necessary parameters to connect
     * @param jmxEnv an optional map which specifies the JMX environment to use. Can be null.
     * @return A JMXConnector instance which can communicate with the specified instance via JMX
     */
    public static JMXConnector getJmxConnector(IInstanceConfig config, Map<String, ?> jmxEnv) {
        return getJmxConnector(config, 5, jmxEnv);
    }


    /**
     * Create an instance of a {@link JMXConnector} to an in-jvm instance based on the input configuration
     * This overload passes a null environment map to the connect call.
     * @param config The instance configuration to use to get the necessary parameters to connect
     * @param numAttempts the number of retries to attempt before failing to connect.
     * @return A JMXConnector instance which can communicate with the specified instance via JMX
     */
    public static JMXConnector getJmxConnector(IInstanceConfig config, int numAttempts)
    {
        return getJmxConnector(config, numAttempts, null);
    }

    /**
     * Create an instance of a {@link JMXConnector} to an in-jvm instance based on the input configuration
     * @param config The instance configuration to use to get the necessary parameters to connect
     * @param numAttempts the number of retries to attempt before failing to connect.
     * @param jmxEnv an optional map which specifies the JMX environment to use. Can be null.
     * @return A JMXConnector instance which can communicate with the specified instance via JMX
     */
    public static JMXConnector getJmxConnector(IInstanceConfig config, int numAttempts, Map<String, ?> jmxEnv) {
        String jmxHost = getJmxHost(config);
        String url = String.format(JMX_SERVICE_URL_FMT, jmxHost, config.jmxPort());
        int attempts = 1;
        Throwable lastThrown = null;
        while (attempts <= numAttempts)
        {
            try
            {
                JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url), jmxEnv);

                LOGGER.info("Connected to JMX server at {} after {} attempt(s)",
                            url, attempts);
                return connector;
            }

            catch(MalformedURLException e)
            {
                // Because we are building the URL from the config, this should never happen.
                // This is unrecoverable, so just rethrow wrapped in RuntimeException
                throw new RuntimeException("Failed to connect to JXM server due to a malformed URL (Instance config must be broken?)", e);
            }

            catch (Throwable thrown)
            {
                lastThrown = thrown;
            }
            if (attempts < numAttempts)
            {
                LOGGER.info("Could not connect to JMX on {} after {} attempts. Will retry.", url, attempts);
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            attempts++;
        }
        String error = "Failed to connect to JMX, which was unreachable after " + attempts + " attempts.";
        LOGGER.error(error, lastThrown);
        throw new RuntimeException(error, lastThrown);
    }

    public static String getJmxHost(IInstanceConfig config) {
        return config.broadcastAddress().getAddress().getHostAddress();
    }
}
