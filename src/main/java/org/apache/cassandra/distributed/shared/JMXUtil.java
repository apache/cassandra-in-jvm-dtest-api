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

import java.io.IOException;
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

    public static JMXConnector getJmxConnector(IInstanceConfig config) {
        return getJmxConnector(config, 5);
    }

    public static JMXConnector getJmxConnector(IInstanceConfig config, int numAttempts) {
        String jmxHost = getJmxHost(config);
        String url = String.format(JMX_SERVICE_URL_FMT, jmxHost, config.jmxPort());
        int attempts = 0;
        Throwable lastThrown = null;
        while (attempts < numAttempts)
        {
            attempts++;
            try
            {
                JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url), null);

                LOGGER.info("Connected to JMX server at {} after {} attempt(s)",
                            url, attempts);
                return connector;
            }

            catch(MalformedURLException e)
            {
                throw new RuntimeException(e);
            }

            catch (Throwable thrown)
            {
                lastThrown = thrown;
            }
            LOGGER.info("Could not connect to JMX on {} after {} attempts. Will retry.", url, attempts);
            Uninterruptables.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        throw new RuntimeException("Could not start JMX - unreachable after 20 attempts", lastThrown);
    }

    public static String getJmxHost(IInstanceConfig config) {
        return config.broadcastAddress().getAddress().getHostAddress();
    }
}
