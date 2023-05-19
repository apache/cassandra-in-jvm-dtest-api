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
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.cassandra.distributed.api.IInstanceConfig;

public final class JMXUtil
{
    private JMXUtil()
    {
    }

    public static final String JMX_SERVICE_URL_FMT = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    public static JMXConnector getJmxConnector(IInstanceConfig config) {
        String jmxHost = getJmxHost(config);
        String url = String.format(JMX_SERVICE_URL_FMT, jmxHost, config.jmxPort());
        try
        {
            return JMXConnectorFactory.connect(new JMXServiceURL(url), null);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String getJmxHost(IInstanceConfig config) {
        return config.broadcastAddress().getAddress().getHostAddress();
    }

}
