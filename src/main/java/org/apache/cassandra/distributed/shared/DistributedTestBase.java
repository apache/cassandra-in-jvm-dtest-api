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

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;

public abstract class DistributedTestBase
{
    public void afterEach()
    {
        System.runFinalization();
        System.gc();
    }

    public static void beforeClass() throws Throwable
    {
        ICluster.setup();
    }

    public abstract <I extends IInstance, C extends ICluster> Builder<I, C> builder();

    public static String KEYSPACE = "distributed_test_keyspace";

    public static String withKeyspace(String replaceIn)
    {
        return String.format(replaceIn, KEYSPACE);
    }

    protected static <C extends ICluster<?>> C init(C cluster)
    {
        return init(cluster, cluster.size());
    }

    protected static <C extends ICluster<?>> C init(C cluster, int replicationFactor)
    {
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "};");
        return cluster;
    }
}
