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

import java.io.IOException;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.shared.AbstractBuilder;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class BuilderTest
{

    @Test
    public void testNodeCount() throws IOException
    {
        ICluster mockCluster = Mockito.mock(ICluster.class);
        AbstractBuilder<?,?,?> builder = new AbstractBuilder(builder1 -> mockCluster){};

        // empty case
        try
        {
            builder.createWithoutStarting();
        }
        catch (Throwable t)
        {
            assertThat(t.getMessage().contains("Cluster must have at least one node"))
            .isTrue();
        }

        // a single node
        builder = new AbstractBuilder(builder1 -> mockCluster){};
        builder.withNodes(1).createWithoutStarting();
        assertThat(builder.getNodeCount())
        .isEqualTo(1);
        assertThat(builder.getNodeIdTopology().size())
        .isEqualTo(1);

        // numNodes > number of nodes in racks
        builder = new AbstractBuilder(builder1 -> mockCluster){};
        builder.withNodes(20)
               .withRack("dc1", "rack1", 5)
               .withRack("dc1", "rack2", 5)
               .withRack("dc2", "rack1", 5);
        try
        {
            builder.createWithoutStarting();
        }
        catch (Throwable t)
        {
            assertThat(t.getMessage().contains("withRacks should only ever increase the node count"))
            .isTrue();
        }

        // numNodes < number of nodes in racks
        builder = new AbstractBuilder(builder1 -> mockCluster){};
        builder.withNodes(10)
               .withRack("dc1", "rack1", 5)
               .withRack("dc1", "rack2", 5)
               .withRack("dc2", "rack1", 5);
        builder.createWithoutStarting();
        assertThat(builder.getNodeCount())
        .isEqualTo(10);
        assertThat(builder.getNodeIdTopology().size())
        .isEqualTo(15);

        builder = new AbstractBuilder(builder1 -> mockCluster){};
        builder.withNodes(10)
               .withRack("dc1", "rack1", 5)
               .withRack("dc1", "rack2", 5)
               .withRack("dc2", "rack1", 5)
               .withNodeIdTopology(new HashMap<Integer, NetworkTopology.DcAndRack>() {{
                   for (int i = 0; i < 3; i++)
                       for (int j = 0; j < 3; j++)
                           put(i * 3 + j + 1, NetworkTopology.dcAndRack("dc" + (i + 1), "rack" + (j + 1)));
               }});
        builder.createWithoutStarting();
        assertThat(builder.getNodeCount())
        .isEqualTo(10);
        assertThat(builder.getNodeIdTopology().size())
        .isEqualTo(9 + 15); // both given topology and racks are applied
    }
}
