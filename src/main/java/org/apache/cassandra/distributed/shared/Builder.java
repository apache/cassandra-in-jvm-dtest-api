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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;

import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;

public abstract class Builder<I extends IInstance, C extends ICluster>
{

    private final int BROADCAST_PORT = 7012;

    public interface Factory<I extends IInstance, C extends ICluster>
    {
        C newCluster(File root, Versions.Version version, List<IInstanceConfig> configs, ClassLoader sharedClassLoader);
    }

    private final Factory<I, C> factory;
    private int nodeCount;
    private int subnet;
    private Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology;
    private TokenSupplier tokenSupplier;
    private File root;
    private Versions.Version version;
    private Consumer<IInstanceConfig> configUpdater;

    public Builder(Factory<I, C> factory)
    {
        this.factory = factory;
    }

    public C start() throws IOException
    {
        C cluster = createWithoutStarting();
        cluster.startup();
        return cluster;
    }

    public C createWithoutStarting() throws IOException
    {
        if (root == null)
            root = Files.createTempDirectory("dtests").toFile();

        if (nodeCount <= 0)
            throw new IllegalStateException("Cluster must have at least one node");

        if (nodeIdTopology == null)
        {
            nodeIdTopology = IntStream.rangeClosed(1, nodeCount).boxed()
                                      .collect(Collectors.toMap(nodeId -> nodeId,
                                                                nodeId -> NetworkTopology.dcAndRack(dcName(0), rackName(0))));
        }

        root.mkdirs();

        ClassLoader sharedClassLoader = Thread.currentThread().getContextClassLoader();

        List<IInstanceConfig> configs = new ArrayList<>();

        // TODO: make token allocation strategy configurable
        if (tokenSupplier == null)
            tokenSupplier = evenlyDistributedTokens(nodeCount);

        for (int i = 0; i < nodeCount; ++i)
        {
            int nodeNum = i + 1;
            configs.add(createInstanceConfig(nodeNum));
        }

        return factory.newCluster(root, version, configs, sharedClassLoader);
    }

    public IInstanceConfig newInstanceConfig(C cluster)
    {
        return createInstanceConfig(cluster.size() + 1);
    }

    protected IInstanceConfig createInstanceConfig(int nodeNum)
    {
        String ipPrefix = "127.0." + subnet + ".";
        String seedIp = ipPrefix + "1";
        String ipAddress = ipPrefix + nodeNum;
        long token = tokenSupplier.token(nodeNum);

        NetworkTopology topology = NetworkTopology.build(ipPrefix, BROADCAST_PORT, nodeIdTopology);

        IInstanceConfig config = generateConfig(nodeNum, ipAddress, topology, root, String.valueOf(token), seedIp);
        if (configUpdater != null)
            configUpdater.accept(config);

        return config;
    }

    protected abstract IInstanceConfig generateConfig(int nodeNum, String ipAddress, NetworkTopology networkTopology, File root, String token, String seedIp);

    public Builder<I, C> withTokenSupplier(TokenSupplier tokenSupplier)
    {
        this.tokenSupplier = tokenSupplier;
        return this;
    }

    public Builder<I, C> withSubnet(int subnet)
    {
        this.subnet = subnet;
        return this;
    }

    public Builder<I, C> withNodes(int nodeCount)
    {
        this.nodeCount = nodeCount;
        return this;
    }

    public Builder<I, C> withDCs(int dcCount)
    {
        return withRacks(dcCount, 1);
    }

    public Builder<I, C> withRacks(int dcCount, int racksPerDC)
    {
        if (nodeCount == 0)
            throw new IllegalStateException("Node count will be calculated. Do not supply total node count in the builder");

        int totalRacks = dcCount * racksPerDC;
        int nodesPerRack = (nodeCount + totalRacks - 1) / totalRacks; // round up to next integer
        return withRacks(dcCount, racksPerDC, nodesPerRack);
    }

    public Builder<I, C> withRacks(int dcCount, int racksPerDC, int nodesPerRack)
    {
        if (nodeIdTopology != null)
            throw new IllegalStateException("Network topology already created. Call withDCs/withRacks once or before withDC/withRack calls");

        nodeIdTopology = new HashMap<>();
        int nodeId = 1;
        for (int dc = 1; dc <= dcCount; dc++)
        {
            for (int rack = 1; rack <= racksPerDC; rack++)
            {
                for (int rackNodeIdx = 0; rackNodeIdx < nodesPerRack; rackNodeIdx++)
                    nodeIdTopology.put(nodeId++, NetworkTopology.dcAndRack(dcName(dc), rackName(rack)));
            }
        }
        // adjust the node count to match the allocatation
        final int adjustedNodeCount = dcCount * racksPerDC * nodesPerRack;
        if (adjustedNodeCount != nodeCount)
        {
            assert adjustedNodeCount > nodeCount : "withRacks should only ever increase the node count";
            System.out.println(String.format("Network topology of %s DCs with %s racks per DC and %s nodes per rack required increasing total nodes to %s",
                                             dcCount, racksPerDC, nodesPerRack, adjustedNodeCount));
            nodeCount = adjustedNodeCount;
        }
        return this;
    }

    public Builder<I, C> withDC(String dcName, int nodeCount)
    {
        return withRack(dcName, rackName(1), nodeCount);
    }

    public Builder<I, C> withRack(String dcName, String rackName, int nodesInRack)
    {
        if (nodeIdTopology == null)
        {
            if (nodeCount > 0)
                throw new IllegalStateException("Node count must not be explicitly set, or allocated using withDCs/withRacks");

            nodeIdTopology = new HashMap<>();
        }
        for (int nodeId = nodeCount + 1; nodeId <= nodeCount + nodesInRack; nodeId++)
            nodeIdTopology.put(nodeId, NetworkTopology.dcAndRack(dcName, rackName));

        nodeCount += nodesInRack;
        return this;
    }

    // Map of node ids to dc and rack - must be contiguous with an entry nodeId 1 to nodeCount
    public Builder<I, C> withNodeIdTopology(Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology)
    {
        if (nodeIdTopology.isEmpty())
            throw new IllegalStateException("Topology is empty. It must have an entry for every nodeId.");

        IntStream.rangeClosed(1, nodeIdTopology.size()).forEach(nodeId -> {
            if (nodeIdTopology.get(nodeId) == null)
                throw new IllegalStateException("Topology is missing entry for nodeId " + nodeId);
        });

        if (nodeCount != nodeIdTopology.size())
        {
            nodeCount = nodeIdTopology.size();
            System.out.println(String.format("Adjusting node count to %s for supplied network topology", nodeCount));
        }

        this.nodeIdTopology = new HashMap<>(nodeIdTopology);

        return this;
    }

    public Builder<I, C> withRoot(File root)
    {
        this.root = root;
        return this;
    }

    public Builder<I, C> withVersion(Versions.Version version)
    {
        this.version = version;
        return this;
    }

    public Builder<I, C> withConfig(Consumer<IInstanceConfig> updater)
    {
        this.configUpdater = updater;
        return this;
    }

    static String dcName(int index)
    {
        return "datacenter" + index;
    }

    static String rackName(int index)
    {
        return "rack" + index;
    }
}


