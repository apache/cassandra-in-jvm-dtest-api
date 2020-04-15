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
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;

import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;

public abstract class AbstractBuilder<I extends IInstance, C extends ICluster, B extends AbstractBuilder<I, C, B>>
{
    public interface Factory<I extends IInstance, C extends ICluster, B extends AbstractBuilder<I, C, B>>
    {
        C newCluster(B builder);
    }

    private final Factory<I, C, B> factory;
    private int nodeCount;
    private int subnet;
    private Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology;
    private TokenSupplier tokenSupplier;
    private File root;
    private Versions.Version version;
    private Consumer<IInstanceConfig> configUpdater;
    private ClassLoader sharedClassLoader = Thread.currentThread().getContextClassLoader();

    public AbstractBuilder(Factory<I, C, B> factory)
    {
        this.factory = factory;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getSubnet() {
        return subnet;
    }

    public Map<Integer, NetworkTopology.DcAndRack> getNodeIdTopology() {
        return nodeIdTopology;
    }

    public TokenSupplier getTokenSupplier() {
        return tokenSupplier;
    }

    public File getRoot() {
        return root;
    }

    public Versions.Version getVersion() {
        return version;
    }

    public Consumer<IInstanceConfig> getConfigUpdater() {
        return configUpdater;
    }

    public ClassLoader getSharedClassLoader() {
        return sharedClassLoader;
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

        root.mkdirs();

        if (nodeIdTopology == null)
            nodeIdTopology = IntStream.rangeClosed(1, nodeCount).boxed()
                                      .collect(Collectors.toMap(nodeId -> nodeId,
                                                                nodeId -> NetworkTopology.dcAndRack(dcName(0), rackName(0))));

        // TODO: make token allocation strategy configurable
        if (tokenSupplier == null)
            tokenSupplier = evenlyDistributedTokens(nodeCount);

        return factory.newCluster((B) this);
    }

    public B withSharedClassLoader(ClassLoader sharedClassLoader)
    {
        this.sharedClassLoader = Objects.requireNonNull(sharedClassLoader, "sharedClassLoader");
        return (B) this;
    }

    public B withTokenSupplier(TokenSupplier tokenSupplier)
    {
        this.tokenSupplier = tokenSupplier;
        return (B) this;
    }

    public B withSubnet(int subnet)
    {
        this.subnet = subnet;
        return (B) this;
    }

    public B withNodes(int nodeCount)
    {
        this.nodeCount = nodeCount;
        return (B) this;
    }

    public B withDCs(int dcCount)
    {
        return withRacks(dcCount, 1);
    }

    public B withRacks(int dcCount, int racksPerDC)
    {
        if (nodeCount == 0)
            throw new IllegalStateException("Node count will be calculated. Do not supply total node count in the builder");

        int totalRacks = dcCount * racksPerDC;
        int nodesPerRack = (nodeCount + totalRacks - 1) / totalRacks; // round up to next integer
        return withRacks(dcCount, racksPerDC, nodesPerRack);
    }

    public B withRacks(int dcCount, int racksPerDC, int nodesPerRack)
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
        return (B) this;
    }

    public B withDC(String dcName, int nodeCount)
    {
        return withRack(dcName, rackName(1), nodeCount);
    }

    public B withRack(String dcName, String rackName, int nodesInRack)
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
        return (B) this;
    }

    // Map of node ids to dc and rack - must be contiguous with an entry nodeId 1 to nodeCount
    public B withNodeIdTopology(Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology)
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

        return (B) this;
    }

    public B withRoot(File root)
    {
        this.root = root;
        return (B) this;
    }

    public B withVersion(Versions.Version version)
    {
        this.version = version;
        return (B) this;
    }

    public B withConfig(Consumer<IInstanceConfig> updater)
    {
        this.configUpdater = updater;
        return (B) this;
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


