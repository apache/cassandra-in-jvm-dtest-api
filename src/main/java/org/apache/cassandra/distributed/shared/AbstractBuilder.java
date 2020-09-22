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
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    private int broadcastPort = 7012;
    private BiConsumer<ClassLoader, Integer> instanceInitializer = (cl, id) -> {};
    private int datadirCount = 3;
    private final List<Rack> racks = new ArrayList<>();
    private boolean finalised;

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

    public int getBroadcastPort() {
        return broadcastPort;
    }

    public BiConsumer<ClassLoader, Integer> getInstanceInitializer()
    {
        return instanceInitializer;
    }

    public int getDatadirCount()
    {
        return datadirCount;
    }

    public C start() throws IOException
    {
        C cluster = createWithoutStarting();
        cluster.startup();
        return cluster;
    }

    public C createWithoutStarting() throws IOException
    {
        finaliseBuilder();
        if (root == null)
            root = Files.createTempDirectory("dtests").toFile();

        root.mkdirs();

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

    public B withBroadcastPort(int broadcastPort) {
        this.broadcastPort = broadcastPort;
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

    /**
     * Start this many nodes initially
     *
     * Note that when using this in combination with withNodeIdTopology or withRacks/withDCs/... we
     * might reduce the actual number of nodes if there are not enough nodes configured in the node id
     * topology. For tests where additional nodes are started after the initial ones, it is ok to have
     * nodeCount < node id topology size
     */
    public B withNodes(int nodeCount)
    {
        this.nodeCount = nodeCount;
        return (B) this;
    }

    /**
     * Adds dcCount datacenters, splits the nodeCount over these dcs
     */
    public B withDCs(int dcCount)
    {
        return withRacks(dcCount, 1);
    }

    /**
     * Adds this many racks per datacenter
     *
     * splits nodeCount over these racks/dcs
     *
     */
    public B withRacks(int dcCount, int racksPerDC)
    {
        assert dcCount > 0 && racksPerDC > 0 : "Both dcCount and racksPerDC must be > 0";

        for (int dc = 1; dc <= dcCount; dc++)
            for (int rack = 1; rack <= racksPerDC; rack++)
                withRack(dcName(dc), rackName(rack), -1);
        return (B) this;
    }

    /**
     * Creates a cluster with dcCount datacenters, racksPerDC racks in each dc and nodesPerRack nodes in each rack
     *
     * Note that node count must be >= dcCount * datacenters * racksPerDC, if it is smaller it will get adjusted up.
     */
    public B withRacks(int dcCount, int racksPerDC, int nodesPerRack)
    {
        assert dcCount > 0 && racksPerDC > 0 && nodesPerRack > 0 : "dcCount, racksPerDC and nodesPerRack must be > 0";
        for (int dc = 1; dc <= dcCount; dc++)
            for (int rack = 1; rack <= racksPerDC; rack++)
                withRack(dcName(dc), rackName(rack), nodesPerRack);
        return (B) this;
    }

    /**
     * Add a dc with name dcName containing a single rack with nodeCount nodes
     */
    public B withDC(String dcName, int nodeCount)
    {
        return withRack(dcName, rackName(1), nodeCount);
    }

    /**
     * Add a rack in dcName with name rackName containing nodesInRack nodes
     */
    public B withRack(String dcName, String rackName, int nodesInRack)
    {
        racks.add(new Rack(dcName, rackName, nodesInRack));
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

    public B withInstanceInitializer(BiConsumer<ClassLoader, Integer> instanceInitializer)
    {
        this.instanceInitializer = instanceInitializer;
        return (B) this;
    }

    public B withDataDirCount(int datadirCount)
    {
        assert datadirCount > 0 : "data dir count requires a positive number but given " + datadirCount;
        this.datadirCount = datadirCount;
        return (B) this;
    }

    private void finaliseBuilder()
    {
        if (finalised)
            return;
        finalised = true;

        if (!racks.isEmpty())
        {
            setRacks();
        }
        else if (nodeIdTopology != null)
        {
            if (nodeIdTopology.size() < nodeCount)
            {
                System.out.println("Adjusting node count since nodeIdTopology contains fewer nodes");
                nodeCount = nodeIdTopology.size();
            }
            else if (nodeIdTopology.size() > nodeCount)
            {
                if (nodeCount == 0)
                    nodeCount = nodeIdTopology.size();
                else
                    System.out.printf("nodeIdTopology configured for %d nodes while nodeCount is %d%n", nodeIdTopology.size(), nodeCount);
            }
        }
        else
        {
            nodeIdTopology = IntStream.rangeClosed(1, nodeCount).boxed()
                                      .collect(Collectors.toMap(nodeId -> nodeId,
                                                                nodeId -> NetworkTopology.dcAndRack(dcName(0), rackName(0))));
        }

        if (nodeCount <= 0)
            throw new IllegalStateException("Cluster must have at least one node");

        System.out.println("Node id topology:");
        for (int i = 1; i <= nodeIdTopology.size(); i++)
        {
            NetworkTopology.DcAndRack dcAndRack = nodeIdTopology.get(i);
            System.out.printf("node %d: dc = %s, rack = %s%n", i, dcAndRack.dc, dcAndRack.rack);
        }
        System.out.printf("Configured node count: %d, nodeIdTopology size: %d%n", nodeCount, nodeIdTopology.size());
    }

    private void setRacks()
    {
        if (nodeIdTopology == null)
            nodeIdTopology = new HashMap<>();

        boolean shouldCalculatePerRackCount = false;
        boolean hasExplicitPerRackCount = false;
        for (Rack rack : racks)
        {
            if (rack.rackNodeCount == -1)
                shouldCalculatePerRackCount = true;
            else
                hasExplicitPerRackCount = true;
        }

        if (shouldCalculatePerRackCount && hasExplicitPerRackCount)
            throw new IllegalStateException("Can't mix explicit and implicit per rack counts");

        int nodeId = nodeIdTopology.isEmpty() ? 1 : Collections.max(nodeIdTopology.keySet()) + 1;
        if (shouldCalculatePerRackCount)
        {
            if (nodeCount == 0)
                throw new IllegalStateException("Node count must be set when not setting per rack counts");
            int totalRacks = racks.size();
            int nodesPerRack = (nodeCount + totalRacks - 1) / totalRacks;

            for (Rack rack : racks)
                for (int i = 1; i <= nodesPerRack; i++)
                    nodeIdTopology.put(nodeId++, NetworkTopology.dcAndRack(rack.dcName, rack.rackName));
        }
        else
        {
            for (Rack rack : racks)
                for (int i = 1; i <= rack.rackNodeCount; i++)
                    nodeIdTopology.put(nodeId++, NetworkTopology.dcAndRack(rack.dcName, rack.rackName));
        }

        if (nodeCount != nodeIdTopology.size())
        {
            assert nodeIdTopology.size() > nodeCount : "withRacks should only ever increase the node count";
            if (nodeCount == 0)
                nodeCount =  nodeIdTopology.size();
            else
                System.out.printf("Network topology of %s requires more nodes, only starting %s out of %s configured nodes%n", nodeIdTopology, nodeCount, nodeIdTopology.size());
        }
    }

    static String dcName(int index)
    {
        return "datacenter" + index;
    }

    static String rackName(int index)
    {
        return "rack" + index;
    }

    private static class Rack
    {
        final String dcName;
        final String rackName;
        final int rackNodeCount;

        private Rack(String dcName, String rackName, int rackNodeCount)
        {
            this.dcName = dcName;
            this.rackName = rackName;
            this.rackNodeCount = rackNodeCount;
        }
    }
}


