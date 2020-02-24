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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NetworkTopology
{
    private final Map<AddressAndPort, DcAndRack> map;

    public static class DcAndRack
    {
        private final String dc;
        private final String rack;

        private DcAndRack(String dc, String rack)
        {
            this.dc = dc;
            this.rack = rack;
        }

        @Override
        public String toString()
        {
            return "DcAndRack{" +
                   "dc='" + dc + '\'' +
                   ", rack='" + rack + '\'' +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DcAndRack dcAndRack = (DcAndRack) o;
            return Objects.equals(dc, dcAndRack.dc) &&
                   Objects.equals(rack, dcAndRack.rack);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(dc, rack);
        }
    }

    public static class AddressAndPort implements Serializable
    {
        private final InetAddress address;
        private final int port;

        public AddressAndPort(InetAddress address, int port)
        {
            this.address = address;
            this.port = port;
        }

        public int getPort()
        {
            return port;
        }

        public InetAddress getAddress()
        {
            return address;
        }

        @Override
        public String toString()
        {
            return "AddressAndPort{" +
                   "address=" + address +
                   ", port=" + port +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AddressAndPort that = (AddressAndPort) o;
            return port == that.port &&
                   Objects.equals(address, that.address);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(address, port);
        }
    }

    public static DcAndRack dcAndRack(String dc, String rack)
    {
        return new DcAndRack(dc, rack);
    }

    public static AddressAndPort addressAndPort(InetAddress address, int port)
    {
        return new AddressAndPort(address, port);
    }

    public static AddressAndPort addressAndPort(String address, int port)
    {
        try
        {
            return new AddressAndPort(InetAddress.getByName(address), port);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException("Unknown address '" + address + '\'');
        }
    }

    private NetworkTopology()
    {
        map = new HashMap<>();
    }

    @SuppressWarnings("WeakerAccess")
    public NetworkTopology(NetworkTopology networkTopology)
    {
        map = new HashMap<>(networkTopology.map);
    }

    public static NetworkTopology build(String ipPrefix, int broadcastPort, Map<Integer, DcAndRack> nodeIdTopology)
    {
        final NetworkTopology topology = new NetworkTopology();

        for (int nodeId = 1; nodeId <= nodeIdTopology.size(); nodeId++)
        {
            String broadcastAddress = ipPrefix + nodeId;

            DcAndRack dcAndRack = nodeIdTopology.get(nodeId);
            if (dcAndRack == null)
                throw new IllegalStateException("nodeId " + nodeId + "not found in instanceMap");

            topology.put(addressAndPort(broadcastAddress, broadcastPort), dcAndRack);
        }
        return topology;
    }

    public DcAndRack put(AddressAndPort addressAndPort, DcAndRack value)
    {
        return map.put(addressAndPort, value);
    }

    public String localRack(NetworkTopology.AddressAndPort key)
    {
        DcAndRack p = map.get(key);
        if (p == null)
            return null;
        return p.rack;
    }

    public String localDC(NetworkTopology.AddressAndPort key)
    {
        DcAndRack p = map.get(key);
        if (p == null)
            return null;
        return p.dc;
    }

    public boolean contains(NetworkTopology.AddressAndPort key)
    {
        return map.containsKey(key);
    }

    public String toString()
    {
        return "NetworkTopology{" + map + '}';
    }


    public static Map<Integer, NetworkTopology.DcAndRack> singleDcNetworkTopology(int nodeCount,
                                                                                  String dc,
                                                                                  String rack)
    {
        return networkTopology(nodeCount, (nodeid) -> NetworkTopology.dcAndRack(dc, rack));
    }

    public static Map<Integer, NetworkTopology.DcAndRack> networkTopology(int nodeCount,
                                                                          IntFunction<DcAndRack> dcAndRackSupplier)
    {

        return IntStream.rangeClosed(1, nodeCount).boxed()
                        .collect(Collectors.toMap(nodeId -> nodeId,
                                                  dcAndRackSupplier::apply));
    }
}