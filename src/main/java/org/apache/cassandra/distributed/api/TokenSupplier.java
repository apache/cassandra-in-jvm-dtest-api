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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface TokenSupplier
{
    Collection<String> tokens(int nodeId);

    @Deprecated
    default long token(int nodeId)
    {
        Collection<String> tokens = tokens(nodeId);
        assert tokens.size() == 1: "tokens function returned multiple tokens, only expected 1: " + tokens;
        return Long.parseLong(tokens.stream().findFirst().get());
    }

    @Deprecated
    static TokenSupplier evenlyDistributedTokens(int numNodes)
    {
        return evenlyDistributedTokens(numNodes, 1);
    }

    static TokenSupplier evenlyDistributedTokens(int numNodes, int numTokens)
    {
        long increment = (Long.MAX_VALUE / (numNodes * numTokens)) * 2;
        List<String>[] tokens = new List[numNodes];
        for (int i = 0; i < numNodes; i++)
            tokens[i] = new ArrayList<>(numTokens);

        long value = Long.MIN_VALUE + 1;
        for (int i = 0; i < numTokens; i++)
        {
            for (int nodeId = 1; nodeId <= numNodes; nodeId++)
            {
                value += increment;
                tokens[nodeId - 1].add(Long.toString(value));
            }
        }
        return (int nodeId) -> tokens[nodeId - 1];
    }
}
