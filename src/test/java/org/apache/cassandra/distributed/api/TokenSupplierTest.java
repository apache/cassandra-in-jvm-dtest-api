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

import org.junit.jupiter.api.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class TokenSupplierTest {
    @Test
    public void evenlyDistributedTokens() {
        Gen<Integer> nodeGen = SourceDSL.integers().between(1, 100);
        Gen<Integer> tokenGen = SourceDSL.integers().between(1, 24);
        qt().forAll(nodeGen, tokenGen).checkAssert((numNodes, numTokens) -> {
            TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(numNodes, numTokens);
            SortedSet<Long> sortedTokens = new TreeSet<>();
            for (int i = 0; i < numNodes; i++) {
                Collection<String> tokens = ts.tokens(i + 1);
                assertThat(tokens).hasSize(numTokens);
                tokens.forEach(s -> sortedTokens.add(Long.valueOf(s)));
            }
            Long previous = null;
            List<Long> diff = new ArrayList<>(sortedTokens.size() - 1);
            for (Long token : sortedTokens) {
                if (previous != null)
                    diff.add(token - previous);
                previous = token;
            }

            assertThat(calculateSD(diff)).isLessThan(1_000);
        });
    }

    private static double calculateSD(Collection<Long> values)
    {
        if (values.isEmpty())
            return 0;
        double sum = 0.0;
        double sd = 0.0;

        for (double num : values)
            sum += num;

        double mean = sum / values.size();

        for (double num : values)
            sd += Math.pow(num - mean, 2);

        return Math.sqrt(sd / values.size());
    }
}