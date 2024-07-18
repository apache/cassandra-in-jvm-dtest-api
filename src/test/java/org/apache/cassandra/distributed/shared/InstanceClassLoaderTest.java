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

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InstanceClassLoaderTest
{

    private static final int NUM_LOADERS = 10;
    private final List<InstanceClassLoader> loaders = new ArrayList<>();

    @Test
    public void testRefCountZeroWhenNoneCreated()
    {
        assertThat(InstanceClassLoader.getApproximateLiveLoaderCount(true)).isEqualTo(0);
    }

    @Test
    public void testRefCountWhenInstancesCreatedAndStillOnHeap()
    {
        createClassLoadersAndAssertCount();
    }

    @Test
    public void testRefCountAfterInstancesCanBeGced()
    {
        createClassLoadersAndAssertCount();
        loaders.clear();
        assertThat(waitForZeroClassLoaders(10)).isTrue();
    }

    @BeforeEach
    public void ensureNoClassLoadersOnStart()
    {
        assertThat(waitForZeroClassLoaders(50)).isTrue();
    }

    public boolean waitForZeroClassLoaders(int gcAttempts)
    {
        int i = 0;
        while (InstanceClassLoader.getApproximateLiveLoaderCount(true) > 0 && i++ < gcAttempts)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        return InstanceClassLoader.getApproximateLiveLoaderCount(false) == 0;
    }

    private void createClassLoadersAndAssertCount()
    {
        for (int i = 0; i < NUM_LOADERS; i++)
        {
            loaders.add(new InstanceClassLoader(0, 0, new URL[0], this.getClass().getClassLoader()));
        }
        assertThat(InstanceClassLoader.getApproximateLiveLoaderCount(true)).isEqualTo(NUM_LOADERS);
    }
}
