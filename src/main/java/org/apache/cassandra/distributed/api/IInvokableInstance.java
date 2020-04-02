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

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This version is only supported for a Cluster running the same code as the test environment, and permits
 * ergonomic cross-node behaviours, without editing the cross-version API.
 * <p>
 * A lambda can be written tto be invoked on any or all of the nodes.
 * <p>
 * The reason this cannot (easily) be made cross-version is that the lambda is tied to the declaring class, which will
 * not be the same in the alternate version.  Even were it not, there would likely be a runtime linkage error given
 * any code divergence.
 */
public interface IInvokableInstance extends IInstance
{
    default <O> CallableNoExcept<Future<O>> asyncCallsOnInstance(SerializableCallable<O> call) { return async(transfer(call)); }
    default <O> CallableNoExcept<O> callsOnInstance(SerializableCallable<O> call) { return sync(transfer(call)); }
    default <O> O callOnInstance(SerializableCallable<O> call) { return callsOnInstance(call).call(); }

    default CallableNoExcept<Future<?>> asyncRunsOnInstance(SerializableRunnable run) { return async(transfer(run)); }
    default Runnable runsOnInstance(SerializableRunnable run) { return sync(transfer(run)); }
    default void runOnInstance(SerializableRunnable run) { runsOnInstance(run).run(); }

    default <I> Function<I, Future<?>> asyncAcceptsOnInstance(SerializableConsumer<I> consumer) { return async(transfer(consumer)); }
    default <I> Consumer<I> acceptsOnInstance(SerializableConsumer<I> consumer) { return sync(transfer(consumer)); }

    default <I1, I2> BiFunction<I1, I2, Future<?>> asyncAcceptsOnInstance(SerializableBiConsumer<I1, I2> consumer) { return async(transfer(consumer)); }
    default <I1, I2> BiConsumer<I1, I2> acceptsOnInstance(SerializableBiConsumer<I1, I2> consumer) { return sync(transfer(consumer)); }

    default <I, O> Function<I, Future<O>> asyncAppliesOnInstance(SerializableFunction<I, O> f) { return async(transfer(f)); }
    default <I, O> Function<I, O> appliesOnInstance(SerializableFunction<I, O> f) { return sync(transfer(f)); }

    default <I1, I2, O> BiFunction<I1, I2, Future<O>> asyncAppliesOnInstance(SerializableBiFunction<I1, I2, O> f) { return async(transfer(f)); }
    default <I1, I2, O> BiFunction<I1, I2, O> appliesOnInstance(SerializableBiFunction<I1, I2, O> f) { return sync(transfer(f)); }

    default <I1, I2, I3, O> TriFunction<I1, I2, I3, Future<O>> asyncAppliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f) { return async(transfer(f)); }
    default <I1, I2, I3, O> TriFunction<I1, I2, I3, O> appliesOnInstance(SerializableTriFunction<I1, I2, I3, O> f) { return sync(transfer(f)); }

    <E extends Serializable> E transfer(E object);

}