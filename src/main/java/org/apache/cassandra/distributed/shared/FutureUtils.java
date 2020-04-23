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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public final class FutureUtils {
    private FutureUtils() { }

    public static <T> T waitOn(Future<T> f)
    {
        try
        {
            return f.get();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Error) throw (Error) cause;
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            throw new RuntimeException(cause);
        }
    }

    public static <A, B> Future<B> map(Future<A> future, Function<? super A, ? extends B> fn) {
        if (future == null) throw new NullPointerException("Future is null");
        if (fn == null) throw new NullPointerException("Function is null");

        if (future instanceof CompletableFuture) {
            return ((CompletableFuture<A>) future).thenApply(fn);
        }
        return new MapFuture<>(future, fn);
    }

    private static final class MapFuture<A, B> implements Future<B>
    {
        private final Future<A> parent;
        private final Function<? super A, ? extends B> fn;

        private MapFuture(Future<A> parent, Function<? super A, ? extends B> fn) {
            this.parent = parent;
            this.fn = fn;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return parent.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return parent.isCancelled();
        }

        @Override
        public boolean isDone() {
            return parent.isDone();
        }

        @Override
        public B get() throws InterruptedException, ExecutionException {
            return fn.apply(parent.get());
        }

        @Override
        public B get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return fn.apply(parent.get(timeout, unit));
        }
    }
}
