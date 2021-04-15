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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface ICluster<I extends IInstance> extends AutoCloseable
{
    public static final String PROPERTY_PREFIX = "cassandra.test";

    void startup();

    I bootstrap(IInstanceConfig config);

    I get(int i);

    I get(InetSocketAddress endpoint);

    ICoordinator coordinator(int node);

    void schemaChange(String query);

    void schemaChange(String statement, int instance);

    int size();

    Stream<I> stream();

    Stream<I> stream(String dcName);

    Stream<I> stream(String dcName, String rackName);

    IMessageFilters filters();

    default void setMessageSink(IMessageSink messageSink) { throw new UnsupportedOperationException(); }

    default void deliverMessage(InetSocketAddress to, IMessage msg)
    {
        IInstance toInstance = get(to);
        if (toInstance != null)
            toInstance.receiveMessage(msg);
    }

    /**
     * dynamically sets the current uncaught exceptions filter
     *
     * the predicate should return true if we should ignore the given throwable on the given instance
     */
    default void setUncaughtExceptionsFilter(BiPredicate<Integer, Throwable> ignoreThrowable) {}
    default void setUncaughtExceptionsFilter(Predicate<Throwable> ignoreThrowable)
    {
        setUncaughtExceptionsFilter((ignored, throwable) -> ignoreThrowable.test(throwable));
    }
    default void checkAndResetUncaughtExceptions() {}

    static void setup() throws Throwable
    {
        setupLogging();
        setSystemProperties();
        nativeLibraryWorkaround();
        processReaperWorkaround();
    }

    static void nativeLibraryWorkaround()
    {
        // Disable the Netty tcnative library otherwise the io.netty.internal.tcnative.CertificateCallbackTask,
        // CertificateVerifierTask, SSLPrivateKeyMethodDecryptTask, SSLPrivateKeyMethodSignTask,
        // SSLPrivateKeyMethodTask, and SSLTask hold a gcroot against the InstanceClassLoader.
        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        System.setProperty("io.netty.transport.noNative", "true");
    }

    static void processReaperWorkaround() throws Throwable
    {
        // Make sure the 'process reaper' thread is initially created under the main classloader,
        // otherwise it gets created with the contextClassLoader pointing to an InstanceClassLoader
        // which prevents it from being garbage collected.
        new ProcessBuilder().command("true").start().waitFor();
    }

    static void setSystemProperties()
    {
        System.setProperty("cassandra.ring_delay_ms", Integer.toString(30 * 1000));
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    static void setupLogging()
    {
        try
        {
            File root = Files.createTempDirectory("in-jvm-dtest").toFile();
            root.deleteOnExit();
            String logConfigPropertyName = System.getProperty(PROPERTY_PREFIX + ".logConfigProperty", "logback.configurationFile");
            Path testConfPath = Paths.get(System.getProperty(PROPERTY_PREFIX + ".logConfigPath", "test/conf/logback-dtest.xml"));
            Path logConfPath = Paths.get(root.getPath(), testConfPath.getFileName().toString());
            if (!logConfPath.toFile().exists())
            {
                Files.copy(testConfPath, logConfPath);
            }

            System.setProperty(logConfigPropertyName, "file://" + logConfPath);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}