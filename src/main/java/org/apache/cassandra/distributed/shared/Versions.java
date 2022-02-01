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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.Semver.SemverType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Versions
{
    private static final Logger logger = LoggerFactory.getLogger(Versions.class);

    public static final String PROPERTY_PREFIX = "cassandra.";

    public static URL[] getClassPath()
    {
        // In Java 9 the default system ClassLoader got changed from URLClassLoader to AppClassLoader which
        // does not extend URLClassLoader nor does it give access to the class path array.
        // Java requires the system property 'java.class.path' to be setup with the classpath seperated by :
        // so this logic parses that string into the URL[] needed to build later
        String cp = System.getProperty("java.class.path");
        assert cp != null && !cp.isEmpty();
        String[] split = cp.split(File.pathSeparator);
        assert split.length > 0;
        URL[] urls = new URL[split.length];
        try
        {
            for (int i = 0; i < split.length; i++)
                urls[i] = Paths.get(split[i]).toUri().toURL();
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException(e);
        }
        return urls;
    }

    public static final class Version implements Comparable<Version>
    {
        public final Semver version;
        public final URL[] classpath;

        public Version(String version, URL[] classpath)
        {
            this(new Semver(version, SemverType.LOOSE), classpath);
        }

        public Version(Semver version, URL[] classpath)
        {
            this.version = version;
            this.classpath = classpath;
        }

        @Override
        public int compareTo(Version o) {
            return version.compareTo(o.version);
        }
    }

    private final Map<Semver, List<Version>> versions;

    private Versions(Map<Semver, List<Version>> versions)
    {
        this.versions = versions;
    }

    public Version get(String full)
    {
        return get(new Semver(full, SemverType.LOOSE));
    }

    public Version get(Semver version)
    {
        Supplier<RuntimeException> onError = () -> new RuntimeException("No version " + version.getOriginalValue() + " found");
        List<Version> versions = this.versions.get(first(version));
        if (versions == null)
            throw onError.get();
        return versions.stream()
                       .filter(v -> version.equals(v.version))
                       .findFirst()
                       .orElseThrow(onError);
    }

    private static Semver first(Semver version)
    {
        int major = version.getMajor();
        int minor = version.getMinor();
        if (major < 2 || (major < 3 && minor < 1))
            throw new IllegalArgumentException(version.getOriginalValue());
        return new Semver(major + "." + minor, SemverType.LOOSE);
    }

    public Version getLatest(Semver version)
    {
        return versions.get(first(version))
											  .stream()
												.findFirst()
											  .orElseThrow(() -> new RuntimeException("No " + version + " versions found"));
    }

    public static Versions find()
    {
        final String dtestJarDirectory = System.getProperty(PROPERTY_PREFIX + "test.dtest_jar_path", "build");
        final File sourceDirectory = new File(dtestJarDirectory);
        logger.info("Looking for dtest jars in " + sourceDirectory.getAbsolutePath());
        final Pattern pattern = Pattern.compile("dtest-(?<fullversion>(\\d+)\\.(\\d+)((\\.|-alpha|-beta|-rc)([0-9]+))?(\\.\\d+)?)([~\\-]\\w[.\\w]*(?:\\-\\w[.\\w]*)*)?(\\+[.\\w]+)?\\.jar");
        final Map<Semver, List<Version>> versions = new HashMap<>();

        if (sourceDirectory.exists())
        {
            for (File file : sourceDirectory.listFiles())
            {
                Matcher m = pattern.matcher(file.getName());
                if (!m.matches())
                    continue;
                Semver version = new Semver(m.group(1), SemverType.LOOSE);
                Semver series = first(version);
                versions.putIfAbsent(series, new ArrayList<>());
                versions.get(series).add(new Version(version, new URL[]{ toURL(file) }));
            }
        }

        for (Map.Entry<Semver, List<Version>> e : versions.entrySet())
        {
            if (e.getValue().isEmpty())
                continue;
            Collections.sort(e.getValue(), Collections.reverseOrder());
            System.out.println("Found " + e.getValue().stream().map(v -> v.version.getOriginalValue()).collect(Collectors.joining(", ")));
        }

        return new Versions(versions);
    }

    private static URL toURL(File file)
    {
        try
        {
            return file.toURI().toURL();
        }
        catch (MalformedURLException e)
        {
            throw new IllegalArgumentException(e);
        }
    }
}
