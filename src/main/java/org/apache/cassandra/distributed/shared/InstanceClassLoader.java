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

import org.apache.cassandra.distributed.api.IClassTransformer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.jar.Manifest;

public class InstanceClassLoader extends URLClassLoader
{
    private static final Predicate<String> DEFAULT_SHARED_PACKAGES =
              name ->
              name.startsWith("org.apache.cassandra.distributed.api.")
              || name.startsWith("org.apache.cassandra.distributed.shared.")
              || name.startsWith("com.vdurmont.semver4j.")
              || name.startsWith("sun.")
              || name.startsWith("oracle.")
              || name.startsWith("com.intellij.")
              || name.startsWith("com.sun.")
              || name.startsWith("com.oracle.")
              || name.startsWith("java.")
              || name.startsWith("javax.")
              || name.startsWith("jdk.")
              || name.startsWith("netscape.")
              || name.startsWith("org.xml.sax.")
              || name.startsWith("org.jboss.byteman.")
              || name.startsWith("oshi.jna.");

    private volatile boolean isClosed = false;
    private final URL[] urls;
    private final int generation; // used to help debug class loader leaks, by helping determine which classloaders should have been collected
    private final int id;
    private final ClassLoader sharedClassLoader;
    private final Predicate<String> loadShared;
    private final IClassTransformer transform;

    public InstanceClassLoader(int generation, int id, URL[] urls, ClassLoader sharedClassLoader)
    {
        this(generation, id, urls, sharedClassLoader, DEFAULT_SHARED_PACKAGES);
    }

    public InstanceClassLoader(int generation, int id, URL[] urls, ClassLoader sharedClassLoader, IClassTransformer transform)
    {
        this(generation, id, urls, sharedClassLoader, DEFAULT_SHARED_PACKAGES, transform);
    }

    public InstanceClassLoader(int generation, int id, URL[] urls, ClassLoader sharedClassLoader, Predicate<String> loadShared)
    {
        this(generation, id, urls, sharedClassLoader, loadShared, null);
    }

    public InstanceClassLoader(int generation, int id, URL[] urls, ClassLoader sharedClassLoader, Predicate<String> loadShared, IClassTransformer transform)
    {
        super(urls, null);
        this.urls = urls;
        this.sharedClassLoader = sharedClassLoader;
        this.generation = generation;
        this.id = id;
        this.loadShared = loadShared == null ? DEFAULT_SHARED_PACKAGES : loadShared;
        this.transform = transform;
    }

    public static Predicate<String> getDefaultLoadSharedFilter()
    {
        return DEFAULT_SHARED_PACKAGES;
    }

    public int getClusterGeneration()
    {
        return generation;
    }

    public int getInstanceId()
    {
        return id;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException
    {
        if (loadShared.test(name))
            return sharedClassLoader.loadClass(name);

        return loadClassInternal(name);
    }

    Class<?> loadClassInternal(String name) throws ClassNotFoundException
    {
        if (isClosed)
            throw new IllegalStateException(String.format("Can't load %s. Instance class loader is already closed.", name));

        synchronized (getClassLoadingLock(name))
        {
            // First, check if the class has already been loaded
            Class<?> c = findLoadedClass(name);

            if (c == null)
                c = findClass(name);

            return c;
        }
    }

    /**
     * @return true iff this class was loaded by an InstanceClassLoader, and as such is used by a dtest node
     */
    public static boolean wasLoadedByAnInstanceClassLoader(Class<?> clazz)
    {
        return clazz.getClassLoader().getClass().getName().equals(InstanceClassLoader.class.getName());
    }

    protected Class<?> findClass(String name) throws ClassNotFoundException
    {
        if (transform == null)
            return super.findClass(name);

        String pkg = name.substring(0, name.lastIndexOf('.'));
        String path = name.replace('.', '/').concat(".class");
        URL url = getResource(path);
        if (url == null)
        {
            byte[] bytes = transform.transform(name, null);
            if (bytes == null)
                throw new ClassNotFoundException(name);
            if (null == getPackage(pkg))
                definePackage(pkg, null, null, null, null, null, null, null);
            return defineClass(name, bytes, 0, bytes.length);
        }
        try
        {
            URLConnection connection = url.openConnection();
            CodeSigner[] codeSigners = null;
            Manifest manifest = null;
            if (connection instanceof JarURLConnection)
            {
                manifest = ((JarURLConnection) connection).getManifest();
                codeSigners = ((JarURLConnection) connection).getJarEntry().getCodeSigners();
            }
            if (null == getPackage(pkg))
            {
                try
                {
                    if (manifest != null) definePackage(pkg, manifest, url);
                    else definePackage(pkg, null, null, null, null, null, null, null);
                }
                catch (IllegalArgumentException iae)
                {
                    if (null == getPackage(pkg))
                        throw iae;
                }
            }
            try (InputStream in = connection.getInputStream())
            {
                byte[] bytes = new byte[in.available()];
                int cur, total = 0;
                while (0 <= (cur = in.read(bytes, total, bytes.length - total)))
                {
                    total += cur;
                    if (cur == 0 && total == bytes.length)
                    {
                        int next = in.read();
                        if (next < 0)
                            break;

                        bytes = Arrays.copyOf(bytes, bytes.length * 2);
                        bytes[total++] = (byte)next;
                    }
                }
                if (total < bytes.length)
                    bytes = Arrays.copyOf(bytes, total);

                bytes = transform.transform(name, bytes);
                return defineClass(name, bytes, 0, bytes.length, new CodeSource(url, codeSigners));
            }
        }
        catch (Throwable t)
        {
            throw new ClassNotFoundException(name, t);
        }
    }

    public String toString()
    {
        return "InstanceClassLoader{" +
               "generation=" + generation +
               ", id = " + id +
               ", urls=" + Arrays.toString(urls) +
               '}';
    }

    public void close() throws IOException
    {
        isClosed = true;
        super.close();
        try
        {
            // The JVM really wants to prevent Class instances from being GCed until the
            // classloader which loaded them is GCed. It therefore maintains a list
            // of Class instances for the sole purpose of providing a GC root for them.
            // Here, we actually want the class instances to be GCed even if this classloader
            // somehow gets stuck with a GC root, so we clear the class list via reflection.
            // The current features implemented technically work without this, but the Garbage
            // Collector works more efficiently with it here, and it may provide value to new
            // feature developers.
            Field f = getField(ClassLoader.class, "classes");
            f.setAccessible(true);
            List<Class<?>> classes = (List<Class<?>>) f.get(this);
            classes.clear();
            // Same problem with packages - without clearing this,
            // the instance classloader can't unload
            f = getField(ClassLoader.class, "packages");
            f.setAccessible(true);
            Map<?,?> packageMap = (Map<?, ?>) f.get(this);
            packageMap.clear();
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Field getField(Class<?> clazz, String fieldName) throws NoSuchFieldException
    {
        // below code works before Java 12
        try
        {
            return clazz.getDeclaredField(fieldName);
        }
        catch (NoSuchFieldException e)
        {
            // this is mitigation for JDK 17 (https://bugs.openjdk.org/browse/JDK-8210522)
            try
            {
                Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
                getDeclaredFields0.setAccessible(true);
                Field[] fields = (Field[]) getDeclaredFields0.invoke(clazz, false);
                for (Field field : fields)
                {
                    if (fieldName.equals(field.getName()))
                    {
                        return field;
                    }
                }
            }
            catch (ReflectiveOperationException ex)
            {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }
}
