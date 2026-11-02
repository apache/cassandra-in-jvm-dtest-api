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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSigner;
import java.util.Arrays;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.distributed.api.IClassTransformer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class InstanceClassLoaderSignedJarTest
{
    static class First {}
    static class Second {}

    @Test
    public void testSameSignersWhenTransformerReadsAnotherEntryFirst(@TempDir Path dir) throws Exception
    {
        URL jar = signedJar(dir, First.class, Second.class).toUri().toURL();

        // read Second's entry before Second is loaded, as the simulator transformer does
        IClassTransformer transform = (name, bytes) -> {
            if (name.equals(First.class.getName()))
            {
                try
                {
                    readFully(new URL("jar:" + jar + "!/" + resourceName(Second.class)).openStream());
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            }
            return bytes;
        };
        InstanceClassLoader loader = new InstanceClassLoader(0, 0, new URL[]{ jar }, getClass().getClassLoader(),
                                                             name -> name.startsWith("java."), transform);

        CodeSigner[] first = signers(Class.forName(First.class.getName(), false, loader));
        CodeSigner[] second = signers(Class.forName(Second.class.getName(), false, loader));

        assertThat(first).isNotEmpty();
        assertThat(second).isEqualTo(first);
    }

    private static CodeSigner[] signers(Class<?> clazz)
    {
        return clazz.getProtectionDomain().getCodeSource().getCodeSigners();
    }

    private static Path signedJar(Path dir, Class<?>... classes) throws Exception
    {
        Path jar = dir.resolve("signed.jar");
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar), manifest))
        {
            for (Class<?> clazz : classes)
            {
                out.putNextEntry(new JarEntry(resourceName(clazz)));
                out.write(readFully(clazz.getClassLoader().getResourceAsStream(resourceName(clazz))));
                out.closeEntry();
            }
        }

        Path keystore = dir.resolve("keystore.p12");
        run(jdkTool("keytool"), "-genkeypair", "-keystore", keystore.toString(), "-storetype", "PKCS12",
            "-storepass", "password", "-keypass", "password", "-alias", "signer", "-dname", "CN=signer",
            "-keyalg", "RSA", "-keysize", "2048", "-validity", "1");
        run(jdkTool("jarsigner"), "-keystore", keystore.toString(), "-storetype", "PKCS12",
            "-storepass", "password", jar.toString(), "signer");
        return jar;
    }

    private static String jdkTool(String name)
    {
        String executable = System.getProperty("os.name").startsWith("Windows") ? name + ".exe" : name;
        Path javaHome = Paths.get(System.getProperty("java.home"));
        Path tool = null;
        // on JDK 8 java.home is <jdk>/jre and jarsigner is in <jdk>/bin
        for (Path bin : Arrays.asList(javaHome.resolve("bin"), javaHome.resolveSibling("bin")))
        {
            if (Files.isExecutable(bin.resolve(executable)))
            {
                tool = bin.resolve(executable);
                break;
            }
        }
        assumeTrue(tool != null, name + " not found for java.home " + javaHome);
        return tool.toString();
    }

    private static void run(String... command) throws IOException, InterruptedException
    {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(readFully(process.getInputStream()), StandardCharsets.UTF_8);
        assertThat(process.waitFor()).as("%s%n%s", String.join(" ", command), output).isZero();
    }

    private static String resourceName(Class<?> clazz)
    {
        return clazz.getName().replace('.', '/') + ".class";
    }

    private static byte[] readFully(InputStream in) throws IOException
    {
        try (InputStream input = in)
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            for (int n = input.read(buffer); n >= 0; n = input.read(buffer))
                out.write(buffer, 0, n);
            return out.toByteArray();
        }
    }
}
