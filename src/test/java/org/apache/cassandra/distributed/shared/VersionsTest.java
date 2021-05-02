/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.distributed.shared;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class VersionsTest
{
    public static final String[] VERSIONS = new String[]
    {
            "2.2.12",
            "2.2.2",
            "3.0.0",
            "3.0.10",
            "3.11.10",
            "3.11.9",
            "4.0-alpha1",
            "4.0-beta1",
            "4.0-rc1",
            "4.0.0",
            "4.1.0"
    };

    @BeforeAll
    public static void beforeAll() throws IOException
    {
        Path root = Files.createTempDirectory("versions");
        System.setProperty(Versions.PROPERTY_PREFIX + "test.dtest_jar_path", root.toAbsolutePath().toString());

        for (String version : VERSIONS)
            Files.createFile(Paths.get(root.toAbsolutePath().toString(), "dtest-" + version + ".jar"));
    }

    @AfterAll
    public static void afterAll() throws IOException
    {
        System.clearProperty(Versions.PROPERTY_PREFIX + "test.dtest_jar_path");
    }

    @Test
    public void testVersions() throws IOException
    {
        Versions versions = Versions.find();
        for (String version : VERSIONS)
            assertThat(versions.get(version)).isNotNull();
    }

    @Test
    public void testGet()
    {
        assertThat(Versions.find().get("2.2.2")).isNotNull();
    }

    @Test
    public void testGetLatest()
    {
        Versions.find().getLatest(Versions.Major.v22);
    }

    @Test
    public void testFind()
    {
        Versions versions = Versions.find();
        assertThat(versions).isNotNull();

    }

    @Test
    public void testToURL()
    {
        assertThat(Versions.getClassPath()).isNotEmpty();
    }

}
