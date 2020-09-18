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

import java.util.Collections;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class NodeToolOutputTest
{
    @Test
    public void testAssertOutputNoThrow()
    {
        NodeToolResult result = create("stdout", "stderr");
        result.asserts().stdoutContains("out");
        result.asserts().stderrContains("err");
        result.asserts().stdoutNotContains("hello");
        result.asserts().stderrNotContains("world");
    }

    @Test
    public void testAssertContainsWithNoOutput()
    {
        NodeToolResult result = create(null, null);
        Throwable exception = catchThrowableOfType(() -> {
            result.asserts().stdoutContains("foo");
        }, AssertionError.class);
        assertThat(exception.getMessage()).isEqualTo("stdout not defined");

        exception = catchThrowableOfType(() -> {
            result.asserts().stdoutNotContains("foo");
        }, AssertionError.class);
        assertThat(exception.getMessage()).isEqualTo("stdout not defined");
    }

    @Test
    public void testAssertContainsFails()
    {
        NodeToolResult result = create("stdout", "stderr");
        Throwable exception = catchThrowableOfType(() -> {
           result.asserts().stdoutContains("foo");
        }, AssertionError.class);
        assertThat(exception.getMessage()).contains("Unable to locate substring");

        exception = catchThrowableOfType(() -> {
            result.asserts().stdoutNotContains("out");
        }, AssertionError.class);
        assertThat(exception.getMessage()).contains("Found unexpected substring");
    }

    private NodeToolResult create(String stdout, String stderr)
    {
        return new NodeToolResult(null, 0, Collections.emptyList(), null, stdout, stderr);
    }
}
