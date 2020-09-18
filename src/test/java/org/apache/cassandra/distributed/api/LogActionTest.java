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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

class LogActionTest
{
    @Test
    public void watchForTimeout() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn, "a", "b", "c", "d"));

        Duration duration = Duration.ofSeconds(1);
        long startNanos = System.nanoTime();
        Assertions.assertThatThrownBy(() -> logs.watchFor(duration, "^ERROR"))
                .isInstanceOf(TimeoutException.class);
        Assertions.assertThat(System.nanoTime())
                .as("duration was smaller than expected timeout")
                .isGreaterThanOrEqualTo(startNanos + duration.toNanos());
    }

    @Test
    public void watchForAndFindFirstAttempt() throws TimeoutException {
        LogAction logs = mockLogAction(fn -> lineIterator(fn, "a", "b", "ERROR match", "d"));

        List<String> matches = logs.watchFor("^ERROR").getResult();
        Assertions.assertThat(matches).isEqualTo(Arrays.asList("ERROR match"));
    }

    @Test
    public void watchForAndFindThirdAttempt() throws TimeoutException {
        class Counter
        {
            int count;
        }
        Counter counter = new Counter();
        LogAction logs = mockLogAction(fn -> {
            if (++counter.count == 3) {
                return lineIterator(fn, "a", "b", "ERROR match", "d");
            } else {
                return lineIterator(fn, "a", "b", "c", "d");
            }
        });

        List<String> matches = logs.watchFor("^ERROR").getResult();
        Assertions.assertThat(matches).isEqualTo(Arrays.asList("ERROR match"));
        Assertions.assertThat(counter.count).isEqualTo(3);
    }

    @Test
    public void grepNoMatch() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn, "a", "b", "c", "d"));

        List<String> matches = logs.grep("^ERROR").getResult();
        Assertions.assertThat(matches).isEmpty();
    }

    @Test
    public void grepMatch() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn, "a", "b", "ERROR match", "d"));

        List<String> matches = logs.grep("^ERROR").getResult();
        Assertions.assertThat(matches).isEqualTo(Arrays.asList("ERROR match"));
    }

    @Test
    public void grepForErrorsNoMatch() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn, "a", "b", "c", "d"));

        List<String> matches = logs.grepForErrors().getResult();
        Assertions.assertThat(matches).isEmpty();
    }

    @Test
    public void grepForErrorsNoStacktrace() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn, "INFO a", "INFO b", "ERROR match", "INFO d"));

        List<String> matches = logs.grepForErrors().getResult();
        Assertions.assertThat(matches).isEqualTo(Arrays.asList("ERROR match"));
    }

    @Test
    public void grepForErrorsWithStacktrace() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn,
                "INFO a", "INFO b",
                "ERROR match",
                "\t\tat class.method(42)",
                "\t\tat class.method(42)"));

        List<String> matches = logs.grepForErrors().getResult();
        Assertions.assertThat(matches).isEqualTo(Arrays.asList("ERROR match\n" +
                "\t\tat class.method(42)\n" +
                "\t\tat class.method(42)"));
    }

    @Test
    public void grepForErrorsMultilineWarnNotException() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn,
                "INFO a", "INFO b",
                "WARN match",
                "\t\tat class.method(42)",
                "\t\tat class.method(42)"));

        List<String> matches = logs.grepForErrors().getResult();
        Assertions.assertThat(matches).isEmpty();
    }

    @Test
    public void grepForErrorsWARNWithStacktrace() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn,
                "INFO a", "INFO b",
                "WARN match but exception in test",
                "\t\tat class.method(42)",
                "\t\tat class.method(42)"));

        List<String> matches = logs.grepForErrors().getResult();
        Assertions.assertThat(matches).isEqualTo(Arrays.asList("WARN match but exception in test\n" +
                "\t\tat class.method(42)\n" +
                "\t\tat class.method(42)"));
    }

    @Test
    public void grepForErrorsWARNWithStacktraceFromAssert() {
        LogAction logs = mockLogAction(fn -> lineIterator(fn,
                "INFO a", "INFO b",
                "WARN match but AssertionError in test",
                "\t\tat class.method(42)",
                "\t\tat class.method(42)"));

        List<String> matches = logs.grepForErrors().getResult();
        Assertions.assertThat(matches).isEqualTo(Arrays.asList("WARN match but AssertionError in test\n" +
                "\t\tat class.method(42)\n" +
                "\t\tat class.method(42)"));
    }

    private static LogAction mockLogActionAnswer(Answer<?> matchAnswer) {
        LogAction logs = Mockito.mock(LogAction.class, Mockito.CALLS_REAL_METHODS);
        // mark is only used by matching, which we also mock out, so its ok to be a constant
        Mockito.when(logs.mark()).thenReturn(0L);
        Mockito.when(logs.match(Mockito.anyLong(), Mockito.any())).thenAnswer(matchAnswer);
        return logs;
    }

    private static LogAction mockLogAction(MockLogMatch match) {
        return mockLogActionAnswer(invoke -> match.answer(invoke.getArgument(0), invoke.getArgument(1)));
    }

    private static LogAction mockLogAction(MockLogMatchPredicate match) {
        return mockLogAction((MockLogMatch) match);
    }

    @FunctionalInterface
    private interface MockLogMatch
    {
        LineIterator answer(long startPosition, Predicate<String> fn) throws Throwable;
    }

    @FunctionalInterface
    private interface MockLogMatchPredicate extends MockLogMatch
    {
        LineIterator answer(Predicate<String> fn) throws Throwable;

        @Override
        default LineIterator answer(long startPosition, Predicate<String> fn) throws Throwable
        {
            return answer(fn);
        }
    }

    private static LineIterator lineIterator(Predicate<String> fn, String... values)
    {
        return new LineIteratorImpl(Arrays.asList(values).iterator(), fn);
    }

    private static final class LineIteratorImpl implements LineIterator
    {
        private final Iterator<String> it;
        private final Predicate<String> fn;
        private String next = null;
        private long count = 0;

        private LineIteratorImpl(Iterator<String> it, Predicate<String> fn) {
            this.it = it;
            this.fn = fn;
        }

        @Override
        public long mark() {
            return count;
        }

        @Override
        public boolean hasNext() {
            if (next != null) // only move forward if consumed
                return true;
            while (it.hasNext())
            {
                count++;
                String next = it.next();
                if (fn.test(next))
                {
                    this.next = next;
                    return true;
                }
            }
            return false;
        }

        @Override
        public String next() {
            String ret = next;
            next = null;
            return ret;
        }
    }
}
