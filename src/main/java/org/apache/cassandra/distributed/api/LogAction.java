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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.distributed.shared.Uninterruptables;

public interface LogAction
{
    /**
     * @return current position of the iterator
     * @see LogAction#grep(long, String)
     * @see LogAction#watchFor(long, String)
     */
    long mark();

    LineIterator match(long startPosition, Predicate<String> fn);

    default LineIterator match(Predicate<String> fn){
        return match(Internal.DEFAULT_START_POSITION, fn);
    }

    default LogResult<List<String>> watchFor(long startPosition, Duration timeout, Predicate<String> fn) throws TimeoutException
    {
        long nowNanos = System.nanoTime();
        long deadlineNanos = nowNanos + timeout.toNanos();
        long previousPosition = startPosition;
        List<String> matches = new ArrayList<>();
        while (System.nanoTime() <= deadlineNanos)
        {
            if (previousPosition == mark())
            {
                // still matching... wait a bit
                Uninterruptables.sleepUninterruptibly(1, TimeUnit.SECONDS);
                continue;
            }
            // position not matching, try to read
            try (LineIterator it = match(previousPosition, fn))
            {
                while (it.hasNext())
                    matches.add(it.next());
                if (!matches.isEmpty())
                    return new BasicLogResult<>(it.mark(), matches);
                previousPosition = it.mark();
            }
        }
        throw new TimeoutException();
    }

    default LogResult<List<String>> watchFor(Duration timeout, Predicate<String> fn) throws TimeoutException {
        return watchFor(Internal.DEFAULT_START_POSITION, timeout, fn);
    }

    default LogResult<List<String>> watchFor(Predicate<String> fn) throws TimeoutException {
        return watchFor(Internal.DEFAULT_START_POSITION, Internal.DEFAULT_TIMEOUT, fn);
    }

    default LogResult<List<String>> watchFor(long startPosition, Duration timeout, List<Pattern> patterns) throws TimeoutException
    {
        return watchFor(startPosition, timeout, Internal.regexPredicate(patterns));
    }

    default LogResult<List<String>> watchFor(Duration timeout, List<Pattern> patterns) throws TimeoutException
    {
        return watchFor(Internal.DEFAULT_START_POSITION, timeout, Internal.regexPredicate(patterns));
    }

    default LogResult<List<String>> watchFor(long startPosition, List<Pattern> patterns) throws TimeoutException
    {
        return watchFor(startPosition, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(patterns));
    }

    default LogResult<List<String>> watchFor(List<Pattern> patterns) throws TimeoutException
    {
        return watchFor(Internal.DEFAULT_START_POSITION, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(patterns));
    }

    default LogResult<List<String>> watchFor(long startPosition, Duration timeout, Pattern pattern) throws TimeoutException
    {
        return watchFor(startPosition, timeout, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(Duration timeout, Pattern pattern) throws TimeoutException
    {
        return watchFor(Internal.DEFAULT_START_POSITION, timeout, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(long startPosition, Pattern pattern) throws TimeoutException
    {
        return watchFor(startPosition, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(Pattern pattern) throws TimeoutException
    {
        return watchFor(Internal.DEFAULT_START_POSITION, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(long startPosition, Duration timeout, String pattern) throws TimeoutException
    {
        return watchFor(startPosition, timeout, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(Duration timeout, String pattern) throws TimeoutException
    {
        return watchFor(Internal.DEFAULT_START_POSITION, timeout, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(long startPosition, String pattern) throws TimeoutException
    {
        return watchFor(startPosition, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(String pattern) throws TimeoutException
    {
        return watchFor(Internal.DEFAULT_START_POSITION, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> watchFor(long startPosition, Duration timeout, String pattern, String... others) throws TimeoutException
    {

        return watchFor(startPosition, timeout, Internal.regexPredicate(pattern, others));
    }

    default LogResult<List<String>> watchFor(Duration timeout, String pattern, String... others) throws TimeoutException
    {

        return watchFor(Internal.DEFAULT_START_POSITION, timeout, Internal.regexPredicate(pattern, others));
    }

    default LogResult<List<String>> watchFor(long startPosition, String pattern, String... others) throws TimeoutException
    {

        return watchFor(startPosition, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(pattern, others));
    }

    default LogResult<List<String>> watchFor(String pattern, String... others) throws TimeoutException
    {
        return watchFor(Internal.DEFAULT_START_POSITION, Internal.DEFAULT_TIMEOUT, Internal.regexPredicate(pattern, others));
    }

    default LogResult<List<String>> grep(long startPosition, Predicate<String> fn)
    {
        try (LineIterator it = match(startPosition, fn))
        {
            return new BasicLogResult<>(it.mark(), Internal.collect(it));
        }
    }

    default LogResult<List<String>> grep(Predicate<String> fn)
    {
        return grep(Internal.DEFAULT_START_POSITION, fn);
    }

    default LogResult<List<String>> grep(long startPosition, List<Pattern> patterns)
    {
        return grep(startPosition, Internal.regexPredicate(patterns));
    }

    default LogResult<List<String>> grep(List<Pattern> patterns)
    {
        return grep(Internal.DEFAULT_START_POSITION, Internal.regexPredicate(patterns));
    }

    default LogResult<List<String>> grep(long startPosition, Pattern pattern)
    {
        return grep(startPosition, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> grep(Pattern pattern)
    {
        return grep(Internal.DEFAULT_START_POSITION, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> grep(long startPosition, String pattern)
    {
        return grep(startPosition, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> grep(String pattern)
    {
        return grep(Internal.DEFAULT_START_POSITION, Internal.regexPredicate(pattern));
    }

    default LogResult<List<String>> grep(long startPosition, String pattern, String... others)
    {

        return grep(startPosition, Internal.regexPredicate(pattern, others));
    }

    default LogResult<List<String>> grep(String pattern, String... others)
    {
        return grep(Internal.DEFAULT_START_POSITION, Internal.regexPredicate(pattern, others));
    }

    /**
     * Attempt to find all errors in the log.  This method is different from {@code grep("^ERROR")} as it will also
     * attempt to stitch the exception stack trace into a single line.
     *
     * This method is modeled after python dtests's grep_for_errors and matches the semantics there.
     *
     * @param startPosition to grep from
     * @param exceptionPattern for WARN logs to check if they might have an exception
     * @return result of all found exceptions, with stitched errors
     */
    default LogResult<List<String>> grepForErrors(long startPosition, Pattern exceptionPattern)
    {
        Objects.requireNonNull(exceptionPattern, "exceptionPattern");

        Pattern logLevelPattern = Internal.LOG_LEVEL_PATTERN;
        Function<String, String> extractLogLevel = line -> {
            Matcher matcher = logLevelPattern.matcher(line);
            if (!matcher.find())
                return null;
            return matcher.group(1);
        };
        List<String> matches = new ArrayList<>();
        try (LineIterator it = match(startPosition, ignore -> true))
        {
            StringBuilder lineBuffer = new StringBuilder();
            while (it.hasNext())
            {
                String line = it.next();
                String logLevelOrNull = extractLogLevel.apply(line);
                if (logLevelOrNull == null)
                {
                    // found a log which isn't the start of a logger line; assume its an exception
                    if (lineBuffer.length() == 0)
                    {
                        // no previous start of line found, so skip this
                        continue;
                    }
                    lineBuffer.append('\n').append(line);
                    continue;
                }
                // line is a start of a log, reset state
                if (lineBuffer.length() != 0)
                {
                    // buffer has content, add and move on
                    matches.add(lineBuffer.toString());
                    lineBuffer.setLength(0);
                }
                switch (logLevelOrNull)
                {
                    case "ERROR":
                        lineBuffer.append(line);
                        break;
                    case "WARN":
                        if (exceptionPattern.matcher(line).find())
                            lineBuffer.append(line);
                        break;
                    default:
                        // ignore
                }
            }
            if (lineBuffer.length() != 0)
            {
                matches.add(lineBuffer.toString());
            }
            return new BasicLogResult<>(it.mark(), matches);
        }
    }

    /**
     * Attempt to find all errors in the log.  This method is different from {@code grep("^ERROR")} as it will also
     * attempt to stitch the exception stack trace into a single line.
     *
     * This method is modeled after python dtests's grep_for_errors and matches the semantics there.
     *
     * @param startPosition to grep from
     * @return result of all found exceptions, with stitched errors
     */
    default LogResult<List<String>> grepForErrors(long startPosition)
    {
        return grepForErrors(startPosition, Internal.LOG_EXCEPTION_PATTERN);
    }

    /**
     * Attempt to find all errors in the log.  This method is different from {@code grep("^ERROR")} as it will also
     * attempt to stitch the exception stack trace into a single line.
     *
     * This method is modeled after python dtests's grep_for_errors and matches the semantics there.
     *
     * @return result of all found exceptions, with stitched errors
     */
    default LogResult<List<String>> grepForErrors()
    {
        return grepForErrors(Internal.DEFAULT_START_POSITION, Internal.LOG_EXCEPTION_PATTERN);
    }

    class BasicLogResult<T> implements LogResult<T>
    {
        private final long mark;
        private final T result;

        public BasicLogResult(long mark, T result) {
            this.mark = mark;
            this.result = Objects.requireNonNull(result);
        }

        @Override
        public long getMark() {
            return mark;
        }

        @Override
        public T getResult() {
            return result;
        }

        @Override
        public String toString() {
            return "LogResult{" +
                    "mark=" + mark +
                    ", result=" + result +
                    '}';
        }
    }

    class Internal
    {
        private static final int DEFAULT_START_POSITION = -1;
        // why 10m?  This is the default for python dtest...
        private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);
        private static final Pattern LOG_LEVEL_PATTERN = Pattern.compile("^(INFO|DEBUG|WARN|ERROR)");
        private static final Pattern LOG_EXCEPTION_PATTERN = Pattern.compile("[Ee]xception|AssertionError");

        private static List<String> collect(LineIterator it)
        {
            List<String> matches = new ArrayList<>();
            while (it.hasNext())
                matches.add(it.next());
            return matches.isEmpty() ? Collections.emptyList() : matches;
        }

        private static Predicate<String> regexPredicate(List<Pattern> patterns)
        {
            return line -> {
                for (Pattern regex : patterns)
                {
                    Matcher m = regex.matcher(line);
                    if (m.find())
                        return true;
                }
                return false;
            };
        }

        private static Predicate<String> regexPredicate(String pattern, String... others)
        {
            List<Pattern> patterns = new ArrayList<>(others.length + 1);
            patterns.add(Pattern.compile(pattern));
            for (String s : others)
                patterns.add(Pattern.compile(s));
            return regexPredicate(patterns);
        }

        private static Predicate<String> regexPredicate(Pattern pattern)
        {
            return line -> pattern.matcher(line).find();
        }

        private static Predicate<String> regexPredicate(String pattern)
        {
            return regexPredicate(Pattern.compile(pattern));
        }
    }
}
