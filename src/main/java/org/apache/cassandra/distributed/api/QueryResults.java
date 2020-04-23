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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;

public final class QueryResults
{
    private static final CompleteQueryResult EMPTY = new CompleteQueryResult(new String[0], null);

    private QueryResults() {}

    public static CompleteQueryResult getEmpty()
    {
        return EMPTY;
    }

    public static QueryResult fromIterator(String[] names, Iterator<Row> iterator)
    {
        Objects.requireNonNull(names, "names");
        Objects.requireNonNull(iterator, "iterator");
        return new IteratorQueryResult(names, iterator);
    }

    public static QueryResult fromObjectArrayIterator(String[] names, Iterator<Object[]> iterator)
    {
        Row row = new Row(names);
        return fromIterator(names, new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Row next() {
                row.setResults(iterator.next());
                return row;
            }
        });
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private static final int UNSET = -1;

        private int numColumns = UNSET;
        private String[] names;
        private List<Object[]> results = new ArrayList<>();

        public Builder columnNames(String... columns)
        {
            if (columns != null)
            {
                if (numColumns == UNSET)
                    numColumns = columns.length;

                if (numColumns != columns.length)
                    throw new AssertionError("Attempted to add column names with different column count; " +
                            "expected " + numColumns + " columns but given " + Arrays.toString(columns));
            }

            names = columns;
            return this;
        }

        public Builder row(Object... columns)
        {
            if (numColumns == UNSET)
                numColumns = columns.length;

            if (numColumns != columns.length)
                throw new AssertionError("Attempted to add row with different column count; " +
                        "expected " + numColumns + " columns but given " + Arrays.toString(columns));
            results.add(columns);
            return this;
        }

        public CompleteQueryResult build()
        {
            if (names == null)
            {
                if (numColumns == UNSET)
                    return QueryResults.getEmpty();
                names = new String[numColumns];
                for (int i = 0; i < numColumns; i++)
                    names[i] = "unknown";
            }
            return new CompleteQueryResult(names, results.stream().toArray(Object[][]::new));
        }
    }

    private static final class IteratorQueryResult implements QueryResult
    {
        private final String[] names;
        private final Iterator<Row> iterator;
        private final Predicate<Row> filter;
        private Row current;

        private IteratorQueryResult(String[] names, Iterator<Row> iterator)
        {
            this.names = names;
            this.iterator = iterator;
            this.filter = ignore -> true;
        }

        private IteratorQueryResult(String[] names, Iterator<Row> iterator, Predicate<Row> filter)
        {
            this.names = names;
            this.iterator = iterator;
            this.filter = filter;
        }

        @Override
        public List<String> getNames()
        {
            return Collections.unmodifiableList(Arrays.asList(names));
        }

        @Override
        public QueryResult filter(Predicate<Row> fn)
        {
            return new IteratorQueryResult(names, iterator, filter.and(fn));
        }

        @Override
        public boolean hasNext()
        {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (filter.test(row)) {
                    current = row;
                    return true;
                }
            }
            current = null;
            return false;
        }

        @Override
        public Row next()
        {
            if (current == null)
                throw new NoSuchElementException();
            return current;
        }
    }
}
