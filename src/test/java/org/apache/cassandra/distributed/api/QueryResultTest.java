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

import org.apache.cassandra.distributed.shared.AssertUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class QueryResultTest
{
    @Test
    public void empty()
    {
        QueryResult result = QueryResults.getEmpty();

        assertThat(result.getNames()).isEmpty();
        assertThat(result.toString()).isEqualTo("[]");

        assertThat(result.hasNext()).isFalse();
        assertThatThrownBy(() -> result.next()).isInstanceOf(NoSuchElementException.class);

        QueryResult filtered = result.filter(ignore -> true);
        assertThat(filtered.hasNext()).isFalse();
        assertThatThrownBy(() -> filtered.next()).isInstanceOf(NoSuchElementException.class);

        Iterator<Object> it = result.map(r -> r.get("undefined"));
        assertThat(it.hasNext()).isFalse();
        assertThatThrownBy(() -> it.next()).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void equals()
    {
        String[] names = { "fname", "lname"};
        Object[][] rows = {
                new Object[] { "david", "capwell"},
                new Object[] { "alex", "petrov"},
                new Object[] { "dinesh", "joshi"},
        };
        CompleteQueryResult result = new CompleteQueryResult(names, rows);
        CompleteQueryResult fromBuilder = QueryResults.builder()
                .columnNames(names)
                .row(rows[0])
                .row(rows[1])
                .row(rows[2])
                .build();

        AssertUtils.assertRows(result, fromBuilder);
    }

    @Test
    public void notEqualLength()
    {
        String[] names = { "fname", "lname"};
        Object[][] rows = {
                new Object[] { "david", "capwell"},
                new Object[] { "alex", "petrov"},
                new Object[] { "dinesh", "joshi"},
        };
        CompleteQueryResult result = new CompleteQueryResult(names, rows);
        CompleteQueryResult fromBuilder = QueryResults.builder()
                .columnNames(names)
                .row(rows[0])
                .row(rows[1])
                .row(rows[2])
                .row("chris", "lohfink")
                .build();

        assertThatThrownBy(() -> AssertUtils.assertRows(result, fromBuilder))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected: ")
                .hasMessageContaining("Actual: ");
    }

    @Test
    public void notEqualColumnLength()
    {
        String[] names = { "fname", "lname"};
        Object[][] rows = {
                new Object[] { "david", "capwell"},
                new Object[] { "alex", "petrov"},
                new Object[] { "dinesh", "joshi"},
        };
        CompleteQueryResult result = new CompleteQueryResult(names, rows);
        CompleteQueryResult fromBuilder = QueryResults.builder()
                .columnNames("fname")
                .row("david")
                .row("alex")
                .row("dinesh")
                .build();

        assertThatThrownBy(() -> AssertUtils.assertRows(result, fromBuilder))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected: ")
                .hasMessageContaining("Actual: ");
    }

    @Test
    public void notEqualContent()
    {
        String[] names = { "fname", "lname"};
        Object[][] rows = {
                new Object[] { "david", "capwell"},
                new Object[] { "alex", "petrov"},
                new Object[] { "dinesh", "joshi"},
        };
        CompleteQueryResult result = new CompleteQueryResult(names, rows);
        CompleteQueryResult fromBuilder = QueryResults.builder()
                .columnNames(names)
                .row("david", "Capwell")
                .row("alex", "Petrov")
                .row("dinesh", "Joshi")
                .build();

        assertThatThrownBy(() -> AssertUtils.assertRows(result, fromBuilder))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Expected: ")
                .hasMessageContaining("Actual: ");
    }

    @Test
    public void completeFilter()
    {
        CompleteQueryResult qr = QueryResults.builder()
                .row(1, 2, 3, 4)
                .row(5, 6, 7, 7)
                .row(1, 2, 4, 8)
                .row(2, 4, 6, 12)
                .build();

        CompleteQueryResult filtered = qr.filter(row -> row.getInteger(0).intValue() != 1);

        AssertUtils.assertRows(filtered, QueryResults.builder()
                .row(5, 6, 7, 7)
                .row(2, 4, 6, 12)
                .build());
    }

    @Test
    public void completeMap()
    {
        CompleteQueryResult qr = QueryResults.builder()
                .row(1, 2, 3, 4)
                .row(5, 6, 7, 7)
                .row(1, 2, 4, 8)
                .row(2, 4, 6, 12)
                .build();

        Iterator<Integer> it = qr.map(r -> r.getInteger(0));
        List<Integer> result = new ArrayList<>(4);
        it.forEachRemaining(result::add);

        assertThat(result).isEqualTo(Arrays.asList(1, 5, 1, 2));
    }
}