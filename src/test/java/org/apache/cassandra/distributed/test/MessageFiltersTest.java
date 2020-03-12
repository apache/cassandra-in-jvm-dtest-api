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

package org.apache.cassandra.distributed.test;

import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.MessageFilters;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageFiltersTest
{
    @Test
    public void simpleInboundFiltersTest()
    {
        simpleFiltersTest(true);
    }

    @Test
    public void simpleOutboundFiltersTest()
    {
        simpleFiltersTest(false);
    }

    private interface Permit
    {
        boolean test(int from, int to, IMessage msg);
    }

    private void simpleFiltersTest(boolean inbound)
    {
        int VERB1 = 1;
        int VERB2 = 2;
        int VERB3 = 3;
        int i1 = 1;
        int i2 = 2;
        int i3 = 3;
        String MSG1 = "msg1";
        String MSG2 = "msg2";

        MessageFilters filters = new MessageFilters();
        Permit permit = inbound ? filters::permitInbound : filters::permitOutbound;

        IMessageFilters.Filter filter = filters.allVerbs().inbound(inbound).from(1).drop();
        Assert.assertFalse(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertFalse(permit.test(i1, i2, msg(VERB2, MSG1)));
        Assert.assertFalse(permit.test(i1, i2, msg(VERB3, MSG1)));
        Assert.assertTrue(permit.test(i2, i1, msg(VERB1, MSG1)));
        filter.off();
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        filters.reset();

        filters.verbs(VERB1).inbound(inbound).from(1).to(2).drop();
        Assert.assertFalse(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i2, msg(VERB2, MSG1)));
        Assert.assertTrue(permit.test(i2, i1, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i2, i3, msg(VERB2, MSG1)));

        filters.reset();
        AtomicInteger counter = new AtomicInteger();
        filters.verbs(VERB1).inbound(inbound).from(1).to(2).messagesMatching((from, to, msg) -> {
            counter.incrementAndGet();
            return Arrays.equals(msg.bytes(), MSG1.getBytes());
        }).drop();
        Assert.assertFalse(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertEquals(counter.get(), 1);
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG2)));
        Assert.assertEquals(counter.get(), 2);

        // filter chain gets interrupted because a higher level filter returns no match
        Assert.assertTrue(permit.test(i2, i1, msg(VERB1, MSG1)));
        Assert.assertEquals(counter.get(), 2);
        Assert.assertTrue(permit.test(i2, i1, msg(VERB2, MSG1)));
        Assert.assertEquals(counter.get(), 2);
        filters.reset();

        filters.allVerbs().inbound(inbound).from(3, 2).to(2, 1).drop();
        Assert.assertFalse(permit.test(i3, i1, msg(VERB1, MSG1)));
        Assert.assertFalse(permit.test(i3, i2, msg(VERB1, MSG1)));
        Assert.assertFalse(permit.test(i2, i1, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i2, i3, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i3, msg(VERB1, MSG1)));
        filters.reset();

        counter.set(0);
        filters.allVerbs().inbound(inbound).from(1).to(2).messagesMatching((from, to, msg) -> {
            counter.incrementAndGet();
            return false;
        }).drop();
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i3, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertEquals(2, counter.get());
    }

    IMessage msg(int verb, String msg)
    {
        return new IMessage()
        {
            public int verb() { return verb; }
            public byte[] bytes() { return msg.getBytes(); }
            public int id() { return 0; }
            public int version() { return 0;  }
            public InetSocketAddress from() { return null; }
            public int fromPort()
            {
                return 0;
            }
        };
    }
}