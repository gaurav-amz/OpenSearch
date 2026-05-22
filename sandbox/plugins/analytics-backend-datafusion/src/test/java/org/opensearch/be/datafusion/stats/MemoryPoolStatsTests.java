/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class MemoryPoolStatsTests extends OpenSearchTestCase {

    public void testFieldsAndAccessors() {
        MemoryPoolStats s = new MemoryPoolStats(123L, 456L);
        assertEquals(123L, s.getUsedBytes());
        assertEquals(456L, s.getLimitBytes());
    }

    public void testToXContentRendersBothFields() throws IOException {
        MemoryPoolStats s = new MemoryPoolStats(123L, 456L);
        XContentBuilder b = XContentFactory.jsonBuilder().startObject();
        s.toXContent(b, ToXContent.EMPTY_PARAMS);
        b.endObject();
        String json = b.toString();
        assertTrue(json.contains("\"used_bytes\":123"));
        assertTrue(json.contains("\"limit_bytes\":456"));
    }

    public void testStreamRoundTrip() throws IOException {
        MemoryPoolStats original = new MemoryPoolStats(7777L, 8888L);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MemoryPoolStats deserialized = new MemoryPoolStats(in);
        assertEquals(original, deserialized);
    }

    public void testEqualsAndHashCode() {
        MemoryPoolStats a = new MemoryPoolStats(1, 2);
        MemoryPoolStats b = new MemoryPoolStats(1, 2);
        MemoryPoolStats c = new MemoryPoolStats(1, 3);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
