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

public class SpillStatsTests extends OpenSearchTestCase {

    public void testFieldsAndAccessors() {
        SpillStats s = new SpillStats(1024L, 4096L, "/tmp/spill");
        assertEquals(1024L, s.getUsedBytes());
        assertEquals(4096L, s.getLimitBytes());
        assertEquals("/tmp/spill", s.getDirectory());
    }

    public void testToXContentRendersAllFields() throws IOException {
        SpillStats s = new SpillStats(1024L, 4096L, "/var/df-spill");
        XContentBuilder b = XContentFactory.jsonBuilder().startObject();
        s.toXContent(b, ToXContent.EMPTY_PARAMS);
        b.endObject();
        String json = b.toString();
        assertTrue(json.contains("\"used_bytes\":1024"));
        assertTrue(json.contains("\"limit_bytes\":4096"));
        assertTrue(json.contains("\"directory\":\"/var/df-spill\""));
    }

    public void testStreamRoundTrip() throws IOException {
        SpillStats original = new SpillStats(1L << 30, 1L << 32, "/data/spill");
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        SpillStats deserialized = new SpillStats(in);
        assertEquals(original, deserialized);
    }

    public void testEqualsAndHashCode() {
        SpillStats a = new SpillStats(10, 20, "/x");
        SpillStats b = new SpillStats(10, 20, "/x");
        SpillStats c = new SpillStats(10, 20, "/y");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
