/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link Writeable} + {@link ToXContentFragment} container for the DataFusion
 * memory pool's used and limit byte counters.
 *
 * <p>{@code usedBytes} comes directly from {@code DynamicLimitPool::reserved()} via the
 * existing {@code df_get_pool_usage} JNI accessor — it is the pool's own accounting
 * counter, not RSS or any process-level approximation.
 */
public class MemoryPoolStats implements Writeable, ToXContentFragment {

    private final long usedBytes;
    private final long limitBytes;

    /**
     * Construct from raw counters.
     *
     * @param usedBytes  bytes currently reserved in the DataFusion memory pool
     * @param limitBytes the pool's current upper bound (matches the dynamic limit)
     */
    public MemoryPoolStats(long usedBytes, long limitBytes) {
        this.usedBytes = usedBytes;
        this.limitBytes = limitBytes;
    }

    /**
     * Deserialize from stream.
     */
    public MemoryPoolStats(StreamInput in) throws IOException {
        this.usedBytes = in.readLong();
        this.limitBytes = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(usedBytes);
        out.writeLong(limitBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("used_bytes", usedBytes);
        builder.field("limit_bytes", limitBytes);
        return builder;
    }

    /** Bytes currently reserved in the pool. */
    public long getUsedBytes() {
        return usedBytes;
    }

    /** Pool upper bound. */
    public long getLimitBytes() {
        return limitBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryPoolStats that = (MemoryPoolStats) o;
        return usedBytes == that.usedBytes && limitBytes == that.limitBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(usedBytes, limitBytes);
    }
}
