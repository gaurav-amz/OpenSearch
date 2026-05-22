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
 * {@link Writeable} + {@link ToXContentFragment} container for DataFusion's
 * disk-spill state.
 *
 * <p>{@code usedBytes} is the on-disk size of the spill working set — the sum of
 * regular file sizes inside the spill directory tree at sample time. It captures
 * what DataFusion's {@code DiskManager} has actually written. Limitations:
 * <ul>
 *   <li>Files unlinked but still held open by the process are not counted (rare,
 *       since {@code DiskManager} cleans up explicitly when reservations drop).</li>
 *   <li>Sparse files are billed by their apparent size, not their on-disk extent.</li>
 *   <li>The walk is best-effort — IO errors are swallowed so a transient permission
 *       blip doesn't crash a stats request.</li>
 * </ul>
 */
public class SpillStats implements Writeable, ToXContentFragment {

    private final long usedBytes;
    private final long limitBytes;
    private final String directory;

    /**
     * Construct from raw counters.
     *
     * @param usedBytes  bytes currently occupied by spill files on disk
     * @param limitBytes spill upper bound (datafusion.spill_memory_limit_bytes)
     * @param directory  absolute path of the spill directory
     */
    public SpillStats(long usedBytes, long limitBytes, String directory) {
        this.usedBytes = usedBytes;
        this.limitBytes = limitBytes;
        this.directory = Objects.requireNonNull(directory);
    }

    /**
     * Deserialize from stream.
     */
    public SpillStats(StreamInput in) throws IOException {
        this.usedBytes = in.readLong();
        this.limitBytes = in.readLong();
        this.directory = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(usedBytes);
        out.writeLong(limitBytes);
        out.writeString(directory);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("used_bytes", usedBytes);
        builder.field("limit_bytes", limitBytes);
        builder.field("directory", directory);
        return builder;
    }

    /** Bytes currently occupied by spill files on disk. */
    public long getUsedBytes() {
        return usedBytes;
    }

    /** Spill upper bound. */
    public long getLimitBytes() {
        return limitBytes;
    }

    /** Spill directory path. */
    public String getDirectory() {
        return directory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpillStats that = (SpillStats) o;
        return usedBytes == that.usedBytes && limitBytes == that.limitBytes && directory.equals(that.directory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(usedBytes, limitBytes, directory);
    }
}
