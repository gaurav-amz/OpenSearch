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
import org.opensearch.plugin.stats.PluginStats;

import java.io.IOException;
import java.util.Objects;

/**
 * Top-level stats container for the DataFusion backend.
 *
 * <p>Implements {@link PluginStats} for Mustang Stats Framework compatibility,
 * {@link Writeable} for transport serialization, and {@link ToXContentFragment}
 * for JSON rendering.
 *
 * <p>Composes three nullable sub-records: native executor metrics
 * ({@link NativeExecutorsStats}), memory-pool counters ({@link MemoryPoolStats}),
 * and spill counters ({@link SpillStats}). Each is rendered under its own JSON
 * key when present and omitted when null.
 */
public class DataFusionStats implements PluginStats, Writeable, ToXContentFragment {

    private final NativeExecutorsStats nativeExecutorsStats; // nullable
    private final MemoryPoolStats memoryPoolStats; // nullable
    private final SpillStats spillStats; // nullable

    /**
     * Backward-compatible constructor — native executor metrics only.
     */
    public DataFusionStats(NativeExecutorsStats nativeExecutorsStats) {
        this(nativeExecutorsStats, null, null);
    }

    /**
     * Construct from all components.
     *
     * @param nativeExecutorsStats native executor metrics (nullable)
     * @param memoryPoolStats      memory-pool counters (nullable)
     * @param spillStats           spill counters (nullable)
     */
    public DataFusionStats(NativeExecutorsStats nativeExecutorsStats, MemoryPoolStats memoryPoolStats, SpillStats spillStats) {
        this.nativeExecutorsStats = nativeExecutorsStats;
        this.memoryPoolStats = memoryPoolStats;
        this.spillStats = spillStats;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionStats(StreamInput in) throws IOException {
        this.nativeExecutorsStats = in.readOptionalWriteable(NativeExecutorsStats::new);
        this.memoryPoolStats = in.readOptionalWriteable(MemoryPoolStats::new);
        this.spillStats = in.readOptionalWriteable(SpillStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(nativeExecutorsStats);
        out.writeOptionalWriteable(memoryPoolStats);
        out.writeOptionalWriteable(spillStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (nativeExecutorsStats != null) {
            nativeExecutorsStats.toXContent(builder, params);
        }
        if (memoryPoolStats != null) {
            builder.startObject("memory_pool");
            memoryPoolStats.toXContent(builder, params);
            builder.endObject();
        }
        if (spillStats != null) {
            builder.startObject("spill");
            spillStats.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    /**
     * Returns the native executor metrics, or {@code null} if absent.
     */
    public NativeExecutorsStats getNativeExecutorsStats() {
        return nativeExecutorsStats;
    }

    /**
     * Returns the memory-pool stats, or {@code null} if absent.
     */
    public MemoryPoolStats getMemoryPoolStats() {
        return memoryPoolStats;
    }

    /**
     * Returns the spill stats, or {@code null} if absent.
     */
    public SpillStats getSpillStats() {
        return spillStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionStats that = (DataFusionStats) o;
        return Objects.equals(nativeExecutorsStats, that.nativeExecutorsStats)
            && Objects.equals(memoryPoolStats, that.memoryPoolStats)
            && Objects.equals(spillStats, that.spillStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativeExecutorsStats, memoryPoolStats, spillStats);
    }
}
