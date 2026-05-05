/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.parquet.ParquetSettings;

import java.io.Closeable;
import java.util.function.Function;

/**
 * Arrow memory allocator pool with configurable limits derived from node settings.
 *
 * <p>Wraps an Apache Arrow {@link RootAllocator} whose maximum allocation is computed as a
 * percentage of available non-heap system memory (total physical memory minus JVM max heap),
 * controlled by the {@code parquet.max_native_allocation} setting (default {@code "10%"}).
 *
 * <p>Child allocators are created per {@link org.opensearch.parquet.vsr.ManagedVSR} instance,
 * each limited to 1/10th of the root allocation, providing memory isolation between batches.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final RootAllocator rootAllocator;
    private final long rootAllocatorLimit;
    /** Volatile so a settings-listener update on one thread is visible to createChildAllocator() on another. */
    private volatile long maxChildAllocation;
    /**
     * Callback to deregister this pool from the plugin's registry when close() is called.
     * Null when constructed without a registrar (unit tests that don't care about dynamism).
     */
    private final Runnable deregister;

    /**
     * Creates a new ArrowBufferPool without plugin-managed dynamic-settings wiring.
     * Used by tests; production code should use the two-arg constructor so the pool
     * receives {@code parquet.write.arrow_child_allocator_bytes} updates.
     */
    public ArrowBufferPool(Settings settings) {
        this(settings, null);
    }

    /**
     * Creates a new ArrowBufferPool and registers it with the plugin's registry so the
     * child-allocator limit can be updated at runtime via the cluster settings API.
     *
     * @param settings   node settings used to derive the root allocator limit + initial child limit
     * @param registrar  registration function; when non-null, the pool is registered on construction
     *                   and the returned {@link Runnable} is invoked on {@link #close()} to deregister
     */
    public ArrowBufferPool(Settings settings, Function<ArrowBufferPool, Runnable> registrar) {
        long maxAllocationInBytes = getMaxAllocationInBytes(settings);
        logger.debug("Max native memory allocation for ArrowBufferPool: {} bytes", maxAllocationInBytes);
        this.rootAllocator = new RootAllocator(maxAllocationInBytes);
        this.rootAllocatorLimit = maxAllocationInBytes;

        // Child allocator: read from setting, cap at root/2 so a single VSR cannot starve the pool.
        long configuredChild = ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.get(settings).getBytes();
        long maxAllowedChild = maxAllocationInBytes / 2;
        if (configuredChild > maxAllowedChild) {
            throw new IllegalArgumentException(
                "parquet.write.arrow_child_allocator_bytes ("
                    + configuredChild
                    + ") exceeds half of the root allocator limit ("
                    + maxAllowedChild
                    + "); reduce the child limit or raise parquet.max_native_allocation"
            );
        }
        this.maxChildAllocation = configuredChild;
        logger.debug("Arrow child allocator limit: {} bytes", this.maxChildAllocation);

        this.deregister = (registrar != null) ? registrar.apply(this) : () -> {};
    }

    /** Root allocator limit in bytes. Immutable after construction. */
    public long getRootAllocatorLimit() {
        return rootAllocatorLimit;
    }

    /** Child allocator limit in bytes. Volatile — updated dynamically via cluster settings. */
    public long getMaxChildAllocation() {
        return maxChildAllocation;
    }

    /**
     * Updates the per-VSR child allocator limit. New child allocators created after this call
     * (i.e., after the next VSR rotation) will use the new limit; existing child allocators keep
     * their construction-time limit until their VSR is closed.
     *
     * <p>Throws {@link IllegalArgumentException} if the requested limit exceeds half the root
     * allocator size, so cluster-state updates that would starve other writers are rejected by
     * the settings API rather than silently clamped.
     *
     * @param newBytes the new per-VSR child allocator limit in bytes
     * @throws IllegalArgumentException if {@code newBytes > rootAllocatorLimit / 2}
     */
    public void updateMaxChildAllocation(long newBytes) {
        long maxAllowed = rootAllocatorLimit / 2;
        if (newBytes > maxAllowed) {
            throw new IllegalArgumentException(
                "parquet.write.arrow_child_allocator_bytes ("
                    + newBytes
                    + ") exceeds half of the root allocator limit ("
                    + maxAllowed
                    + "); reduce the child limit or raise parquet.max_native_allocation"
            );
        }
        long old = this.maxChildAllocation;
        this.maxChildAllocation = newBytes;
        logger.info("Arrow child allocator limit updated: {} -> {} bytes", old, newBytes);
    }

    /**
     * Creates a child allocator with the given name.
     * @param name the allocator name
     * @return a new child buffer allocator
     */
    public BufferAllocator createChildAllocator(String name) {
        return rootAllocator.newChildAllocator(name, 0, maxChildAllocation);
    }

    /** Returns the total bytes currently allocated by the root allocator. */
    public long getTotalAllocatedBytes() {
        return rootAllocator.getAllocatedMemory();
    }

    @Override
    public void close() {
        try {
            deregister.run();
        } finally {
            rootAllocator.close();
        }
    }

    private static long getMaxAllocationInBytes(Settings settings) {
        long totalAvailableMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        RatioValue ratio = RatioValue.parseRatioValue(ParquetSettings.MAX_NATIVE_ALLOCATION.get(settings));
        return (long) (totalAvailableMemory * ratio.getAsRatio());
    }
}
