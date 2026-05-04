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
    private final long maxChildAllocation;

    /**
     * Creates a new ArrowBufferPool.
     *
     * @param settings node settings used to derive the maximum native allocation
     */
    public ArrowBufferPool(Settings settings) {
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
    }

    /** Root allocator limit in bytes. Immutable after construction. */
    public long getRootAllocatorLimit() {
        return rootAllocatorLimit;
    }

    /** Child allocator limit in bytes. Final in the current implementation; restart to change. */
    public long getMaxChildAllocation() {
        return maxChildAllocation;
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
        rootAllocator.close();
    }

    private static long getMaxAllocationInBytes(Settings settings) {
        long totalAvailableMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        RatioValue ratio = RatioValue.parseRatioValue(ParquetSettings.MAX_NATIVE_ALLOCATION.get(settings));
        return (long) (totalAvailableMemory * ratio.getAsRatio());
    }
}
