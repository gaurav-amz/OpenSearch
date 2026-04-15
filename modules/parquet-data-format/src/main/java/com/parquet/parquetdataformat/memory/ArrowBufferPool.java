package com.parquet.parquetdataformat.memory;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import com.parquet.parquetdataformat.ParquetSettings;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;

import java.io.Closeable;

/**
 * Manages BufferAllocator lifecycle with configurable allocation strategies.
 * Provides factory methods for creating allocators with different policies
 * based on OpenSearch settings and memory pressure conditions.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final RootAllocator rootAllocator;
    private volatile long maxChildAllocation;
    private final long rootAllocatorLimit;

    public ArrowBufferPool(Settings settings) {
        // Root allocator: use the setting value (which defaults to 10% of native memory)
        long maxAllocationInBytes = ParquetSettings.ARROW_POOL_BYTES.get(settings).getBytes();
        this.rootAllocatorLimit = maxAllocationInBytes;

        // Child allocator: use setting value
        this.maxChildAllocation = ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.get(settings).getBytes();

        logger.info("ArrowBufferPool root allocator limit: {} bytes, child allocator limit: {} bytes",
            maxAllocationInBytes, maxChildAllocation);
        this.rootAllocator = new RootAllocator(maxAllocationInBytes);
    }

    /**
     * Creates a new child allocator with the configured strategy and limits.
     *
     * @param name Unique name for the allocator
     * @return BufferAllocator configured with pool settings
     */
    public BufferAllocator createChildAllocator(String name) {
        return createChildAllocator(name, maxChildAllocation);
    }

    /**
     * Creates a new child allocator with custom limits.
     *
     * @param name     Unique name for the allocator
     * @param maxAllocation Maximum allocation limit
     * @return BufferAllocator configured with specified limits
     */
    private BufferAllocator createChildAllocator(String name, long maxAllocation) {
        return rootAllocator.newChildAllocator(name, 0, maxAllocation);
    }

    public long getTotalAllocatedBytes() {
        return rootAllocator.getAllocatedMemory();
    }

    /**
     * Updates the child allocator limit for new allocators.
     * Existing child allocators keep their old limit.
     * Validates that the new limit doesn't exceed root allocator / 2.
     */
    public void updateMaxChildAllocation(long newMaxChildAllocation) {
        long maxAllowed = rootAllocatorLimit / 2;
        if (newMaxChildAllocation > maxAllowed) {
            logger.warn("Requested child allocator limit {} exceeds max allowed {} (root/2). Capping to max.",
                newMaxChildAllocation, maxAllowed);
            newMaxChildAllocation = maxAllowed;
        }
        this.maxChildAllocation = newMaxChildAllocation;
        logger.info("Arrow child allocator limit updated to {} bytes", this.maxChildAllocation);
    }

    /**
     * Returns the current child allocator limit.
     */
    public long getMaxChildAllocation() {
        return maxChildAllocation;
    }

    /**
     * Returns the root allocator limit (immutable after construction).
     */
    public long getRootAllocatorLimit() {
        return rootAllocatorLimit;
    }

    /**
     * Closes all active allocators and cleans up the pool.
     */
    @Override
    public void close() {
        rootAllocator.close();
    }

    private static long getMaxAllocationInBytes(Settings settings) {
        long totalAvailableSystemMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        RatioValue maxAllocationPercentage = RatioValue.parseRatioValue(settings.get(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), ParquetSettings.DEFAULT_MAX_NATIVE_ALLOCATION));
        return (long) (totalAvailableSystemMemory * maxAllocationPercentage.getAsRatio());
    }
}
