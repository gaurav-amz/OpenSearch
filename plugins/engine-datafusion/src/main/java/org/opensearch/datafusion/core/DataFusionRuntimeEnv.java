/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;

import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.GlobalRuntimeHandle;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheUtils;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;

/**
 * DataFusion runtime environment manager.
 * Manages the lifecycle of native DataFusion runtime (includes memory pool and Tokio runtime).
 */
public final class DataFusionRuntimeEnv implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(DataFusionRuntimeEnv.class);

    private final GlobalRuntimeHandle runtimeHandle;

    private CacheManager cacheManager;

    /**
     * Returns the estimated available native memory (total physical - JVM heap).
     * Used to compute percentage-based defaults for native memory settings.
     * Returns the value as a whole-byte string to avoid fractional byte size parsing errors.
     */
    static long getAvailableNativeMemory() {
        long totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long jvmHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        return Math.max(totalPhysical - jvmHeap, 0);
    }

    /**
     * Controls the memory used for the datafusion query execution.
     * Default: 10% of available native memory (total physical - JVM heap).
     * Accepts absolute values ("10gb").
     * Dynamic: can be changed at runtime via cluster settings API.
     */
    public static final Setting<ByteSizeValue> DATAFUSION_MEMORY_POOL_CONFIGURATION = Setting.byteSizeSetting(
        "datafusion.search.memory_pool",
        settings -> (long) (getAvailableNativeMemory() * 0.10) + ByteSizeUnit.BYTES.getSuffix(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Controls the spill disk space used for the datafusion query execution.
     * Default: 20% of available native memory (total physical - JVM heap).
     * Dynamic: can be changed at runtime via cluster settings API.
     * Note: DiskManager runtime enforcement is future work.
     */
    public static final Setting<ByteSizeValue> DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION = Setting.byteSizeSetting(
        "datafusion.spill.memory_limit",
        settings -> (long) (getAvailableNativeMemory() * 0.20) + ByteSizeUnit.BYTES.getSuffix(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Creates a new DataFusion runtime environment.
     */
    public DataFusionRuntimeEnv(ClusterService clusterService, String spill_dir) {
        long memoryLimit = clusterService.getClusterSettings().get(DATAFUSION_MEMORY_POOL_CONFIGURATION).getBytes();
        long spillLimit = clusterService.getClusterSettings().get(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION).getBytes();
        long cacheManagerConfigPtr = CacheUtils.createCacheConfig(clusterService.getClusterSettings());
        NativeBridge.initTokioRuntimeManager(Runtime.getRuntime().availableProcessors());
        this.runtimeHandle = new GlobalRuntimeHandle(memoryLimit, cacheManagerConfigPtr, spill_dir, spillLimit);
        logger.info("DataFusion runtime created with memory pool limit: {} bytes", memoryLimit);
        this.cacheManager = new CacheManager(this.runtimeHandle);

        // Register dynamic settings listener for memory pool limit changes
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            DATAFUSION_MEMORY_POOL_CONFIGURATION,
            newValue -> {
                long newLimitBytes = newValue.getBytes();
                long runtimePtr = runtimeHandle.getPointer();
                if (runtimePtr != 0) {
                    NativeBridge.setMemoryPoolLimit(runtimePtr, newLimitBytes);
                    logger.info("DataFusion memory pool limit updated to {} bytes via cluster settings", newLimitBytes);
                }
            }
        );

        // Register dynamic settings listener for spill limit changes
        // Note: DiskManager doesn't support runtime limit changes yet.
        // This listener logs the change for observability. Full runtime enforcement
        // requires a DiskManager handle (future work).
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION,
            newValue -> {
                long newLimitBytes = newValue.getBytes();
                logger.info("DataFusion spill limit setting updated to {} bytes. "
                    + "Note: DiskManager limit change requires restart to take full effect.", newLimitBytes);
            }
        );
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return runtimeHandle.getPointer();
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        runtimeHandle.close();
        NativeBridge.shutdownTokioRuntimeManager();
    }
}
