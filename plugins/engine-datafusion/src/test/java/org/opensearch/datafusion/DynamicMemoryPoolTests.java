/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.datafusion.core.DataFusionRuntimeEnv;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_MEMORY_POOL_CONFIGURATION;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION;
import static org.opensearch.datafusion.search.cache.CacheSettings.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for dynamic memory pool limit changes via JNI.
 *
 * Proves that the DynamicLimitPool can:
 * 1. Report its current limit
 * 2. Have its limit changed at runtime
 * 3. Report current and peak memory usage
 *
 * Run with: ./gradlew :plugins:engine-datafusion:test --tests
 *   "org.opensearch.datafusion.DynamicMemoryPoolTests" -Dtest.native.enabled=true
 */
public class DynamicMemoryPoolTests extends OpenSearchTestCase {

    private static DataFusionService service;

    @Mock
    private ClusterService clusterService;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        clusterService = mock(ClusterService.class);

        Set<org.opensearch.common.settings.Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(STATISTICS_CACHE_ENABLED);
        clusterSettingsToAdd.add(STATISTICS_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(STATISTICS_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(DATAFUSION_MEMORY_POOL_CONFIGURATION);
        clusterSettingsToAdd.add(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);

        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        service = new DataFusionService(Collections.emptyMap(), clusterService, "/tmp");
        service.doStart();
    }

    @AfterClass
    public static void cleanUp() {
        if (service != null) {
            service.doStop();
        }
    }

    /**
     * Test 1: Verify we can read the initial pool limit.
     * The default is 10 GB (from DATAFUSION_MEMORY_POOL_CONFIGURATION).
     */
    public void testGetInitialPoolLimit() {
        long runtimePtr = service.getRuntimePointer();
        assertTrue("Runtime pointer should be non-zero", runtimePtr != 0);

        long limit = NativeBridge.getMemoryPoolLimit(runtimePtr);
        long expectedDefault = 10L * 1024 * 1024 * 1024; // 10 GB default
        assertEquals("Initial pool limit should be 10 GB", expectedDefault, limit);

        System.out.println("=== Test 1: Initial pool limit ===");
        System.out.println("  Limit: " + (limit / (1024 * 1024)) + " MB");
    }

    /**
     * Test 2: Verify we can read current and peak usage.
     * With no queries running, usage should be 0 or very small.
     */
    public void testGetMemoryUsage() {
        long runtimePtr = service.getRuntimePointer();

        long currentUsage = NativeBridge.getMemoryPoolCurrentUsage(runtimePtr);
        long peakUsage = NativeBridge.getMemoryPoolPeakUsage(runtimePtr);

        System.out.println("=== Test 2: Memory usage (idle) ===");
        System.out.println("  Current: " + currentUsage + " bytes");
        System.out.println("  Peak:    " + peakUsage + " bytes");

        assertTrue("Current usage should be >= 0", currentUsage >= 0);
        assertTrue("Peak usage should be >= current", peakUsage >= currentUsage);
    }

    /**
     * Test 3: Verify we can change the pool limit at runtime.
     * This is the core of the dynamic memory allocation POC.
     */
    public void testSetPoolLimit() {
        long runtimePtr = service.getRuntimePointer();

        // Read initial limit
        long initialLimit = NativeBridge.getMemoryPoolLimit(runtimePtr);
        System.out.println("=== Test 3: Dynamic limit change ===");
        System.out.println("  Initial limit: " + (initialLimit / (1024 * 1024)) + " MB");

        // Increase limit to 20 GB
        long newLimit = 20L * 1024 * 1024 * 1024;
        NativeBridge.setMemoryPoolLimit(runtimePtr, newLimit);
        long afterIncrease = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals("Limit should be 20 GB after increase", newLimit, afterIncrease);
        System.out.println("  After increase: " + (afterIncrease / (1024 * 1024)) + " MB");

        // Decrease limit to 5 GB
        long smallerLimit = 5L * 1024 * 1024 * 1024;
        NativeBridge.setMemoryPoolLimit(runtimePtr, smallerLimit);
        long afterDecrease = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals("Limit should be 5 GB after decrease", smallerLimit, afterDecrease);
        System.out.println("  After decrease: " + (afterDecrease / (1024 * 1024)) + " MB");

        // Restore original limit
        NativeBridge.setMemoryPoolLimit(runtimePtr, initialLimit);
        long afterRestore = NativeBridge.getMemoryPoolLimit(runtimePtr);
        assertEquals("Limit should be restored", initialLimit, afterRestore);
        System.out.println("  After restore:  " + (afterRestore / (1024 * 1024)) + " MB");
        System.out.println("  RESULT: Dynamic limit change works!");
    }

    /**
     * Test 4: Verify that passing 0 as runtime pointer doesn't crash.
     */
    public void testNullSafety() {
        assertEquals("Usage with null ptr should be 0", 0, NativeBridge.getMemoryPoolCurrentUsage(0));
        assertEquals("Peak with null ptr should be 0", 0, NativeBridge.getMemoryPoolPeakUsage(0));
        assertEquals("Limit with null ptr should be 0", 0, NativeBridge.getMemoryPoolLimit(0));
        // setMemoryPoolLimit with 0 ptr should not crash
        NativeBridge.setMemoryPoolLimit(0, 1024);
        System.out.println("=== Test 4: Null safety — all passed ===");
    }
}
