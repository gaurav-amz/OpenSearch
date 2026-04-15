/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_MEMORY_POOL_CONFIGURATION;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION;

/**
 * Tests for the datafusion.spill.memory_limit setting being dynamic.
 */
public class SpillLimitSettingTests extends OpenSearchTestCase {

    /**
     * Verify the spill limit setting is declared as Dynamic (not Final).
     */
    public void testSpillLimitSettingIsDynamic() {
        assertTrue(
            "datafusion.spill.memory_limit should be a dynamic setting",
            DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION.isDynamic()
        );
    }

    /**
     * Verify the spill limit setting is node-scoped.
     */
    public void testSpillLimitSettingIsNodeScoped() {
        assertFalse(
            "datafusion.spill.memory_limit should not be index-scoped",
            DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION.hasIndexScope()
        );
        assertTrue(
            "datafusion.spill.memory_limit should be node-scoped",
            DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION.hasNodeScope()
        );
    }

    /**
     * Verify the default value is 20% of native memory (not a fixed value).
     */
    public void testSpillLimitDefaultValue() {
        long defaultBytes = DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION.getDefault(Settings.EMPTY).getBytes();
        assertTrue("Default spill limit should be positive (20% of native memory)", defaultBytes > 0);
    }

    /**
     * Verify the spill limit setting can be updated via ClusterSettings.
     */
    public void testSpillLimitCanBeUpdatedViaClusterSettings() {
        Set<Setting<?>> settingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        settingsToAdd.add(DATAFUSION_MEMORY_POOL_CONFIGURATION);
        settingsToAdd.add(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsToAdd);

        // Should not throw — setting is dynamic and can be updated
        Settings newSettings = Settings.builder()
            .put("datafusion.spill.memory_limit", "30gb")
            .build();
        clusterSettings.applySettings(newSettings);

        // Verify the value was accepted
        long updatedValue = clusterSettings.get(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION).getBytes();
        assertEquals("Spill limit should be updated to 30 GB", 30L * 1024 * 1024 * 1024, updatedValue);
    }
}
