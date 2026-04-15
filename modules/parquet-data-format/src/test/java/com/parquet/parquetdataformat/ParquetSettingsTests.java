/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for new Parquet memory settings: arrow_pool_bytes, arrow_child_allocator_bytes, max_rows_per_vsr.
 */
public class ParquetSettingsTests extends OpenSearchTestCase {

    // --- Arrow Pool Bytes (4.1) ---

    public void testArrowPoolBytesDefaultValue() {
        // Default is 10% of native memory (total physical - JVM heap), not a fixed value
        long defaultBytes = ParquetSettings.ARROW_POOL_BYTES.getDefault(Settings.EMPTY).getBytes();
        assertTrue("Default arrow pool should be positive (10% of native memory)", defaultBytes > 0);
    }

    public void testArrowPoolBytesIsFinal() {
        assertFalse(
            "parquet.write.arrow_pool_bytes should NOT be dynamic (it's Final)",
            ParquetSettings.ARROW_POOL_BYTES.isDynamic()
        );
    }

    public void testArrowPoolBytesIsNodeScoped() {
        assertTrue(
            "parquet.write.arrow_pool_bytes should be node-scoped",
            ParquetSettings.ARROW_POOL_BYTES.hasNodeScope()
        );
    }

    public void testArrowPoolBytesCustomValue() {
        Settings settings = Settings.builder()
            .put("parquet.write.arrow_pool_bytes", "5gb")
            .build();
        long value = ParquetSettings.ARROW_POOL_BYTES.get(settings).getBytes();
        assertEquals("Custom arrow pool should be 5 GB", 5L * 1024 * 1024 * 1024, value);
    }

    // --- Arrow Child Allocator Bytes (4.2) ---

    public void testArrowChildAllocatorBytesDefaultValue() {
        long defaultBytes = ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.getDefault(Settings.EMPTY).getBytes();
        assertEquals("Default child allocator should be 1 GB", 1L * 1024 * 1024 * 1024, defaultBytes);
    }

    public void testArrowChildAllocatorBytesIsDynamic() {
        assertTrue(
            "parquet.write.arrow_child_allocator_bytes should be dynamic",
            ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.isDynamic()
        );
    }

    public void testArrowChildAllocatorBytesIsNodeScoped() {
        assertTrue(
            "parquet.write.arrow_child_allocator_bytes should be node-scoped",
            ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.hasNodeScope()
        );
    }

    // --- Max Rows Per VSR (4.4) ---

    public void testMaxRowsPerVsrDefaultValue() {
        int defaultValue = ParquetSettings.MAX_ROWS_PER_VSR.getDefault(Settings.EMPTY);
        assertEquals("Default max rows per VSR should be 50000", 50000, defaultValue);
    }

    public void testMaxRowsPerVsrIsDynamic() {
        assertTrue(
            "index.parquet.max_rows_per_vsr should be dynamic",
            ParquetSettings.MAX_ROWS_PER_VSR.isDynamic()
        );
    }

    public void testMaxRowsPerVsrIsIndexScoped() {
        assertTrue(
            "index.parquet.max_rows_per_vsr should be index-scoped",
            ParquetSettings.MAX_ROWS_PER_VSR.hasIndexScope()
        );
    }

    public void testMaxRowsPerVsrMinimumBound() {
        // Values below 1000 should be rejected
        Settings settings = Settings.builder()
            .put("index.parquet.max_rows_per_vsr", 500)
            .build();
        expectThrows(IllegalArgumentException.class, () -> ParquetSettings.MAX_ROWS_PER_VSR.get(settings));
    }

    public void testMaxRowsPerVsrMaximumBound() {
        // Values above 1000000 should be rejected
        Settings settings = Settings.builder()
            .put("index.parquet.max_rows_per_vsr", 2000000)
            .build();
        expectThrows(IllegalArgumentException.class, () -> ParquetSettings.MAX_ROWS_PER_VSR.get(settings));
    }

    public void testMaxRowsPerVsrValidCustomValue() {
        Settings settings = Settings.builder()
            .put("index.parquet.max_rows_per_vsr", 100000)
            .build();
        int value = ParquetSettings.MAX_ROWS_PER_VSR.get(settings);
        assertEquals("Custom max rows should be 100000", 100000, value);
    }
}
