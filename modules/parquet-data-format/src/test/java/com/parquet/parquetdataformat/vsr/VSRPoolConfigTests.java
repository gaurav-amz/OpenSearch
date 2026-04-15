/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for VSRPool's configurable maxRowsPerVSR.
 */
public class VSRPoolConfigTests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private Schema testSchema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put("parquet.write.arrow_pool_bytes", "1gb")
            .build();
        bufferPool = new ArrowBufferPool(settings);
        testSchema = new Schema(List.of(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ));
    }

    @Override
    public void tearDown() throws Exception {
        if (bufferPool != null) {
            bufferPool.close();
        }
        super.tearDown();
    }

    /**
     * Test that default maxRowsPerVSR is 50000.
     */
    public void testDefaultMaxRowsPerVSR() {
        try (VSRPool pool = new VSRPool("test-pool", testSchema, bufferPool)) {
            assertEquals("Default maxRowsPerVSR should be 50000", 50000, pool.getMaxRowsPerVSR());
        }
    }

    /**
     * Test that maxRowsPerVSR can be updated dynamically.
     */
    public void testUpdateMaxRowsPerVSR() {
        try (VSRPool pool = new VSRPool("test-pool", testSchema, bufferPool)) {
            pool.updateMaxRowsPerVSR(100000);
            assertEquals("maxRowsPerVSR should be updated to 100000", 100000, pool.getMaxRowsPerVSR());
        }
    }

    /**
     * Test that rotation respects the updated maxRowsPerVSR.
     * With a higher threshold, the VSR should not rotate at the old threshold.
     */
    public void testRotationRespectsUpdatedThreshold() throws Exception {
        try (VSRPool pool = new VSRPool("test-pool", testSchema, bufferPool)) {
            // Increase threshold to 100000
            pool.updateMaxRowsPerVSR(100000);

            ManagedVSR activeVSR = pool.getActiveVSR();
            assertNotNull("Active VSR should exist", activeVSR);

            // Set row count to 50000 (old threshold)
            activeVSR.setRowCount(50000);

            // Should NOT rotate because new threshold is 100000
            boolean rotated = pool.maybeRotateActiveVSR();
            assertFalse("Should not rotate at 50000 when threshold is 100000", rotated);
        }
    }

    /**
     * Test that rotation triggers at the new lower threshold.
     */
    public void testRotationTriggersAtLowerThreshold() throws Exception {
        try (VSRPool pool = new VSRPool("test-pool", testSchema, bufferPool)) {
            // Decrease threshold to 10000
            pool.updateMaxRowsPerVSR(10000);

            ManagedVSR activeVSR = pool.getActiveVSR();
            assertNotNull("Active VSR should exist", activeVSR);

            // Set row count to 10000 (new threshold)
            activeVSR.setRowCount(10000);

            // Should rotate because threshold is now 10000
            boolean rotated = pool.maybeRotateActiveVSR();
            assertTrue("Should rotate at 10000 when threshold is 10000", rotated);
        }
    }
}
