/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.memory;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowBufferPoolTests extends OpenSearchTestCase {

    public void testAllocatedBytesIncreasesOnAllocation() {
        try (ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY)) {
            BufferAllocator child = pool.createChildAllocator("alloc-test");
            assertNotNull(child);
            assertEquals(0, pool.getTotalAllocatedBytes());
            ArrowBuf buf = child.buffer(1024);
            assertTrue(pool.getTotalAllocatedBytes() > 0);
            buf.close();
            child.close();
        }
    }

    public void testMultipleChildAllocators() {
        try (ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY)) {
            BufferAllocator c1 = pool.createChildAllocator("c1");
            BufferAllocator c2 = pool.createChildAllocator("c2");
            ArrowBuf b1 = c1.buffer(512);
            ArrowBuf b2 = c2.buffer(512);
            assertTrue(pool.getTotalAllocatedBytes() >= 1024);
            b1.close();
            b2.close();
            c1.close();
            c2.close();
        }
    }

    public void testAllocatedBytesDecreasesAfterFree() {
        try (ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY)) {
            BufferAllocator child = pool.createChildAllocator("free-test");
            ArrowBuf buf = child.buffer(1024);
            assertTrue(pool.getTotalAllocatedBytes() > 0);
            buf.close();
            child.close();
            assertEquals(0, pool.getTotalAllocatedBytes());
        }
    }

    public void testCloseWithOpenChildAllocatorThrows() {
        ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY);
        BufferAllocator child = pool.createChildAllocator("leaked-child");
        ArrowBuf buf = child.buffer(1024);
        // Closing pool with outstanding child allocations throws
        IllegalStateException e = expectThrows(IllegalStateException.class, pool::close);
        assertTrue(e.getMessage().contains("Memory was leaked"));
        // First close marked root as closed, so child.close() can't notify parent.
        // Release buffer directly — Arrow frees the underlying memory even if allocator is closed.
        buf.getReferenceManager().release();
    }

    // ---- New tests for configurable child allocator limit ----

    public void testDefaultChildAllocatorIsOneGiB() {
        try (ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY)) {
            assertEquals("Default child allocator should be 1 GiB", ByteSizeUnit.GB.toBytes(1), pool.getMaxChildAllocation());
        }
    }

    public void testCustomChildAllocatorRespected() {
        Settings settings = Settings.builder()
            .put("parquet.max_native_allocation", "50%")
            .put(ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
            .build();
        try (ArrowBufferPool pool = new ArrowBufferPool(settings)) {
            assertEquals("Custom child allocator should be 512 MiB", ByteSizeUnit.MB.toBytes(512), pool.getMaxChildAllocation());
        }
    }

    public void testChildAllocatorOverHalfRootIsRejected() {
        // Force the root to ~1% of native so the 1 GiB default child exceeds root/2 on most boxes.
        // Prefer an explicit tiny override so the test is deterministic across machines.
        Settings settings = Settings.builder()
            .put("parquet.max_native_allocation", "0.001%")
            .put(ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.getKey(), new ByteSizeValue(1, ByteSizeUnit.GB))
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ArrowBufferPool(settings));
        assertTrue("Error should mention the over-commit", e.getMessage().contains("exceeds half of the root allocator limit"));
    }

    public void testArrowChildAllocatorBytesIsFinal() {
        assertFalse(
            "parquet.write.arrow_child_allocator_bytes should be Final (restart to change)",
            ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.isDynamic()
        );
        assertTrue(ParquetSettings.ARROW_CHILD_ALLOCATOR_BYTES.hasNodeScope());
    }

    public void testMaxRowsPerVsrIsDynamic() {
        assertTrue(
            "parquet.max_rows_per_vsr should be Dynamic so new ParquetWriters pick up updates",
            ParquetSettings.MAX_ROWS_PER_VSR.isDynamic()
        );
        assertTrue(ParquetSettings.MAX_ROWS_PER_VSR.hasNodeScope());
    }
}
