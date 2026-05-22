/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests for {@link DataFusionService#spillDirectorySize(String)} only.
 *
 * <p>{@code spillDirectorySize} is package-private so it can be exercised here
 * without spinning up the native runtime.
 */
public class DataFusionServiceSpillSizeTests extends OpenSearchTestCase {

    public void testNullDirectoryReturnsZero() {
        assertEquals(0L, DataFusionService.spillDirectorySize(null));
    }

    public void testNonexistentDirectoryReturnsZero() throws Exception {
        Path p = createTempDir().resolve("does-not-exist");
        assertEquals(0L, DataFusionService.spillDirectorySize(p.toString()));
    }

    public void testEmptyDirectoryReturnsZero() throws Exception {
        Path dir = createTempDir();
        assertEquals(0L, DataFusionService.spillDirectorySize(dir.toString()));
    }

    public void testSumsRegularFileSizes() throws Exception {
        Path dir = createTempDir();
        Files.write(dir.resolve("a.tmp"), new byte[100]);
        Files.write(dir.resolve("b.tmp"), new byte[250]);
        Path nested = Files.createDirectories(dir.resolve("nested"));
        Files.write(nested.resolve("c.tmp"), new byte[50]);
        assertEquals(400L, DataFusionService.spillDirectorySize(dir.toString()));
    }
}
