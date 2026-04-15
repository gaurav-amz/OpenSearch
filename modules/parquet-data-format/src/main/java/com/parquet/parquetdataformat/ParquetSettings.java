/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;

import static org.opensearch.common.settings.WriteableSetting.SettingType.ByteSizeValue;

/**
 * Settings for Parquet data format.
 */
public final class ParquetSettings {

    private ParquetSettings() {}

    public static final String DEFAULT_MAX_NATIVE_ALLOCATION = "10%";

    public static final Setting<Settings> PARQUET_SETTINGS = Setting.groupSetting(
        "index.parquet.",
        Setting.Property.IndexScope
    );

    public static final Setting<ByteSizeValue> ROW_GROUP_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.row_group_size_bytes",
        new ByteSizeValue(128, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    public static final Setting<ByteSizeValue> PAGE_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.page_size_bytes",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    public static final Setting<Integer> PAGE_ROW_LIMIT = Setting.intSetting(
        "index.parquet.page_row_limit",
        20000,
        1,
        Setting.Property.IndexScope
    );

    public static final Setting<ByteSizeValue> DICT_SIZE_BYTES = Setting.byteSizeSetting(
        "index.parquet.dict_size_bytes",
        new ByteSizeValue(2, ByteSizeUnit.MB),
        Setting.Property.IndexScope
    );

    public static final Setting<String> COMPRESSION_TYPE = Setting.simpleString(
        "index.parquet.compression_type",
        "ZSTD",
        Setting.Property.IndexScope
    );

    public static final Setting<Integer> COMPRESSION_LEVEL = Setting.intSetting(
        "index.parquet.compression_level",
        2,
        1,
        9,
        Setting.Property.IndexScope
    );

    public static final Setting<String> MAX_NATIVE_ALLOCATION = Setting.simpleString(
        "index.parquet.max_native_allocation",
        DEFAULT_MAX_NATIVE_ALLOCATION,
        Setting.Property.NodeScope
    );

    /**
     * Maximum total off-heap memory for the Arrow root allocator (all writers on the node).
     * Default: 10% of available native memory (total physical - JVM heap).
     * Startup-only (Final) because Arrow's RootAllocator limit is immutable after construction.
     * Accepts absolute values ("10gb") or will resolve the computed default at startup.
     */
    public static final Setting<ByteSizeValue> ARROW_POOL_BYTES = Setting.byteSizeSetting(
        "parquet.write.arrow_pool_bytes",
        settings -> {
            long totalPhysical = OsProbe.getInstance().getTotalPhysicalMemorySize();
            long jvmHeap = JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
            long nativeAvailable = Math.max(totalPhysical - jvmHeap, 0);
            return (long) (nativeAvailable * 0.10) + ByteSizeUnit.BYTES.getSuffix();
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Maximum off-heap memory per VSR child allocator.
     * Dynamic: each new createChildAllocator() call reads the current value.
     * Existing child allocators keep their old limit until their VSR rotates.
     */
    public static final Setting<ByteSizeValue> ARROW_CHILD_ALLOCATOR_BYTES = Setting.byteSizeSetting(
        "parquet.write.arrow_child_allocator_bytes",
        new ByteSizeValue(1, ByteSizeUnit.GB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum rows per VSR before rotation from ACTIVE to FROZEN.
     * Dynamic: shouldRotateVSR() reads this on every addDoc call.
     * The child allocator limit provides a hard safety net regardless of this value.
     */
    public static final Setting<Integer> MAX_ROWS_PER_VSR = Setting.intSetting(
        "index.parquet.max_rows_per_vsr",
        50000,
        1000,
        1000000,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
}
