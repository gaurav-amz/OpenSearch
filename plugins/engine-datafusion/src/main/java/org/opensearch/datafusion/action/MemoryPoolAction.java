/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * POC REST handler for dynamic memory pool management.
 *
 * GET  /_plugins/datafusion/memory  — read current pool stats
 * PUT  /_plugins/datafusion/memory?limit_bytes=...  — change pool limit
 */
public class MemoryPoolAction extends BaseRestHandler {

private final long runtimePtr;

    /**
     * Constructor.
     * @param runtimePtr pointer to the native DataFusion runtime
     */
    public MemoryPoolAction(long runtimePtr) {
        this.runtimePtr = runtimePtr;
    }

    @Override
    public String getName() {
        return "datafusion_memory_pool_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_plugins/datafusion/memory"),
            new Route(PUT, "/_plugins/datafusion/memory")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.method() == PUT) {
            return handleSetLimit(request);
        }
        return handleGetStats(request);
    }

    private RestChannelConsumer handleGetStats(RestRequest request) {
        return channel -> {
            long currentUsage = NativeBridge.getMemoryPoolCurrentUsage(runtimePtr);
            long peakUsage = NativeBridge.getMemoryPoolPeakUsage(runtimePtr);
            long limit = NativeBridge.getMemoryPoolLimit(runtimePtr);

            String json = String.format(
                "{\"memory_pool\":{\"limit_bytes\":%d,\"limit_mb\":%d,"
                + "\"current_usage_bytes\":%d,\"current_usage_mb\":%d,"
                + "\"peak_usage_bytes\":%d,\"peak_usage_mb\":%d,"
                + "\"utilization_percent\":%.1f}}",
                limit, limit / (1024 * 1024),
                currentUsage, currentUsage / (1024 * 1024),
                peakUsage, peakUsage / (1024 * 1024),
                limit > 0 ? (currentUsage * 100.0 / limit) : 0
            );
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", json));
        };
    }

    private RestChannelConsumer handleSetLimit(RestRequest request) {
        long newLimitBytes = request.paramAsLong("limit_bytes", -1);
        if (newLimitBytes <= 0) {
            return channel -> channel.sendResponse(new BytesRestResponse(
                RestStatus.BAD_REQUEST, "application/json",
                "{\"error\":\"Missing or invalid 'limit_bytes' parameter\"}"
            ));
        }

        return channel -> {
            long oldLimit = NativeBridge.getMemoryPoolLimit(runtimePtr);
            long currentUsage = NativeBridge.getMemoryPoolCurrentUsage(runtimePtr);

            NativeBridge.setMemoryPoolLimit(runtimePtr, newLimitBytes);

            long newLimit = NativeBridge.getMemoryPoolLimit(runtimePtr);

            String json = String.format(
                "{\"memory_pool\":{\"old_limit_bytes\":%d,\"old_limit_mb\":%d,"
                + "\"new_limit_bytes\":%d,\"new_limit_mb\":%d,"
                + "\"current_usage_bytes\":%d,\"current_usage_mb\":%d,"
                + "\"status\":\"%s\"}}",
                oldLimit, oldLimit / (1024 * 1024),
                newLimit, newLimit / (1024 * 1024),
                currentUsage, currentUsage / (1024 * 1024),
                currentUsage > newLimit ? "WARNING: current usage exceeds new limit" : "OK"
            );
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json", json));
        };
    }
}
