package com.graph.dist.leader;

import io.grpc.StatusRuntimeException;
import io.javalin.http.Context;

import java.util.Collections;
import java.util.concurrent.Semaphore;

public class ApiHandler {

    private final Semaphore lock = new Semaphore(1);

    public ApiHandler() {
    }

    public void handleShortestPath(Context ctx) throws Exception {
        System.out.println("Received shortest path request: " + ctx.queryParamMap());

        String from = ctx.queryParam("from");
        String to = ctx.queryParam("to");

        if (from == null || to == null) {
            ctx.status(400).json(Collections.singletonMap("error", "Missing 'from' or 'to' query parameter."));
            return;
        }

        int fromNode;
        int toNode;
        try {
            fromNode = Integer.parseInt(from);
            toNode = Integer.parseInt(to);

            if (fromNode < 0 || toNode < 0) {
                throw new NumberFormatException("Node IDs must be non-negative integers.");
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid 'from' or 'to' parameter: " + e.getMessage());
            ctx.status(400).json(
                    Collections.singletonMap("error",
                            "Invalid 'from' or 'to' parameter. Must be valid non-negative integers."));
            return;
        }

        lock.acquire();

        try {

            System.out.println("Processing shortest path from " + from + " to " + to);

            LeaderApp.ShortestPathResult result = LeaderApp.findShortestPath(fromNode, toNode);

            System.out.println("Shortest path result: distance=" + result.distance + ", path=" + result.path);

            ctx.json(result);
        } catch (IllegalArgumentException e) {
            ctx.status(404).json(Collections.singletonMap("error", e.getMessage()));
        } catch (StatusRuntimeException e) {
            ctx.status(500).json(Collections.singletonMap("error", "gRPC error: " + e.getStatus().toString()));
        } finally {
            lock.release();
        }
    }

}
