package com.graph.dist.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ApiHandler {

    private final ReentrantLock lock = new ReentrantLock();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void handleShortestPath(Context ctx) throws Exception {
        System.out.println("Received shortest path request: " + ctx.queryParamMap());
        if (!lock.tryLock()) {
            System.out.println("Another query is already in progress. Rejecting this request.");
            ctx.status(503).json(
                    Collections.singletonMap("error", "Another query is already in progress. Please try again later."));
            return;
        }

        try {
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");

            if (from == null || to == null) {
                ctx.status(400).json(Collections.singletonMap("error", "Missing 'from' or 'to' query parameter."));
                return;
            }

            System.out.println("Processing shortest path from " + from + " to " + to);

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

            // TODO: Validate that fromNode and toNode exist in the graph

            ShortestPathResult result = findShortestPath(fromNode, toNode);

            System.out
                    .println("Shortest path result: distance=" + result.distance + ", path=" + result.path);

            ctx.json(result);
        } finally {
            lock.unlock();
        }
    }

    private ShortestPathResult findShortestPath(int from, int to) {
        // TODO: Implement the shortest path algorithm here.
        // This is a placeholder implementation.
        System.out.println("Calculating shortest path from " + from + " to " + to);
        return new ShortestPathResult(0, Collections.emptyList());
    }

    // A simple class to hold the result
    private static class ShortestPathResult {
        public double distance;
        public List<Integer> path;

        public ShortestPathResult(double distance, List<Integer> path) {
            this.distance = distance;
            this.path = path;
        }
    }
}
