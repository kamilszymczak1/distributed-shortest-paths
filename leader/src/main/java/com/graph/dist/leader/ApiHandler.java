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
        if (!lock.tryLock()) {
            ctx.status(503).json(Collections.singletonMap("error", "Another query is already in progress. Please try again later."));
            return;
        }

        try {
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");

            if (from == null || to == null) {
                ctx.status(400).json(Collections.singletonMap("error", "Missing 'from' or 'to' query parameter."));
                return;
            }

            int fromNode = Integer.parseInt(from);
            int toNode = Integer.parseInt(to);

            ShortestPathResult result = findShortestPath(fromNode, toNode);

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
