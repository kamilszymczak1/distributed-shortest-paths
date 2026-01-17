package com.graph.dist.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.graph.dist.proto.GraphServiceGrpc;
import com.graph.dist.proto.ShortestPathRequest;
import com.graph.dist.proto.ShortestPathResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.javalin.http.Context;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ApiHandler {

    private final ShardManager shardManager;
    private final ReentrantLock lock = new ReentrantLock();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ApiHandler(ShardManager shardManager) {
        this.shardManager = shardManager;
    }

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
        } catch (IllegalArgumentException e) {
            ctx.status(404).json(Collections.singletonMap("error", e.getMessage()));
        } catch (StatusRuntimeException e) {
            ctx.status(500).json(Collections.singletonMap("error", "gRPC error: " + e.getStatus().toString()));
        }
        finally {
            lock.unlock();
        }
    }
    
    private String getWorkerHost(int shardId) {
        String workerServiceName = System.getenv().getOrDefault("WORKER_SERVICE_NAME", "worker");
        String namespace = System.getenv().getOrDefault("NAMESPACE", "");
        String host;
        if (namespace.isEmpty()) {
            host = "worker-" + shardId;
        } else {
            host = "worker-" + shardId + "." + workerServiceName + "." + namespace + ".svc.cluster.local";
        }
        return host;
    }

    private ShortestPathResult findShortestPath(int from, int to) {
        int fromShard = shardManager.getShardIdForNode(from);
        int toShard = shardManager.getShardIdForNode(to);

        if (fromShard == -1) {
            throw new IllegalArgumentException("Start node " + from + " not found in any shard.");
        }
        if (toShard == -1) {
            throw new IllegalArgumentException("End node " + to + " not found in any shard.");
        }

        int workerPort = Integer.parseInt(System.getenv().getOrDefault("WORKER_PORT", "9090"));
        int queryId = (int) (System.currentTimeMillis() / 1000);
        ShortestPathRequest request = ShortestPathRequest.newBuilder()
                .setQueryId(queryId)
                .setStartNode(from)
                .setEndNode(to)
                .build();

        if (fromShard == toShard) {
             String host = getWorkerHost(fromShard);
             System.out.println("Forwarding shortest path request for " + from + " -> " + to + " to worker " + fromShard + " at " + host);
             ManagedChannel channel = ManagedChannelBuilder.forAddress(host, workerPort)
                .usePlaintext()
                .build();
            try {
                GraphServiceGrpc.GraphServiceBlockingStub stub = GraphServiceGrpc.newBlockingStub(channel);
                ShortestPathResponse response = stub.solveShortestPath(request);
                return new ShortestPathResult(response.getDistance(), response.getNodesList());
            } finally {
                try {
                    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            String startHost = getWorkerHost(fromShard);
            String endHost = getWorkerHost(toShard);
            
            System.out.println("Forwarding shortest path request for " + from + " -> " + to + " to worker " + fromShard + " (start) and " + toShard + " (end)");

            ManagedChannel startChannel = ManagedChannelBuilder.forAddress(startHost, workerPort).usePlaintext().build();
            ManagedChannel endChannel = ManagedChannelBuilder.forAddress(endHost, workerPort).usePlaintext().build();

            try {
                // Async call to start node worker
                GraphServiceGrpc.GraphServiceStub startStub = GraphServiceGrpc.newStub(startChannel);
                startStub.solveShortestPath(request, new StreamObserver<ShortestPathResponse>() {
                    @Override
                    public void onNext(ShortestPathResponse value) {
                        // We don't care about the response from the start worker
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error from start worker " + fromShard + ": " + t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        // Done
                    }
                });

                // Blocking call to end node worker
                GraphServiceGrpc.GraphServiceBlockingStub endStub = GraphServiceGrpc.newBlockingStub(endChannel);
                ShortestPathResponse response = endStub.solveShortestPath(request);
                return new ShortestPathResult(response.getDistance(), response.getNodesList());

            } finally {
                try {
                    startChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                    endChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // A simple class to hold the result
    private static class ShortestPathResult {
        public int distance;
        public List<Integer> path;

        public ShortestPathResult(int distance, List<Integer> path) {
            this.distance = distance;
            this.path = path;
        }
    }
}
