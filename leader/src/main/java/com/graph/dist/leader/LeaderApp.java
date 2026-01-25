package com.graph.dist.leader;

import com.graph.dist.proto.LeaderServiceGrpc;
import com.graph.dist.proto.ShardData;
import com.graph.dist.proto.ShardRequest;
import com.graph.dist.utils.WorkerClient;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.jgrapht.alg.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import io.javalin.Javalin;

public class LeaderApp {

    private static ShardManager shardManager;
    private static ApiHandler apiHandler;

    private static ArrayList<WorkerClient> workerClients = new ArrayList<>();

    private static ArrayList<ArrayList<Integer>> pathSegments;

    public static int getNumWorkers() {
        String numWorkers = System.getenv().getOrDefault("NUM_WORKERS", "1");
        return Integer.parseInt(numWorkers);
    }

    private static String getWorkerHost(int shardId) {
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

    public static void main(String[] args) throws Exception {
        System.out.println("Leader starting...");

        String coordinateFileName = "/data/graph.co.gz";
        String edgesFileName = "/data/graph.gr.gz";


        int numWorkers = getNumWorkers();

        for (int i = 0; i < numWorkers; i++) {
            String host = getWorkerHost(i);
            int workerPort = Integer.parseInt(System.getenv().getOrDefault("WORKER_PORT", "9090"));
            WorkerClient client = new WorkerClient(host, workerPort);
            workerClients.add(client);
        }

        // Create shards
        shardManager = new ShardManager(coordinateFileName, edgesFileName, numWorkers);
        shardManager.createShards();

        apiHandler = new ApiHandler();
        Javalin app = Javalin.create().start(8080);
        app.get("/shortest-path", apiHandler::handleShortestPath);

        System.out.println("Leader initialization complete. Starting gRPC server on port 9090...");

        // Start gRPC server to serve shards
        Server server = ServerBuilder.forPort(9090)
                .addService(new LeaderServiceImpl())
                .maxInboundMessageSize(50 * 1024 * 1024)
                .build();

        server.start();
        System.out.println("Leader server started. Waiting for workers to connect...");
        server.awaitTermination();
    }

    static class LeaderServiceImpl extends LeaderServiceGrpc.LeaderServiceImplBase {
        @Override
        public void getShard(ShardRequest request, StreamObserver<ShardData> responseObserver) {
            int workerId = request.getWorkerId();
            System.out.println("Received shard request from worker-" + workerId);

            ShardData data = shardManager.getShardData(workerId);

            if (data != null) {
                System.out.println("Serving shard " + workerId + " to worker-" + workerId + ". It has "
                        + data.getNodesCount() + " nodes and "
                        + data.getEdgesCount() + " edges.");
                responseObserver.onNext(data);
                responseObserver.onCompleted();
            } else {
                System.err.println("Shard " + workerId + " not found!");
                responseObserver.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("Shard " + workerId + " not found")
                        .asRuntimeException());
            }
        }

        @Override
        public void reportPathSegment(com.graph.dist.proto.ReportPathSegmentRequest request,
                StreamObserver<com.graph.dist.proto.ReportPathSegmentResponse> responseObserver) {
            System.out.println("Received path segment report with " + request.getPathSegmentCount() + " nodes.");
            System.out.println("Path segment: " + request.getPathSegmentList().toString());

            pathSegments.add(new ArrayList<>(request.getPathSegmentList()));

            com.graph.dist.proto.ReportPathSegmentResponse response = com.graph.dist.proto.ReportPathSegmentResponse
                    .newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private static Integer getShardIdForNode(int nodeId) {
        for (int shardId = 0; shardId < getNumWorkers(); shardId++) {
            if (workerClients.get(shardId).hasNode(nodeId)) {
                return shardId;
            }
        }
        return null;
    }

    public static ShortestPathResult findShortestPath(int fromId, int toId) {
        int numWorkers = LeaderApp.getNumWorkers();

        Integer fromShard = getShardIdForNode(fromId);
        Integer toShard = getShardIdForNode(toId);


        if (fromShard == null) {
            throw new IllegalArgumentException("Start node " + fromId + " not found in any shard.");
        }
        if (toShard == null) {
            throw new IllegalArgumentException("End node " + toId + " not found in any shard.");
        }

        for (int i = 0; i < numWorkers; i++) {
            WorkerClient client = workerClients.get(i);
            boolean prepared = client.prepareForNewQuery();
            if (prepared) {
                System.out.println("Prepared worker-" + i + " for new query.");
            } else {
                throw new RuntimeException("Failed to prepare worker-" + i + " for new query.");
            }
        }

        if (!workerClients.get(fromShard).startShortestPathComputation(fromId)) {
            throw new RuntimeException(
                    "Failed to start shortest path computation on worker for shard " + fromShard);
        }

        System.out
                .println("Shortest path computation started from node " + fromId + " (shard " + fromShard + ") to node "
                        + toId + " (shard " + toShard + ").");

        while (true) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for computation to complete", e);
            }

            System.out.println("Checking if all workers are done...");

            boolean allDone = true;

            for (int i = 0; i < numWorkers; i++) {
                WorkerClient client = workerClients.get(i);
                if (!client.isWorkerDone()) {
                    System.out.println("Worker-" + i + " is not done yet.");
                    allDone = false;
                } else {
                    System.out.println("Worker-" + i + " is done.");
                }
            }

            if (allDone) {
                for (int i = 0; i < numWorkers; i++) {
                    WorkerClient client = workerClients.get(i);
                    if (!client.isWorkerDone()) {
                        allDone = false;
                    }
                }
                if (allDone) {
                    break;
                }
            }
        }

        pathSegments = new ArrayList<>();

        int interShardDistance = workerClients.get(toShard).retrievePath(toId);

        int distance = interShardDistance;
        ArrayList<Integer> path = combinePaths(pathSegments);

        System.out.println(
                "Distance from node " + fromId + " to node " + toId + " via inter-shard computation is "
                        + interShardDistance);

        if (fromShard == toShard) {

            Pair<Integer, ArrayList<Integer>> intraShardResult = workerClients.get(fromShard).getIntraShardPath(fromId,
                    toId);
            int intraShardDistance = intraShardResult.getFirst();
            ArrayList<Integer> intraShardPath = intraShardResult.getSecond();

            if (intraShardDistance < distance) {
                distance = intraShardDistance;
                path = intraShardPath;
            }

            System.out.println("Both nodes are in the same shard " + fromShard + ".");
            System.out.println("Intra-shard distance from node " + fromId + " to node " + toId + " is "
                    + intraShardDistance);
        }

        System.out.println("Distance from node " + fromId + " to node " + toId + " is " + distance);

        return new ShortestPathResult(distance, path);
    }

    public static ArrayList<Integer> combinePaths(ArrayList<ArrayList<Integer>> segments) {
        // The segments are collected in reverse order, so we need to reverse them first
        Collections.reverse(segments);

        System.out.println("Combining " + segments.size() + " path segments.");

        ArrayList<Integer> fullPath = new ArrayList<>();
        for (ArrayList<Integer> segment : segments) {
            System.out.println("Segment: " + segment.toString());
            if (fullPath.isEmpty() || fullPath.get(fullPath.size() - 1) != segment.get(0)) {
                fullPath.addAll(segment);
            } else {
                // Avoid duplicating the connecting node
                fullPath.addAll(segment.subList(1, segment.size()));
            }
        }
        return fullPath;
    }

    // A simple class to hold the result
    public static class ShortestPathResult {
        public int distance;
        public List<Integer> path;

        public ShortestPathResult(int distance, List<Integer> path) {
            this.distance = distance;
            this.path = path;
        }
    }
}