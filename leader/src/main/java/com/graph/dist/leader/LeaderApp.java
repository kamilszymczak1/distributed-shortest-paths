package com.graph.dist.leader;

import com.graph.dist.proto.Edge;
import com.graph.dist.proto.LeaderServiceGrpc;
import com.graph.dist.proto.Node;
import com.graph.dist.proto.ShardData;
import com.graph.dist.proto.ShardRequest;
import com.graph.dist.utils.Point;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LeaderApp {

    private static ShardManager shardManager;

    public static int getNumWorkers() {
        String numWorkers = System.getenv().getOrDefault("NUM_WORKERS", "1");
        return Integer.parseInt(numWorkers);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Leader starting...");

        Map<Integer, Point> coords = null;
        String coPath = "/data/graph.co.gz";
        String grPath = "/data/graph.gr.gz";

        try {
            coords = DimacsParser.parseCoordinates(coPath);
            System.out.println("Graph coordinates parsed with " + coords.size() + " coordinates.");
        } catch (Exception e) {
            System.err.println("Failed to parse graph: " + e.getMessage());
            return;
        }

        int numWorkers = getNumWorkers();

        // Create shards
        shardManager = new ShardManager(coords, grPath, numWorkers);
        shardManager.createShards();
        
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
                System.out.println("Serving shard " + workerId + " to worker-" + workerId);
                responseObserver.onNext(data);
                responseObserver.onCompleted();
            } else {
                System.err.println("Shard " + workerId + " not found!");
                responseObserver.onError(io.grpc.Status.NOT_FOUND
                    .withDescription("Shard " + workerId + " not found")
                    .asRuntimeException());
            }
        }
    }

    private static Map<Integer, Integer> splitNodesIntoShards(DimacsParser.GraphData data, int numWorkers) {
        Map<Integer, Integer> nodeToShard = new HashMap<>();
        int currentShard = 0;
        for (Integer nodeId : data.coords.keySet()) {
            nodeToShard.put(nodeId, currentShard);
            currentShard = (currentShard + 1) % numWorkers;
        }
        return nodeToShard;
    }

    public static void distributeShards(DimacsParser.GraphData data, int numWorkers) {
        System.out.println("Distributing shards to " + numWorkers + " workers...");

        Map<Integer, Integer> nodeToShard = splitNodesIntoShards(data, numWorkers);

        // Prepare shard data builders
        List<ShardData.Builder> builders = java.util.stream.IntStream.range(0, numWorkers)
                .mapToObj(i -> {
                    ShardData.Builder builder = ShardData.newBuilder();
                    builder.setShardId(i);
                    return builder;
                })
                .collect(Collectors.toList());

        // Add nodes with coordinates to respective shards
        data.coords.forEach((nodeId, pt) -> {
            int shardId = nodeToShard.get(nodeId);
            builders.get(shardId).addNodes(
                    Node.newBuilder()
                            .setId(nodeId)
                            .setX(pt.x)
                            .setY(pt.y)
                            .build());
        });

        // Add edges to respective shards
        data.edges.forEach((edge) -> {
            int fromShard = nodeToShard.get(edge.from);
            int toShard = nodeToShard.get(edge.to);

            builders.get(fromShard).addEdges(
                    Edge.newBuilder()
                            .setFrom(edge.from)
                            .setTo(edge.to)
                            .setToShard(toShard)
                            .setWeight(edge.weight)
                            .build());
        });

        // 4. Send to Workers
        String workerServiceName = System.getenv().getOrDefault("WORKER_SERVICE_NAME", "worker");
        String namespace = System.getenv().getOrDefault("NAMESPACE", "");
        int workerPort = Integer.parseInt(System.getenv().getOrDefault("WORKER_PORT", "9090"));
        
        for (int i = 0; i < numWorkers; i++) {
            // For Kubernetes StatefulSet: worker-0.worker.graph-dist.svc.cluster.local
            // For Docker Compose: worker-0
            String host;
            if (namespace.isEmpty()) {
                // Docker Compose mode
                host = "worker-" + i;
            } else {
                // Kubernetes mode: <pod-name>.<service-name>.<namespace>.svc.cluster.local
                host = "worker-" + i + "." + workerServiceName + "." + namespace + ".svc.cluster.local";
            }
            
            System.out.println("Sending shard " + i + " to " + host + " with "
                    + builders.get(i).getNodesCount() + " nodes.");
            WorkerClient client = new WorkerClient(host, workerPort);
            client.loadShard(builders.get(i).build());
            client.shutdown();
        }
    }
}