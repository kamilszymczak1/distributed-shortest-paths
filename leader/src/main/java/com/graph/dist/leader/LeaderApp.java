package com.graph.dist.leader;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.graph.dist.proto.LeaderServiceGrpc;
import com.graph.dist.proto.ShardRequest;
import com.graph.dist.proto.ShardData;

public class LeaderApp {

    private static ShardManager shardManager;

    public static int getNumWorkers() {
        String numWorkers = System.getenv().getOrDefault("NUM_WORKERS", "1");
        return Integer.parseInt(numWorkers);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Leader starting...");

        DimacsParser.GraphData graphData = null;

        try {
            graphData = DimacsParser.parse("/data/graph.co.gz", "/data/graph.gr.gz");
            System.out.println("Graph parsed with " + graphData.coords.size() + " coordinates.");
        } catch (Exception e) {
            System.err.println("Failed to parse graph: " + e.getMessage());
            return;
        }

        int numWorkers = getNumWorkers();

        // Create shards
        shardManager = new ShardManager(graphData, numWorkers);
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
}