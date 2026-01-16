package com.graph.dist.leader;

import com.graph.dist.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class WorkerClient {
    private final GraphServiceGrpc.GraphServiceBlockingStub stub;
    private final ManagedChannel channel;

    public WorkerClient(String host, int port) {
        // Create a connection (channel) to the worker
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // No SSL for this toy example
                .maxInboundMessageSize(50 * 1024 * 1024) // 50 MB
                .build();

        // Create the "stub" (the actual remote caller)
        this.stub = GraphServiceGrpc.newBlockingStub(channel);
    }

    public boolean loadShard(ShardData shard) {
        ShardResponse response = stub.loadShard(shard);
        return response.getSuccess();
    }

    public ShortestPathResponse solveShortestPath(ShortestPathRequest request) {
        return stub.solveShortestPath(request);
    }

    public void shutdown() {
        channel.shutdown();
    }
}