package com.graph.dist.leader;

import com.graph.dist.proto.GraphServiceGrpc;
import com.graph.dist.proto.ShardData;
import com.graph.dist.proto.ShardResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class WorkerClient {

    private final ManagedChannel channel;
    private final GraphServiceGrpc.GraphServiceBlockingStub blockingStub;

    public WorkerClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = GraphServiceGrpc.newBlockingStub(channel);
    }

    public boolean loadShard(ShardData shardData) {
        try {
            ShardResponse response = blockingStub.loadShard(shardData);
            return response.getSuccess();
        } catch (Exception e) {
            System.err.println("Failed to send shard to worker: " + e.getMessage());
            return false;
        }
    }

    public ShortestPathResponse solveShortestPath(ShortestPathRequest request) {
        return stub.solveShortestPath(request);
    }

    public void shutdown() {
        channel.shutdown();
    }
}
