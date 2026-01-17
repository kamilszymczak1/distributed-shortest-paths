package com.graph.dist.leader;

import com.graph.dist.proto.GraphServiceGrpc;
import com.graph.dist.proto.ShortestPathResponse;
import com.graph.dist.proto.ShortestPathRequest;
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

    public ShortestPathResponse solveShortestPath(ShortestPathRequest request) {
        return blockingStub.solveShortestPath(request);
    }

    public void shutdown() {
        channel.shutdown();
    }
}
