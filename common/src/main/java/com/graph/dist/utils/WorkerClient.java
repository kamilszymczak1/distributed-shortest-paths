package com.graph.dist.utils;

import com.graph.dist.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import org.jgrapht.alg.util.Pair;

public class WorkerClient {

    private final ManagedChannel channel;
    private final GraphServiceGrpc.GraphServiceBlockingStub blockingStub;
    private final GraphServiceGrpc.GraphServiceStub asyncStub;

    public WorkerClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = GraphServiceGrpc.newBlockingStub(channel);
        this.asyncStub = GraphServiceGrpc.newStub(channel);
    }

    public boolean prepareForNewQuery() {
        PrepareForNewQueryRequest request = PrepareForNewQueryRequest
                .newBuilder()
                .setQueryId(0)
                .build();
        PrepareForNewQueryResponse response = blockingStub.prepareForNewQuery(request);
        return response.getSuccess();
    }

    public boolean startShortestPathComputation(int startNodeId) {
        StartShortestPathComputationRequest request = StartShortestPathComputationRequest
                .newBuilder()
                .setStartNode(startNodeId)
                .build();
        StartShortestPathComputationResponse response = blockingStub
                .startShortestPathComputation(request);
        return response.getSuccess();
    }

    public int retrievePath(int targetNodeId) {
        RetrievePathRequest request = RetrievePathRequest
                .newBuilder()
                .setQueryId(0)
                .setTargetNode(targetNodeId)
                .build();
        RetrievePathResponse response = blockingStub.retrievePath(request);
        return response.getDistance();
    }

    public Pair<Integer, ArrayList<Integer>> getIntraShardPath(int fromNodeId, int toNodeId) {
        GetIntraShardPathRequest request = GetIntraShardPathRequest
                .newBuilder()
                .setFromNode(fromNodeId)
                .setToNode(toNodeId)
                .build();
        GetIntraShardPathResponse response = blockingStub.getIntraShardPath(request);
        return new Pair<>(response.getDistance(), new ArrayList<>(response.getPathList()));
    }

    public boolean tracePath(int sourceNodeId) {
        TracePathRequest request = TracePathRequest
                .newBuilder()
                .setQueryId(0)
                .setSourceNode(sourceNodeId)
                .build();
        TracePathResponse response = blockingStub.tracePath(request);
        return response.getSuccess();
    }

    public boolean isWorkerDone() {
        IsWorkerDoneRequest request = IsWorkerDoneRequest
                .newBuilder()
                .build();
        IsWorkerDoneResponse response = blockingStub.isWorkerDone(request);
        return response.getDone();
    }

    public boolean tracePath(int queryId, int sourceNodeId) {
        TracePathRequest request = TracePathRequest
                .newBuilder()
                .setQueryId(queryId)
                .setSourceNode(sourceNodeId)
                .build();
        TracePathResponse response = blockingStub.tracePath(request);
        return response.getSuccess();
    }

    public void batchedUpdateDistanceToNode(int queryId, int fromShardId,
            ArrayList<UpdateDistanceToNodeData> updates) {
        BatchedUpdateDistanceToNodeRequest batchRequest = BatchedUpdateDistanceToNodeRequest
                .newBuilder()
                .setFromShard(fromShardId)
                .setQueryId(queryId)
                .addAllUpdates(updates)
                .build();

        StreamObserver<BatchedUpdateDistanceToNodeResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(BatchedUpdateDistanceToNodeResponse value) {
                // Server received it successfully.
            }

            @Override
            public void onError(Throwable t) {
                // Handle failures (network down, timeout, etc.)
                System.err.println("Async Batch Update failed: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                // Transaction finished.
            }
        };
        asyncStub.batchedUpdateDistanceToNode(batchRequest, responseObserver);
    }

    public void shutdown() {
        channel.shutdown();
    }
}
