package com.graph.dist.utils;

import java.util.ArrayList;

import com.graph.dist.proto.LeaderServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class LeaderClient {

    private final ManagedChannel channel;
    private final LeaderServiceGrpc.LeaderServiceBlockingStub stub;

    public LeaderClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .maxInboundMessageSize(50 * 1024 * 1024)
                .build();

        this.stub = LeaderServiceGrpc.newBlockingStub(channel);
    }

    public boolean reportPathSegment(ArrayList<Integer> pathSegment) {
        com.graph.dist.proto.ReportPathSegmentRequest request = com.graph.dist.proto.ReportPathSegmentRequest
                .newBuilder()
                .addAllPathSegment(pathSegment)
                .build();
        return stub.reportPathSegment(request).getSuccess();
    }

    public void shutdown() {
        channel.shutdown();
    }
}
