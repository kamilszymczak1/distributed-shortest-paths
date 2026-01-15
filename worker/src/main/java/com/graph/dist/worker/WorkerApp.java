package com.graph.dist.worker;

import java.util.HashMap;

import com.graph.dist.proto.*;
import com.graph.dist.utils.Point;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class WorkerApp {
    public static void main(String[] args) throws Exception {
        // Start the gRPC server on port 9090
        Server server = ServerBuilder.forPort(9090)
                .addService(new GraphServiceImpl())
                .maxInboundMessageSize(50 * 1024 * 1024) // 50 MB
                .build();

        System.out.println("Worker started on port 9090...");
        server.start();
        server.awaitTermination();
    }

    static class GraphServiceImpl extends GraphServiceGrpc.GraphServiceImplBase {
        @Override
        public void loadShard(ShardData request, StreamObserver<ShardResponse> responseObserver) {
            HashMap<Integer, Point> localGraph = new HashMap<>();

            int min_x = Integer.MAX_VALUE;
            int max_x = Integer.MIN_VALUE;

            for (com.graph.dist.proto.Node n : request.getNodesList()) {
                localGraph.put(n.getId(), new Point(n.getX(), n.getY()));
                if (n.getX() < min_x)
                    min_x = n.getX();
                if (n.getX() > max_x)
                    max_x = n.getX();
            }

            System.out
                    .println("Worker " + request.getShardId() + " loaded " + localGraph.size() + " nodes.");
            System.out.println("X coordinate range: [" + min_x + ", " + max_x + "]");

            responseObserver.onNext(ShardResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }
    }
}