package com.graph.dist.worker;

import java.util.ArrayList;
import java.util.HashMap;

import com.graph.dist.proto.*;
import com.graph.dist.utils.Point;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class WorkerApp {

    public static class OutBoundEdge {
        public int to;
        public int toShard;
        public int weight;

        public OutBoundEdge(int to, int toShard, int weight) {
            this.to = to;
            this.toShard = toShard;
            this.weight = weight;
        }
    }

    static int my_shard_id;
    static HashMap<Integer, Point> nodeCoords = new HashMap<>();
    static HashMap<Integer, ArrayList<OutBoundEdge>> outBoundEdges = new HashMap<>();
    static boolean has_graph = false;

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

            if (has_graph) {
                responseObserver.onNext(ShardResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Graph already loaded")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            has_graph = true;
            my_shard_id = request.getShardId();

            int min_x = Integer.MAX_VALUE;
            int max_x = Integer.MIN_VALUE;
            int min_y = Integer.MAX_VALUE;
            int max_y = Integer.MIN_VALUE;

            for (com.graph.dist.proto.Node n : request.getNodesList()) {
                nodeCoords.put(n.getId(), new Point(n.getX(), n.getY()));
                outBoundEdges.put(n.getId(), new ArrayList<>());
                min_x = Math.min(min_x, n.getX());
                max_x = Math.max(max_x, n.getX());
                min_y = Math.min(min_y, n.getY());
                max_y = Math.max(max_y, n.getY());
            }

            for (com.graph.dist.proto.Edge e : request.getEdgesList()) {
                assert (nodeCoords.containsKey(e.getFrom()));
                outBoundEdges.get(e.getFrom()).add(new OutBoundEdge(e.getTo(), e.getToShard(), e.getWeight()));
            }

            int num_boundary_nodes = 0;
            int num_boundary_edges = 0;
            for (int nodeId : nodeCoords.keySet()) {
                boolean is_boundary = false;
                for (OutBoundEdge obe : outBoundEdges.get(nodeId)) {
                    if (obe.toShard != my_shard_id) {
                        num_boundary_edges++;
                        is_boundary = true;
                    }
                }
                if (is_boundary) {
                    num_boundary_nodes++;
                }
            }

            System.out
                    .println("Worker " + request.getShardId() + " loaded " + nodeCoords.size() + " nodes and "
                            + request.getEdgesCount() + " edges.");
            System.out.println("Boundary nodes: " + num_boundary_nodes);
            System.out.println("Boundary edges: " + num_boundary_edges);
            System.out.println("X coordinate range: [" + min_x + ", " + max_x + "]");
            System.out.println("Y coordinate range: [" + min_y + ", " + max_y + "]");

            responseObserver.onNext(ShardResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }
    }
}