package com.graph.dist.worker;

import java.util.ArrayList;
import java.util.HashMap;

import com.graph.dist.proto.*;
import com.graph.dist.utils.Point;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
        // Determine ID
        String hostname = java.net.InetAddress.getLocalHost().getHostName();
        int workerId = 0; 
        try {
            // Assumes hostname format like "worker-0", "worker-1"
            String[] parts = hostname.split("-");
            workerId = Integer.parseInt(parts[parts.length - 1]);
        } catch (Exception e) {
             System.err.println("Could not parse worker ID from hostname " + hostname + ", defaulting to 0");
        }
        System.out.println("Worker starting with ID " + workerId);

        // Fetch shard from Leader
        fetchShardFromLeader(workerId);

        // Start the gRPC server on port 9090
        Server server = ServerBuilder.forPort(9090)
                .addService(new GraphServiceImpl())
                .maxInboundMessageSize(50 * 1024 * 1024) // 50 MB
                .build();

        System.out.println("Worker started on port 9090...");
        server.start();
        server.awaitTermination();
    }

    private static void fetchShardFromLeader(int workerId) {
        String leaderHost = System.getenv().getOrDefault("LEADER_HOST", "leader");
        int leaderPort = 9090;
        
        System.out.println("Connecting to leader at " + leaderHost + ":" + leaderPort);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(leaderHost, leaderPort)
                .usePlaintext()
                // Leader server init max msg size is 50MB, so client must allow it
                .maxInboundMessageSize(50 * 1024 * 1024) 
                .build();
        
        try {
             LeaderServiceGrpc.LeaderServiceBlockingStub stub = LeaderServiceGrpc.newBlockingStub(channel);
             
             ShardData data = null;
             while (data == null) {
                 try {
                     System.out.println("Requesting shard " + workerId + "...");
                     data = stub.getShard(ShardRequest.newBuilder().setWorkerId(workerId).build());
                 } catch (Exception e) {
                     System.err.println("Failed to fetch shard from leader: " + e.getMessage() + ". Retrying in 5s...");
                     try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
                 }
             }
             loadShardData(data);
             
        } finally {
            channel.shutdown();
        }
    }

    private static void loadShardData(ShardData request) {
        if (has_graph) {
            System.out.println("Graph already loaded, skipping.");
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
            // Note: In distributed graph, some edges might come from nodes not in our nodeCoords list?
            // But the proto definition says "edges that originate from nodes in this shard".
            // So assert should pass.
            if (nodeCoords.containsKey(e.getFrom())) {
                outBoundEdges.get(e.getFrom()).add(new OutBoundEdge(e.getTo(), e.getToShard(), e.getWeight()));
            }
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
    }

    static class GraphServiceImpl extends GraphServiceGrpc.GraphServiceImplBase {
        @Override
        public void loadShard(ShardData request, StreamObserver<ShardResponse> responseObserver) {
            // Legacy support or push override
            
            if (has_graph) {
                 responseObserver.onNext(ShardResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Graph already loaded")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            loadShardData(request);

            responseObserver.onNext(ShardResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }
    }
}