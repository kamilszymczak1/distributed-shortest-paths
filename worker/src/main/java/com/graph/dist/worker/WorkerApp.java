package com.graph.dist.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.jgrapht.alg.util.Pair;

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
    static HashMap<Integer, Integer> toBoundaryId = new HashMap<>();
    static HashMap<Integer, Integer> fromBoundaryId = new HashMap<>();
    static HashMap<Integer, ArrayList<OutBoundEdge>> outerEdges = new HashMap<>();  // node_id, list(out_of_shard_edge)
    static HashMap<Integer, HashMap<Integer, Integer>> queryDist = new HashMap<>(); // query_id, (node_id, distance)
    static HashMap<Integer, HashMap<Integer, Pair<Integer, Integer>>> queryFrom = new HashMap<>(); // query_id, (node_id, (from_node, from_shard))
    static int distBoundary[][];
    static boolean has_graph = false;
    static final HashMap<Integer, GraphServiceGrpc.GraphServiceBlockingStub> workerStubs = new HashMap<>();

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

        // After fetching our shard, we can connect to our peer workers.
        // We'll use an environment variable to determine the total number of workers.
        int numWorkers = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS", "1"));
        for (int i = 0; i < numWorkers; i++) {
            if (i == my_shard_id) {
                continue;
            }
            String workerHost = "worker-" + i;
            System.out.println("Connecting to peer worker " + i + " at " + workerHost + ":9090");
            ManagedChannel channel = ManagedChannelBuilder.forAddress(workerHost, 9090)
                    .usePlaintext()
                    .build();
            workerStubs.put(i, GraphServiceGrpc.newBlockingStub(channel));
        }

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
                        outerEdges.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(obe);
                    }
                }
                if (is_boundary) {
                    toBoundaryId.put(nodeId, num_boundary_nodes);
                    fromBoundaryId.put(num_boundary_nodes, nodeId);
                    num_boundary_nodes++;
                }
            }
            distBoundary = new int[num_boundary_nodes][num_boundary_nodes];
            precomputeBoundaryDistances();

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

        @Override
        public void solveShortestPath(ShortestPathRequest request, StreamObserver<ShortestPathResponse> responseObserver) {
            int startNode = request.getStartNode();
            int endNode = request.getEndNode();
            boolean startShard = nodeCoords.containsKey(startNode);
            boolean endShard = nodeCoords.containsKey(endNode);

            System.out.println("Solving shortest path from " + startNode + " to " + endNode);

            if (!startShard && !endShard) {
                responseObserver.onError(
                    new io.grpc.StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT.withDescription("Neither start nor end node is in this shard"))
                );
                return;
            }

            if (startShard && endShard) {                
                Pair<HashMap<Integer, Integer>, HashMap<Integer, Pair<Integer, Integer>>> dist_from = dijkstra(startNode);
                HashMap<Integer, Integer> dist = dist_from.getFirst();
                HashMap<Integer, Pair<Integer, Integer>> from = dist_from.getSecond();
                
                int distance = dist.get(endNode);
                responseObserver.onNext(ShortestPathResponse.newBuilder().setDistance(distance).build());
                responseObserver.onCompleted();
                return;
            }

            else if (startShard) {                
                Pair<HashMap<Integer, Integer>, HashMap<Integer, Pair<Integer, Integer>>> dist_from = dijkstra(startNode);
                HashMap<Integer, Integer> dist = dist_from.getFirst();
                HashMap<Integer, Pair<Integer, Integer>> from = dist_from.getSecond();

                queryDist.put(request.getQueryId(), dist);
                queryFrom.put(request.getQueryId(), from);

                for (int i = 0; i < distBoundary.length; i++) {
                    int nodeId = fromBoundaryId.get(i);
                    relaxOuterEdges(nodeId, dist.get(nodeId), request.getQueryId());
                }
            }

            else { // endShard
                Pair<HashMap<Integer, Integer>, HashMap<Integer, Pair<Integer, Integer>>> dist_from = dijkstra(endNode);
                HashMap<Integer, Integer> dist = dist_from.getFirst();
                HashMap<Integer, Pair<Integer, Integer>> from = dist_from.getSecond();
                
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                
                int minDist = Integer.MAX_VALUE;
                for (int i = 0; i < distBoundary.length; i++) {
                    int nodeId = fromBoundaryId.get(i);
                    int distFromStart = queryDist.get(request.getQueryId()).get(nodeId);
                    int distToEnd = distFromStart + dist.get(nodeId);
                    minDist = Integer.min(minDist, distToEnd);
                }

                responseObserver.onNext(ShortestPathResponse.newBuilder().setDistance(minDist).build());
                responseObserver.onCompleted();
                return;
            }
        }

        @Override
        public void updateShortestPath(UpdateShortestPathRequest request, StreamObserver<UpdateShortestPathResponse> responseObserver) {
            System.out.println("Received UpdateShortestPath request: from " + request.getFromNode() +
                    " to " + request.getToNode() + " with new distance " + request.getDistance());

            handleUpdate(request);
            // responseObserver.onNext(UpdateShortestPathResponse.newBuilder().setSuccess(true).build());
            // responseObserver.onCompleted();
        }
    }

    static void handleUpdate(UpdateShortestPathRequest request) {
        int queryId = request.getQueryId();
        int from = request.getFromNode();
        int fromShard = request.getFromShard();
        int to = request.getToNode();
        int dist = request.getDistance();

        HashMap<Integer, Integer> qDist = queryDist.computeIfAbsent(queryId, k -> new HashMap<>());
        HashMap<Integer, Pair<Integer, Integer>> qFrom  = queryFrom.computeIfAbsent(queryId, k -> new HashMap<>());

        if (qDist.getOrDefault(to, Integer.MAX_VALUE) > dist) {
            qDist.put(to, dist);
            qFrom.put(to, new Pair<>(from, fromShard));
            
            int startBoundaryId = toBoundaryId.get(to);
            relaxOuterEdges(to, dist, queryId);
            for (int i = 0; i < distBoundary.length; i++) {
                int nodeId = toBoundaryId.get(i);

                if (qDist.getOrDefault(nodeId, Integer.MAX_VALUE) > dist + distBoundary[startBoundaryId][i]) {
                    qDist.put(nodeId, dist + distBoundary[startBoundaryId][i]);
                    qFrom.put(nodeId, new Pair<>(startBoundaryId, my_shard_id));
                    relaxOuterEdges(nodeId, qDist.get(nodeId), queryId);
                }
            }
        }
    }

    static void relaxOuterEdges(int nodeId, int myDist, int queryId) {
        for (OutBoundEdge obe : outerEdges.get(nodeId)) {
            GraphServiceGrpc.GraphServiceBlockingStub stub = workerStubs.get(obe.toShard);
            System.out.println("Query " + queryId + ": Propagating update from worker " + my_shard_id + ":" + nodeId + " to " + obe.toShard + ":" + obe.to);
            try {
                UpdateShortestPathRequest newRequest = UpdateShortestPathRequest.newBuilder()
                    .setQueryId(queryId)
                    .setFromNode(nodeId)
                    .setFromShard(my_shard_id)
                    .setToNode(obe.to)
                    .setDistance(myDist + obe.weight)
                    .build();

                // This is the call that sends the message to the other worker
                stub.updateShortestPath(newRequest);

            } catch (Exception e) {
                System.err.println("Failed to send update to worker " + obe.toShard + ": " + e.getMessage());
            }
        }
    }
    
    static void precomputeBoundaryDistances() {
        for (int i = 0; i < distBoundary.length; i++) {
            Pair<HashMap<Integer, Integer>, HashMap<Integer, Pair<Integer, Integer>>> dist_from = dijkstra(fromBoundaryId.get(i));
            HashMap<Integer, Integer> dist = dist_from.getFirst();

            for (int j = 0; j < distBoundary.length; j++) {
                distBoundary[i][j] = dist.get(fromBoundaryId.get(j));
            }
        }
    }

    static Pair<HashMap<Integer, Integer>, HashMap<Integer, Pair<Integer, Integer>>> dijkstra(int src) {
        HashMap<Integer, Integer> dist = new HashMap<>(nodeCoords.size());
        HashMap<Integer, Pair<Integer, Integer>> from = new HashMap<>(nodeCoords.size());
        HashMap<Integer, Boolean> visited = new HashMap<>(nodeCoords.size());
        for (Integer id : nodeCoords.keySet()) {
            dist.put(id, Integer.MAX_VALUE);
            from.put(id, new Pair<>(-1, -1));
            visited.put(id, false);
        }
        dist.put(src, 0);

        PriorityQueue<Pair<Integer,Integer>> pq =
                new PriorityQueue<>(distBoundary.length, Comparator.comparing(Pair::getFirst));

        pq.add(new Pair<>(0, src));
        while (!pq.isEmpty()) {
            Pair<Integer,Integer> p = pq.poll();
            int curDist = p.getFirst();
            int u = p.getSecond();
            if (visited.get(u)) continue;
            visited.put(u, true);
            for (OutBoundEdge v : outBoundEdges.get(u)) {
                if (visited.getOrDefault(v.to, true)) continue;
                int newDist = curDist + v.weight;
                if (newDist < dist.get(v.to)) {
                    dist.put(v.to, newDist);
                    from.put(v.to, new Pair<>(u, my_shard_id));
                    pq.add(new Pair<>(newDist, v.to));
                }
            }
        }
        return new Pair<>(dist, from);
    }
}