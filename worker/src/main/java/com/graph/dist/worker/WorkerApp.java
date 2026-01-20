package com.graph.dist.worker;

import java.util.ArrayList;
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

    public static class InternalEdge {
        public int toLocalId;
        public int weight;

        public InternalEdge(int toLocalId, int weight) {
            this.toLocalId = toLocalId;
            this.weight = weight;
        }
    }

    public static class OutOfShardEdge {
        public int toGlobalId;
        public int toShard;
        public int weight;

        public OutOfShardEdge(int toGlobalId, int toShard, int weight) {
            this.toGlobalId = toGlobalId;
            this.toShard = toShard;
            this.weight = weight;
        }
    }

    static int my_shard_id;
    static HashMap<Integer, Integer> globalIdToLocalId = new HashMap<>();
    static ArrayList<ArrayList<InternalEdge>> localOutBoundEdges;
    static int[] localIdToGlobalId, localIdtoBoundaryId, boundaryIdtoLocalId;
    static int num_local_nodes;

    static HashMap<Integer, ArrayList<OutOfShardEdge>> outerEdges = new HashMap<>(); // node_id, list(out_of_shard_edge)
    static HashMap<Integer, HashMap<Integer, Integer>> queryDist = new HashMap<>(); // query_id, (node_id, distance)
    static HashMap<Integer, HashMap<Integer, Pair<Integer, Integer>>> queryFrom = new HashMap<>(); // query_id,
                                                                                                   // (node_id,
                                                                                                   // (from_node,
                                                                                                   // from_shard))
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
        String leaderServiceName = System.getenv().getOrDefault("LEADER_SERVICE_NAME", "leader");
        String namespace = System.getenv().getOrDefault("NAMESPACE", "");
        int leaderPort = Integer.parseInt(System.getenv().getOrDefault("LEADER_PORT", "9090"));

        String leaderHost;
        if (namespace.isEmpty()) {
            leaderHost = leaderServiceName; // local / docker-compose
        } else {
            leaderHost = leaderServiceName + "." + namespace + ".svc.cluster.local"; // k8s FQDN
        }

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
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ignored) {
                    }
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
        num_local_nodes = request.getNodesCount();

        int min_x = Integer.MAX_VALUE;
        int max_x = Integer.MIN_VALUE;
        int min_y = Integer.MAX_VALUE;
        int max_y = Integer.MIN_VALUE;

        localOutBoundEdges = new ArrayList<>(num_local_nodes);
        localIdToGlobalId = new int[num_local_nodes];
        for (int i = 0; i < num_local_nodes; i++) {
            localOutBoundEdges.add(new ArrayList<>());
        }

        int lastId = 0;
        for (com.graph.dist.proto.Node n : request.getNodesList()) {
            globalIdToLocalId.put(n.getId(), lastId);
            localIdToGlobalId[lastId] = n.getId();
            lastId++;
        }

        for (com.graph.dist.proto.Node n : request.getNodesList()) {
            min_x = Math.min(min_x, n.getX());
            max_x = Math.max(max_x, n.getX());
            min_y = Math.min(min_y, n.getY());
            max_y = Math.max(max_y, n.getY());
        }

        boolean[] isBoundaryNode = new boolean[num_local_nodes];
        for (int i = 0; i < num_local_nodes; i++) {
            isBoundaryNode[i] = false;
        }

        int num_boundary_nodes = 0;
        int num_boundary_edges = 0;

        boundaryIdtoLocalId = new int[num_local_nodes];
        localIdtoBoundaryId = new int[num_local_nodes];
        for (int i = 0; i < num_local_nodes; i++) {
            localIdtoBoundaryId[i] = -1;
            boundaryIdtoLocalId[i] = -1;
        }

        for (com.graph.dist.proto.Edge e : request.getEdgesList()) {
            // Note: In distributed graph, some edges might come from nodes not in our
            // nodeCoords list?
            // But the proto definition says "edges that originate from nodes in this
            // shard".
            // So assert should pass.
            int localIdFrom = globalIdToLocalId.get(e.getFrom());
            if (globalIdToLocalId.get(e.getTo()) == null) {
                // This is an out-of-shard edge
                num_boundary_edges++;
                if (isBoundaryNode[localIdFrom] == false) {
                    boundaryIdtoLocalId[num_boundary_nodes] = localIdFrom;
                    localIdtoBoundaryId[localIdFrom] = num_boundary_nodes;
                    num_boundary_nodes++;
                    isBoundaryNode[localIdFrom] = true;
                }
                // TODO: Keep the edge info for later processing
            } else {
                // This is an internal edge
                int localIdTo = globalIdToLocalId.get(e.getTo());
                localOutBoundEdges.get(localIdFrom).add(new InternalEdge(localIdTo, e.getWeight()));
            }
        }

        System.out
                .println("Worker " + request.getShardId() + " loaded " + num_local_nodes + " nodes and "
                        + request.getEdgesCount() + " edges.");
        System.out.println("Boundary nodes: " + num_boundary_nodes);
        System.out.println("Boundary edges: " + num_boundary_edges);
        System.out.println("X coordinate range: [" + min_x + ", " + max_x + "]");
        System.out.println("Y coordinate range: [" + min_y + ", " + max_y + "]");

        distBoundary = new int[num_boundary_nodes][num_boundary_nodes];
        precomputeBoundaryDistances();
    }

    static class GraphServiceImpl extends GraphServiceGrpc.GraphServiceImplBase {

        @Override
        public void solveShortestPath(ShortestPathRequest request,
                StreamObserver<ShortestPathResponse> responseObserver) {
            int startGlobalId = request.getStartNode();
            int endGlobalId = request.getEndNode();
            boolean startShard = globalIdToLocalId.containsKey(startGlobalId);
            boolean endShard = globalIdToLocalId.containsKey(endGlobalId);

            System.out.println("Solving shortest path from " + startGlobalId + " to " + endGlobalId);

            if (!startShard && !endShard) {
                responseObserver.onError(
                        new io.grpc.StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT
                                .withDescription("Neither start nor end node is in this shard")));
                return;
            }

            if (startShard && endShard) {
                int startLocalId = globalIdToLocalId.get(startGlobalId);
                int endLocalId = globalIdToLocalId.get(endGlobalId);
                int[] dist = dijkstra(
                        startLocalId);

                int distance = dist[endLocalId];
                responseObserver.onNext(ShortestPathResponse.newBuilder().setDistance(distance).build());
                responseObserver.onCompleted();
                return;
            }

            else if (startShard) {
                int startLocalId = globalIdToLocalId.get(startGlobalId);
                int[] dist = dijkstra(startLocalId);

                // queryDist.put(request.getQueryId(), dist);

                // for (int i = 0; i < distBoundary.length; i++) {
                // int nodeId = fromBoundaryId.get(i);
                // relaxOuterEdges(nodeId, dist[nodeId], request.getQueryId());
                // }
            }

            else { // endShard
                int endLocalId = globalIdToLocalId.get(endGlobalId);
                int[] dist = dijkstra(endLocalId);

                // try {
                // // W
                // Thread.sleep(10 * 1000);
                // } catch (InterruptedException e) {
                // e.printStackTrace();
                // }

                // int minDist = Integer.MAX_VALUE;
                // for (int i = 0; i < distBoundary.length; i++) {
                // int nodeId = fromBoundaryId.get(i);
                // int distFromStart = queryDist.get(request.getQueryId()).get(nodeId);
                // int distToEnd = distFromStart + dist.get(nodeId);
                // minDist = Integer.min(minDist, distToEnd);
                // }

                responseObserver.onNext(ShortestPathResponse.newBuilder().setDistance(0).build());
                responseObserver.onCompleted();
                return;
            }
        }

        @Override
        public void updateShortestPath(UpdateShortestPathRequest request,
                StreamObserver<UpdateShortestPathResponse> responseObserver) {
            System.out.println("Received UpdateShortestPath request: from " + request.getFromNode() +
                    " to " + request.getToNode() + " with new distance " + request.getDistance());

            // handleUpdate(request);
            // responseObserver.onNext(UpdateShortestPathResponse.newBuilder().setSuccess(true).build());
            // responseObserver.onCompleted();
        }
    }

    static void handleUpdate(UpdateShortestPathRequest request) {
        int queryId = request.getQueryId();
        int fromGlobalId = request.getFromNode();
        int fromShard = request.getFromShard();
        int toGlobalId = request.getToNode();
        int dist = request.getDistance();

        HashMap<Integer, Integer> qDist = queryDist.computeIfAbsent(queryId, k -> new HashMap<>());
        HashMap<Integer, Pair<Integer, Integer>> qFrom = queryFrom.computeIfAbsent(queryId, k -> new HashMap<>());

        // if (qDist.getOrDefault(to, Integer.MAX_VALUE) > dist) {
        // qDist.put(to, dist);
        // qFrom.put(to, new Pair<>(from, fromShard));

        // int startBoundaryId = toBoundaryId.get(to);
        // relaxOuterEdges(to, dist, queryId);
        // for (int i = 0; i < distBoundary.length; i++) {
        // int nodeId = toBoundaryId.get(i);

        // if (qDist.getOrDefault(nodeId, Integer.MAX_VALUE) > dist +
        // distBoundary[startBoundaryId][i]) {
        // qDist.put(nodeId, dist + distBoundary[startBoundaryId][i]);
        // qFrom.put(nodeId, new Pair<>(startBoundaryId, my_shard_id));
        // relaxOuterEdges(nodeId, qDist.get(nodeId), queryId);
        // }
        // }
        // }
    }

    static void relaxOuterEdges(int nodeId, int myDist, int queryId) {
        // for (OutBoundEdge obe : outerEdges.get(nodeId)) {
        // GraphServiceGrpc.GraphServiceBlockingStub stub =
        // workerStubs.get(obe.toShard);
        // System.out.println("Query " + queryId + ": Propagating update from worker " +
        // my_shard_id + ":" + nodeId
        // + " to " + obe.toShard + ":" + obe.to);
        // try {
        // UpdateShortestPathRequest newRequest = UpdateShortestPathRequest.newBuilder()
        // .setQueryId(queryId)
        // .setFromNode(nodeId)
        // .setFromShard(my_shard_id)
        // .setToNode(obe.to)
        // .setDistance(myDist + obe.weight)
        // .build();

        // // This is the call that sends the message to the other worker
        // stub.updateShortestPath(newRequest);

        // } catch (Exception e) {
        // System.err.println("Failed to send update to worker " + obe.toShard + ": " +
        // e.getMessage());
        // }
        // }
    }

    static void precomputeBoundaryDistances() {
        for (int i = 0; i < distBoundary.length; i++) {
            System.out.println("Precomputing distances from boundary node " + boundaryIdtoLocalId[i] + " (" + (i + 1)
                    + "/" + distBoundary.length + ")");

            int[] dist = dijkstra(
                    boundaryIdtoLocalId[i]);

            for (int j = 0; j < distBoundary.length; j++) {
                distBoundary[i][j] = dist[boundaryIdtoLocalId[j]];
            }
        }
    }

    static int[] dijkstra(int sourceLocalId) {
        int[] dist = new int[num_local_nodes];
        boolean[] visited = new boolean[num_local_nodes];
        for (int i = 0; i < num_local_nodes; i++) {
            dist[i] = Integer.MAX_VALUE;
            visited[i] = false;
        }
        dist[sourceLocalId] = 0;

        IndexedPriorityQueue priorityQueue = new IndexedPriorityQueue(num_local_nodes);

        priorityQueue.addOrUpdate(sourceLocalId, 0);

        while (!priorityQueue.isEmpty()) {
            int u = priorityQueue.poll();
            int currentDist = dist[u];
            visited[u] = true;
            for (InternalEdge v : localOutBoundEdges.get(u)) {
                if (visited[v.toLocalId])
                    continue;
                int newDist = currentDist + v.weight;
                if (newDist < dist[v.toLocalId]) {
                    dist[v.toLocalId] = newDist;
                    priorityQueue.addOrUpdate(v.toLocalId, newDist);
                }
            }
        }
        return dist;
    }
}