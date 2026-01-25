package com.graph.dist.worker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.Collections;

import org.jgrapht.alg.util.Pair;

import com.graph.dist.proto.*;
import com.graph.dist.utils.LeaderClient;
import com.graph.dist.utils.WorkerClient;

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

    public static class ShortestPathSource {
        public static enum Type {
            FROM_STARTING_NODE,
            FROM_BOUNDARY_NODE,
            FROM_OUT_OF_SHARD_UPDATE,
        }

        private final Type type;
        private final int fromGlobalId;
        private final int fromShard;

        ShortestPathSource(Type type, int fromGlobalId, int fromShard) {
            this.type = type;
            this.fromGlobalId = fromGlobalId;
            this.fromShard = fromShard;
        }

        public Type getType() {
            return type;
        }

        public int getFromGlobalId() {
            return fromGlobalId;
        }

        public int getFromShard() {
            return fromShard;
        }
    }

    private static String getWorkerHost(int shardId) {
        String workerServiceName = System.getenv().getOrDefault("WORKER_SERVICE_NAME", "worker");
        String namespace = System.getenv().getOrDefault("NAMESPACE", "");
        String host;
        if (namespace.isEmpty()) {
            host = "worker-" + shardId;
        } else {
            host = "worker-" + shardId + "." + workerServiceName + "." + namespace + ".svc.cluster.local";
        }
        return host;
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

    public static class OutOfShardUpdate {
        public int toGlobalId;
        public int fromGlobalId;
        public int fromShard;
        public int distance;

        public OutOfShardUpdate(int toGlobalId, int fromGlobalId, int fromShard, int distance) {
            this.toGlobalId = toGlobalId;
            this.fromGlobalId = fromGlobalId;
            this.fromShard = fromShard;
            this.distance = distance;
        }
    }

    static int myShardId;
    static HashMap<Integer, Integer> globalIdToLocalId = new HashMap<>();
    static ArrayList<ArrayList<InternalEdge>> localOutBoundEdges, localOutBoundEdgesRev;
    static ArrayList<ArrayList<OutOfShardEdge>> localOutOfShardEdges;
    static int[] localIdToGlobalId, localIdtoBoundaryId, boundaryIdtoLocalId;
    static int numLocalNodes, numBoundaryNodes;

    static int[] boundaryNodesDistance;
    static int[] lastReportedDistanceToBoundaryNode; // The last distance we used to send updates to other shards
                                                     // This helps us avoid sending duplicate updates unnecessarily
    static ShortestPathSource[] boundaryNodesSource;

    static HashMap<Integer, ArrayList<OutOfShardEdge>> outerEdges = new HashMap<>(); // node_id, list(out_of_shard_edge)

    static int distBoundary[][];
    static final ArrayList<WorkerClient> workerClients = new ArrayList<>();

    static BlockingQueue<OutOfShardUpdate> updatesFromOtherShards = new LinkedBlockingDeque<>();
    static BlockingQueue<Integer> nodesToSendToOtherShards = new LinkedBlockingDeque<>();

    static ExecutorService executor;

    static LeaderClient leaderClient;

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

        String leaderServiceName = System.getenv().getOrDefault("LEADER_SERVICE_NAME", "leader");
        String namespace = System.getenv().getOrDefault("NAMESPACE", "");
        int leaderPort = Integer.parseInt(System.getenv().getOrDefault("LEADER_PORT", "9090"));

        leaderClient = new LeaderClient(leaderServiceName + "." + namespace + ".svc.cluster.local", leaderPort);

        // After fetching our shard, we can connect to our peer workers.
        // We'll use an environment variable to determine the total number of workers.
        int numWorkers = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS", "1"));
        for (int i = 0; i < numWorkers; i++) {
            String workerHost;
            if (i == myShardId) {
                workerHost = "localhost";
            } else {
                workerHost = getWorkerHost(i);
            }
            System.out.println("Connecting to peer worker " + i + " at " + workerHost + ":9090");
            workerClients.add(new WorkerClient(workerHost, 9090));
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
        myShardId = request.getShardId();
        numLocalNodes = request.getNodesCount();

        int min_x = Integer.MAX_VALUE;
        int max_x = Integer.MIN_VALUE;
        int min_y = Integer.MAX_VALUE;
        int max_y = Integer.MIN_VALUE;

        localOutBoundEdges = new ArrayList<>(numLocalNodes);
        localOutBoundEdgesRev = new ArrayList<>(numLocalNodes);
        localOutOfShardEdges = new ArrayList<>(numLocalNodes);
        localIdToGlobalId = new int[numLocalNodes];
        for (int i = 0; i < numLocalNodes; i++) {
            localOutBoundEdges.add(new ArrayList<>());
            localOutBoundEdgesRev.add(new ArrayList<>());
            localOutOfShardEdges.add(new ArrayList<>());
        }

        int lastId = 0;
        for (com.graph.dist.proto.Node n : request.getNodesList()) {
            globalIdToLocalId.put(n.getId(), lastId);
            localIdToGlobalId[lastId] = n.getId();
            lastId++;
        }

        System.out.println("Worker " + request.getShardId() + " has " + numLocalNodes + " local nodes.");
        // System.out.println("The node ids are " + Arrays.toString(localIdToGlobalId));

        // System.out.println("Global id to local id map: " + globalIdToLocalId);

        for (com.graph.dist.proto.Node n : request.getNodesList()) {
            min_x = Math.min(min_x, n.getX());
            max_x = Math.max(max_x, n.getX());
            min_y = Math.min(min_y, n.getY());
            max_y = Math.max(max_y, n.getY());
        }

        boolean[] isBoundaryNode = new boolean[numLocalNodes];
        for (int i = 0; i < numLocalNodes; i++) {
            isBoundaryNode[i] = false;
        }

        int numBoundaryEdges = 0;
        numBoundaryNodes = 0;

        boundaryIdtoLocalId = new int[numLocalNodes];
        localIdtoBoundaryId = new int[numLocalNodes];
        for (int i = 0; i < numLocalNodes; i++) {
            localIdtoBoundaryId[i] = -1;
            boundaryIdtoLocalId[i] = -1;
        }

        System.out.println("Loading edges...");

        for (com.graph.dist.proto.Edge e : request.getEdgesList()) {
            // System.out.println("Processing edge from " + e.getFrom() + " to " + e.getTo()
            //     + " (to shard " + e.getToShard() + ") with weight " + e.getWeight());

            int localIdFrom = globalIdToLocalId.get(e.getFrom());
            if (globalIdToLocalId.get(e.getTo()) == null) {
                // This is an out-of-shard edge
                numBoundaryEdges++;
                if (isBoundaryNode[localIdFrom] == false) {
                    boundaryIdtoLocalId[numBoundaryNodes] = localIdFrom;
                    localIdtoBoundaryId[localIdFrom] = numBoundaryNodes;
                    numBoundaryNodes++;
                    isBoundaryNode[localIdFrom] = true;
                }

                localOutOfShardEdges.get(localIdFrom).add(new OutOfShardEdge(e.getTo(), e.getToShard(), e.getWeight()));
            } else {
                // This is an internal edge
                int localIdTo = globalIdToLocalId.get(e.getTo());
                localOutBoundEdges.get(localIdFrom).add(new InternalEdge(localIdTo, e.getWeight()));
                localOutBoundEdgesRev.get(localIdTo).add(new InternalEdge(localIdFrom, e.getWeight()));
            }
        }

        System.out.println("Finished loading edges.");

        System.out
                .println("Worker " + request.getShardId() + " loaded " + numLocalNodes + " nodes and "
                        + request.getEdgesCount() + " edges.");
        System.out.println("Boundary nodes: " + numBoundaryNodes);
        System.out.println("Boundary edges: " + numBoundaryEdges);
        System.out.println("X coordinate range: [" + min_x + ", " + max_x + "]");
        System.out.println("Y coordinate range: [" + min_y + ", " + max_y + "]");

        distBoundary = new int[numBoundaryNodes][numBoundaryNodes];
        precomputeBoundaryDistances();
    }

    static class GraphServiceImpl extends GraphServiceGrpc.GraphServiceImplBase {

        @Override
        public void prepareForNewQuery(PrepareForNewQueryRequest request,
                StreamObserver<PrepareForNewQueryResponse> responseObserver) {
            System.out.println("Received PrepareForNewQuery request for query ID " + request.getQueryId());

            if (boundaryNodesDistance == null) {
                boundaryNodesDistance = new int[numBoundaryNodes];
            }
            Arrays.fill(boundaryNodesDistance, Integer.MAX_VALUE);

            if (lastReportedDistanceToBoundaryNode == null) {
                lastReportedDistanceToBoundaryNode = new int[numBoundaryNodes];
            }
            Arrays.fill(lastReportedDistanceToBoundaryNode, Integer.MAX_VALUE);

            if (boundaryNodesSource == null) {
                boundaryNodesSource = new ShortestPathSource[numLocalNodes];
            }
            Arrays.fill(boundaryNodesSource, null);

            updatesFromOtherShards.clear();
            nodesToSendToOtherShards.clear();

            if (executor != null) {
                executor.shutdownNow();
            }

            executor = Executors.newFixedThreadPool(2);
            Runnable processUpdatesTask = () -> {
                System.out.println("Started processing out-of-shard updates thread on worker " + myShardId);
                int numberOfProcessedUpdates = 0;
                while (true) {
                    try {
                        OutOfShardUpdate update = updatesFromOtherShards.take();
                        // System.out.println("Processing out-of-shard update for node "
                        //         + update.toGlobalId + " with new distance " + update.distance);

                        numberOfProcessedUpdates++;
                        if (numberOfProcessedUpdates % 250 == 0) {
                            System.out.println(
                                    "Processed " + numberOfProcessedUpdates + " out-of-shard updates so far...");
                        }
                        int toGlobalId = update.toGlobalId;
                        int fromGlobalId = update.fromGlobalId;
                        int fromShard = update.fromShard;
                        int newDistance = update.distance;

                        int toLocalId = globalIdToLocalId.get(toGlobalId);

                        int boundaryId = localIdtoBoundaryId[toLocalId];

                        if (newDistance < boundaryNodesDistance[boundaryId]) {
                            boundaryNodesDistance[boundaryId] = newDistance;
                            boundaryNodesSource[boundaryId] = new ShortestPathSource(
                                    ShortestPathSource.Type.FROM_OUT_OF_SHARD_UPDATE,
                                    fromGlobalId, fromShard);
                            nodesToSendToOtherShards.add(toLocalId);
                            for (int j = 0; j < numBoundaryNodes; j++) {
                                if (j == boundaryId || distBoundary[boundaryId][j] == Integer.MAX_VALUE)
                                    continue;
                                int propagatedDistance = newDistance + distBoundary[boundaryId][j];
                                if (propagatedDistance < boundaryNodesDistance[j]) {
                                    boundaryNodesDistance[j] = propagatedDistance;
                                    boundaryNodesSource[j] = new ShortestPathSource(
                                            ShortestPathSource.Type.FROM_BOUNDARY_NODE,
                                            localIdToGlobalId[toLocalId],
                                            myShardId);
                                    nodesToSendToOtherShards.add(boundaryIdtoLocalId[j]);
                                }
                            }
                        }

                    } catch (InterruptedException e) {
                        break; // Exit the loop if interrupted
                    } catch (Throwable t) {
                        System.err.println("CRASH in processUpdatesTask!");
                        t.printStackTrace();
                    }
                }
            };

            Runnable sendUpdatesTask = () -> {
                // Configuration
                final int BATCH_SIZE = 50;
                final long FLUSH_INTERVAL_MS = 50;

                // Buffer: Map<TargetShardID, List<Update>>
                ArrayList<ArrayList<UpdateDistanceToNodeData>> buffers = new ArrayList<>(workerClients.size());
                for (int i = 0; i < workerClients.size(); i++) {
                    buffers.add(new ArrayList<>());
                }
                long lastFlushTime = System.currentTimeMillis();

                while (true) {
                    try {
                        // Poll with a timeout so we can flush even if queue is empty
                        Integer localId = nodesToSendToOtherShards.poll(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

                        long currentTime = System.currentTimeMillis();

                        // If we got an item, add to the correct buffer
                        if (localId != null) {
                            int boundaryId = localIdtoBoundaryId[localId];
                            int distance = boundaryNodesDistance[boundaryId];
                            int fromGlobalId = localIdToGlobalId[localId];

                            if (distance == lastReportedDistanceToBoundaryNode[boundaryId]) {
                                // No change since last report, skip
                                continue;
                            }

                            lastReportedDistanceToBoundaryNode[boundaryId] = distance;

                            // A node might have edges to MULTIPLE shards.
                            // We must batch for each target shard separately.
                            for (OutOfShardEdge edge : localOutOfShardEdges.get(localId)) {
                                int targetShard = edge.toShard;

                                // Get or create buffer for this shard
                                ArrayList<UpdateDistanceToNodeData> buffer = buffers.get(targetShard);

                                // Add update
                                buffer.add(UpdateDistanceToNodeData.newBuilder()
                                        .setTargetNode(edge.toGlobalId)
                                        .setDistance(distance + edge.weight)
                                        .setFromNode(fromGlobalId)
                                        .build());

                                // Check if THIS buffer is full
                                if (buffer.size() >= BATCH_SIZE) {
                                    flushBuffer(targetShard, buffer, request.getQueryId());
                                }
                            }
                        }

                        // Time-based Flush (for all shards)
                        // If we haven't flushed in a while, send everything pending
                        if (currentTime - lastFlushTime >= FLUSH_INTERVAL_MS) {
                            for (int shardId = 0; shardId < buffers.size(); shardId++) {
                                if (shardId == myShardId)
                                    continue;
                                ArrayList<UpdateDistanceToNodeData> buffer = buffers.get(shardId);
                                flushBuffer(shardId, buffer, request.getQueryId());
                            }
                            lastFlushTime = currentTime;
                        }

                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            executor.submit(processUpdatesTask);
            executor.submit(sendUpdatesTask);

            PrepareForNewQueryResponse response = PrepareForNewQueryResponse.newBuilder()
                    .setSuccess(true)
                    .build();

            System.out.println("Prepared for new query " + request.getQueryId());

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        private void flushBuffer(int targetShard, ArrayList<UpdateDistanceToNodeData> buffer, int queryId) {
            if (buffer.isEmpty())
                return;
            workerClients.get(targetShard).batchedUpdateDistanceToNode(queryId, myShardId, buffer);
            // Clear the buffer after sending
            buffer.clear();
        }

        @Override
        public void startShortestPathComputation(StartShortestPathComputationRequest request,
                StreamObserver<StartShortestPathComputationResponse> responseObserver) {
            int startGlobalId = request.getStartNode();
            int startLocalId = globalIdToLocalId.get(startGlobalId);

            System.out.println("Starting shortest path computation from node " + startGlobalId + " (local ID "
                    + startLocalId + ")");

            Pair<int[], int[]> distAndFrom = dijkstra(startLocalId, localOutBoundEdges);
            int[] dist = distAndFrom.getFirst();

            for (int i = 0; i < numBoundaryNodes; i++) {
                boundaryNodesDistance[i] = dist[boundaryIdtoLocalId[i]];
                if (boundaryNodesDistance[i] != Integer.MAX_VALUE) {
                    boundaryNodesSource[i] = new ShortestPathSource(ShortestPathSource.Type.FROM_STARTING_NODE,
                            startGlobalId, myShardId);
                    nodesToSendToOtherShards.add(boundaryIdtoLocalId[i]);
                }
            }

            StartShortestPathComputationResponse response = StartShortestPathComputationResponse.newBuilder()
                    .setSuccess(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void retrievePath(RetrievePathRequest request,
                StreamObserver<RetrievePathResponse> responseObserver) {
            int targetGlobalId = request.getTargetNode();
            int targetLocalId = globalIdToLocalId.get(targetGlobalId);

            System.out.println("Retrieving path to target node " + targetGlobalId);

            RetrievePathResponse.Builder responseBuilder = RetrievePathResponse.newBuilder();

            int distance = Integer.MAX_VALUE;
            Pair<int[], int[]> distAndFromRev = dijkstra(targetLocalId, localOutBoundEdgesRev);
            int[] distsRev = distAndFromRev.getFirst();
            int[] fromRev = distAndFromRev.getSecond();
            int bestBoundaryId = -1;
            for (int i = 0; i < numBoundaryNodes; i++) {
                int boundaryLocalId = boundaryIdtoLocalId[i];
                if (distsRev[boundaryLocalId] != Integer.MAX_VALUE &&
                        boundaryNodesDistance[i] != Integer.MAX_VALUE) {
                    if (boundaryNodesDistance[i] + distsRev[boundaryLocalId] < distance) {
                        bestBoundaryId = i;
                        distance = boundaryNodesDistance[i] + distsRev[boundaryLocalId];
                    }
                }
            }

            System.out.println("Computed distance to target node " + targetGlobalId + " is " + distance);

            if (distance != Integer.MAX_VALUE) {
                assert bestBoundaryId != -1;
                // Reconstruct path
                ArrayList<Integer> path = new ArrayList<>();
                int currentLocalId = boundaryIdtoLocalId[bestBoundaryId];

                while (currentLocalId != targetLocalId) {
                    path.add(localIdToGlobalId[currentLocalId]);
                    currentLocalId = fromRev[currentLocalId];
                }
                path.add(localIdToGlobalId[currentLocalId]);

                System.out.println("Reporting path segment: " + path.toString());

                if (!leaderClient.reportPathSegment(path)) {
                    throw new RuntimeException("Failed to report path segment to leader.");
                }
                if (!workerClients.get(myShardId).tracePath(0,
                        localIdToGlobalId[boundaryIdtoLocalId[bestBoundaryId]])) {
                    throw new RuntimeException("Failed to trace path on worker.");
                }
            }

            RetrievePathResponse response = responseBuilder
                    .setDistance(distance)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isWorkerDone(IsWorkerDoneRequest request,
                StreamObserver<IsWorkerDoneResponse> responseObserver) {
            int updatesFromOtherShardsSize = updatesFromOtherShards.size();
            int nodesToSendToOtherShardsSize = nodesToSendToOtherShards.size();
            boolean done = (updatesFromOtherShardsSize == 0) && (nodesToSendToOtherShardsSize == 0);
            System.out.println("isWorkerDone called. Done status: " + done +
                    " (updatesFromOtherShardsSize=" + updatesFromOtherShardsSize +
                    ", nodesToSendToOtherShardsSize=" + nodesToSendToOtherShardsSize + ")");
            IsWorkerDoneResponse response = IsWorkerDoneResponse.newBuilder()
                    .setDone(done)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void batchedUpdateDistanceToNode(BatchedUpdateDistanceToNodeRequest request,
                StreamObserver<BatchedUpdateDistanceToNodeResponse> responseObserver) {
            int fromShard = request.getFromShard();

            for (UpdateDistanceToNodeData data : request.getUpdatesList()) {
                updatesFromOtherShards.add(new OutOfShardUpdate(
                        data.getTargetNode(),
                        data.getFromNode(),
                        fromShard,
                        data.getDistance()));
            }

            BatchedUpdateDistanceToNodeResponse response = BatchedUpdateDistanceToNodeResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void tracePath(TracePathRequest request,
                StreamObserver<TracePathResponse> responseObserver) {
            System.out.println("Received TracePath request for source node " + request.getSourceNode());

            int globalId = request.getSourceNode();
            int localId = globalIdToLocalId.get(globalId);

            int boundaryId = localIdtoBoundaryId[localId];

            ShortestPathSource source = boundaryNodesSource[boundaryId];

            ArrayList<Integer> path = new ArrayList<>();

            if (source.getType() == ShortestPathSource.Type.FROM_STARTING_NODE ||
                    source.getType() == ShortestPathSource.Type.FROM_BOUNDARY_NODE) {
                int fromGlobalId = source.getFromGlobalId();
                int fromLocalId = globalIdToLocalId.get(fromGlobalId);

                Pair<int[], int[]> distAndFrom = dijkstra(fromLocalId, localOutBoundEdges);
                int[] from = distAndFrom.getSecond();

                // Reconstruct path
                int currentLocalId = localId;
                while (currentLocalId != fromLocalId) {
                    path.add(localIdToGlobalId[currentLocalId]);
                    currentLocalId = from[currentLocalId];
                }
                path.add(localIdToGlobalId[fromLocalId]);

                // Reverse the path
                Collections.reverse(path);

            } else {
                // Path starts from an out-of-shard update, so just report the single node
                path.add(globalId);
            }

            System.out.println("Reporting path segment: " + path.toString());

            if (!leaderClient.reportPathSegment(path)) {
                throw new RuntimeException("Failed to report path segment to leader.");
            }

            if (source.getType() == ShortestPathSource.Type.FROM_BOUNDARY_NODE ||
                    source.getType() == ShortestPathSource.Type.FROM_OUT_OF_SHARD_UPDATE) {
                // Continue tracing from the source node
                System.out.println("Continuing trace path to source node " + source.getFromGlobalId()
                        + " from shard " + source.getFromShard());
                if (!workerClients.get(source.getFromShard()).tracePath(0, source.getFromGlobalId())) {
                    throw new RuntimeException("Failed to trace path on worker.");
                }
            }

            TracePathResponse response = TracePathResponse.newBuilder()
                    .setSuccess(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getIntraShardPath(GetIntraShardPathRequest request,
                StreamObserver<GetIntraShardPathResponse> responseObserver) {
            int fromGlobalId = request.getFromNode();
            int toGlobalId = request.getToNode();
            int fromLocalId = globalIdToLocalId.get(fromGlobalId);
            int toLocalId = globalIdToLocalId.get(toGlobalId);

            Pair<int[], int[]> distFromAndFrom = dijkstra(fromLocalId, localOutBoundEdges);
            int[] distances = distFromAndFrom.getFirst();
            int[] from = distFromAndFrom.getSecond();

            int distance = distances[toLocalId];

            ArrayList<Integer> path = new ArrayList<>();

            if (distance != Integer.MAX_VALUE) {
                // Reconstruct path
                int currentLocalId = toLocalId;
                while (currentLocalId != fromLocalId) {
                    path.add(localIdToGlobalId[currentLocalId]);
                    currentLocalId = from[currentLocalId];
                }
                path.add(localIdToGlobalId[fromLocalId]);

                // Reverse the path
                Collections.reverse(path);
            }

            GetIntraShardPathResponse response = GetIntraShardPathResponse.newBuilder()
                    .setDistance(distance)
                    .addAllPath(path)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void hasNode(HasNodeRequest request,
                StreamObserver<HasNodeResponse> responseObserver) {
            int globalId = request.getNodeId();
            boolean hasNode = globalIdToLocalId.containsKey(globalId);

            HasNodeResponse response = HasNodeResponse.newBuilder()
                    .setHasNode(hasNode)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    static void precomputeBoundaryDistances() {
        for (int i = 0; i < distBoundary.length; i++) {
            System.out
                    .println("Precomputing distances from boundary node " + localIdToGlobalId[boundaryIdtoLocalId[i]]
                            + " (" + (i + 1)
                            + "/" + distBoundary.length + ")");

            Pair<int[], int[]> distFromAndFrom = dijkstra(boundaryIdtoLocalId[i], localOutBoundEdges);
            int[] dist = distFromAndFrom.getFirst();

            for (int j = 0; j < distBoundary.length; j++) {
                distBoundary[i][j] = dist[boundaryIdtoLocalId[j]];
            }
        }
    }

    static Pair<int[], int[]> dijkstra(int sourceLocalId, ArrayList<ArrayList<InternalEdge>> localEdges) {
        int[] dist = new int[numLocalNodes];
        int[] from = new int[numLocalNodes];
        boolean[] visited = new boolean[numLocalNodes];
        for (int i = 0; i < numLocalNodes; i++) {
            dist[i] = Integer.MAX_VALUE;
            visited[i] = false;
            from[i] = -1;
        }
        dist[sourceLocalId] = 0;

        PriorityQueue<Pair<Integer, Integer>> pq = new PriorityQueue<>(distBoundary.length,
                Comparator.comparing(Pair::getFirst));
        pq.add(new Pair<>(0, sourceLocalId));

        while (!pq.isEmpty()) {
            Pair<Integer, Integer> p = pq.poll();
            int currentDist = p.getFirst();
            int localId = p.getSecond();
            if (visited[localId])
                continue;
            visited[localId] = true;
            for (InternalEdge v : localEdges.get(localId)) {
                if (visited[v.toLocalId])
                    continue;
                int newDist = currentDist + v.weight;
                if (newDist < dist[v.toLocalId]) {
                    dist[v.toLocalId] = newDist;
                    pq.add(new Pair<>(newDist, v.toLocalId));
                    from[v.toLocalId] = localId;
                }
            }
        }
        return new Pair<>(dist, from);
    }
}