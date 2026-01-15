package com.graph.dist.leader;

import com.graph.dist.utils.Point;
import java.util.*;

/**
 * Manages the creation and distribution of shards.
 * Keeps track of shards so they can be resent to workers if pods restart.
 */
public class ShardManager {
    
    private final DimacsParser.GraphData graphData;
    private final int numWorkers;
    
    // Stores the final shards indexed by shard ID (worker index)
    private final Map<Integer, Shard> shardAssignments = new HashMap<>();
    
    // Maps node ID to shard ID for quick lookup
    private Map<Integer, Integer> nodeToShardId;

    public ShardManager(DimacsParser.GraphData graphData, int numWorkers) {
        this.graphData = graphData;
        this.numWorkers = numWorkers;
    }

    /**
     * Creates shards by iteratively splitting the largest shard until
     * we have exactly numWorkers shards.
     * 
     * Algorithm:
     * 1. Start with one shard containing all nodes
     * 2. Use a max-heap (priority queue) sorted by node count
     * 3. Repeatedly pick the largest shard and split it along the longer edge
     * 4. Use binary search to find the optimal split coordinate
     * 5. Continue until n_shards == n_workers
     */
    public void createShards() {
        System.out.println("Creating " + numWorkers + " shards using spatial partitioning...");

        // Priority queue sorted by node count (descending)
        PriorityQueue<Shard> shardQueue = new PriorityQueue<>(
            Comparator.comparingInt(Shard::getNodeCount).reversed()
        );

        // Start with a single shard containing all nodes
        Shard initialShard = Shard.createInitialShard(graphData.coords);
        shardQueue.add(initialShard);

        System.out.println("Initial shard: " + initialShard);

        // Iterate until we have the desired number of shards
        while (shardQueue.size() < numWorkers) {
            // Pick the shard with the most nodes
            Shard largestShard = shardQueue.poll();
            
            if (largestShard == null || largestShard.getNodeCount() <= 1) {
                System.err.println("Warning: Cannot split further. " + 
                                   "Shard has " + (largestShard != null ? largestShard.getNodeCount() : 0) + " nodes.");
                if (largestShard != null) {
                    shardQueue.add(largestShard);
                }
                break;
            }

            System.out.println("Splitting shard with " + largestShard.getNodeCount() + 
                               " nodes (width=" + largestShard.getWidth() + 
                               ", height=" + largestShard.getHeight() + ")");

            // Split the shard
            Shard[] newShards = largestShard.split(graphData.coords);
            
            System.out.println("  -> Created: " + newShards[0] + " and " + newShards[1]);

            // Add the new shards to the queue
            for (Shard shard : newShards) {
                if (shard.getNodeCount() > 0) {
                    shardQueue.add(shard);
                }
            }
        }

        // Assign shard IDs and store them
        int shardId = 0;
        nodeToShardId = new HashMap<>();
        
        for (Shard shard : shardQueue) {
            shardAssignments.put(shardId, shard);
            
            // Build node to shard mapping
            for (int nodeId : shard.nodeIds) {
                nodeToShardId.put(nodeId, shardId);
            }
            
            System.out.println("Shard " + shardId + ": " + shard);
            shardId++;
        }

        System.out.println("Created " + shardAssignments.size() + " shards.");
    }

    /**
     * Gets the shard ID for a given node.
     */
    public int getShardIdForNode(int nodeId) {
        return nodeToShardId.getOrDefault(nodeId, -1);
    }

    /**
     * Gets a shard by its ID.
     */
    public Shard getShard(int shardId) {
        return shardAssignments.get(shardId);
    }

    /**
     * Gets all shard assignments.
     */
    public Map<Integer, Shard> getShardAssignments() {
        return Collections.unmodifiableMap(shardAssignments);
    }

    /**
     * Gets the node to shard ID mapping.
     */
    public Map<Integer, Integer> getNodeToShardMapping() {
        return Collections.unmodifiableMap(nodeToShardId);
    }

    /**
     * Returns the number of shards created.
     */
    public int getShardCount() {
        return shardAssignments.size();
    }

    /**
     * Sends a specific shard to its assigned worker.
     * Used for initial distribution or when a worker pod restarts.
     * 
     * @param shardId The ID of the shard to send
     * @return true if successful, false otherwise
     */
    public boolean sendShardToWorker(int shardId) {
        Shard shard = shardAssignments.get(shardId);
        if (shard == null) {
            System.err.println("Shard " + shardId + " not found.");
            return false;
        }

        String host = "worker-" + shardId;
        System.out.println("Sending shard " + shardId + " to " + host + 
                           " with " + shard.getNodeCount() + " nodes.");

        try {
            WorkerClient client = new WorkerClient(host, 9090);
            boolean success = client.loadShard(buildShardData(shardId, shard));
            client.shutdown();
            return success;
        } catch (Exception e) {
            System.err.println("Failed to send shard " + shardId + " to " + host + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Sends all shards to their respective workers.
     */
    public void distributeAllShards() {
        System.out.println("Distributing " + shardAssignments.size() + " shards to workers...");
        
        for (int shardId : shardAssignments.keySet()) {
            sendShardToWorker(shardId);
        }
    }

    /**
     * Builds the protobuf ShardData message for a shard.
     */
    private com.graph.dist.proto.ShardData buildShardData(int shardId, Shard shard) {
        com.graph.dist.proto.ShardData.Builder builder = com.graph.dist.proto.ShardData.newBuilder();
        builder.setShardId(shardId);

        // Add nodes with their coordinates
        for (int nodeId : shard.nodeIds) {
            Point pt = graphData.coords.get(nodeId);
            builder.addNodes(
                com.graph.dist.proto.Node.newBuilder()
                    .setId(nodeId)
                    .setX(pt.x)
                    .setY(pt.y)
                    .build()
            );
        }

        // Add edges that originate from nodes in this shard
        Set<Integer> nodeSet = new HashSet<>(shard.nodeIds);
        for (DimacsParser.Edge edge : graphData.edges) {
            if (nodeSet.contains(edge.from)) {
                int toShard = getShardIdForNode(edge.to);
                builder.addEdges(
                    com.graph.dist.proto.Edge.newBuilder()
                        .setFrom(edge.from)
                        .setTo(edge.to)
                        .setToShard(toShard)
                        .setWeight(edge.weight)
                        .build()
                );
            }
        }

        return builder.build();
    }
}
