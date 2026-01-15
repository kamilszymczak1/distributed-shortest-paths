package com.graph.dist.leader;

public class LeaderApp {

    // Keep the shard manager as a static field so shards are remembered
    // and can be resent if a worker pod dies and restarts
    private static ShardManager shardManager;

    public static int getNumWorkers() {
        String numWorkers = System.getenv().getOrDefault("NUM_WORKERS", "1");
        return Integer.parseInt(numWorkers);
    }

    public static void main(String[] args) {
        System.out.println("Leader starting...");

        DimacsParser.GraphData graphData = null;

        try {
            graphData = DimacsParser.parse("/data/graph.co.gz", "/data/graph.gr.gz");
            System.out.println("Graph parsed with " + graphData.coords.size() + " coordinates.");
        } catch (Exception e) {
            System.err.println("Failed to parse graph: " + e.getMessage());
            return;
        }

        int numWorkers = getNumWorkers();

        // Create and distribute shards using the new spatial partitioning algorithm
        shardManager = new ShardManager(graphData, numWorkers);
        shardManager.createShards();
        shardManager.distributeAllShards();

        System.out.println("Leader initialization complete. Shards are stored for potential resending.");
    }

    /**
     * Returns the shard manager, which can be used to resend shards to workers
     * if they restart.
     */
    public static ShardManager getShardManager() {
        return shardManager;
    }

    /**
     * Resends a shard to a specific worker. Useful when a worker pod restarts.
     * 
     * @param workerId The worker/shard ID to resend
     * @return true if successful, false otherwise
     */
    public static boolean resendShardToWorker(int workerId) {
        if (shardManager == null) {
            System.err.println("ShardManager not initialized.");
            return false;
        }
        return shardManager.sendShardToWorker(workerId);
    }
}