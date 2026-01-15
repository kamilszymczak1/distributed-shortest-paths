package com.graph.dist.leader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.graph.dist.leader.DimacsParser;
import com.graph.dist.leader.WorkerClient;
import com.graph.dist.utils.Point;

public class LeaderApp {

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

        distributeShards(graphData, numWorkers);
    }

    public static void distributeShards(DimacsParser.GraphData data, int numWorkers) {
        System.out.println("Distributing shards to " + numWorkers + " workers...");

        // 1. Sort nodes by X coordinate
        List<Integer> sortedIds = new ArrayList<>(data.coords.keySet());
        sortedIds.sort(Comparator.comparingDouble(id -> data.coords.get(id).x));

        int shardSize = (int) Math.ceil((double) sortedIds.size() / numWorkers);
        Map<Integer, Integer> nodeToShard = new HashMap<>();

        // 2. Prepare Builders
        List<com.graph.dist.proto.ShardData.Builder> builders = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            builders.add(com.graph.dist.proto.ShardData.newBuilder().setShardId(i));

            int start = i * shardSize;
            int end = Math.min(start + shardSize, sortedIds.size());
            for (int j = start; j < end; j++) {
                int id = sortedIds.get(j);
                Point c = data.coords.get(id);
                builders.get(i).addNodes(com.graph.dist.proto.Node.newBuilder().setId(id).setX(c.x).setY(c.y));
                nodeToShard.put(id, i);
            }
        }

        // 4. Send to Workers
        for (int i = 0; i < numWorkers; i++) {
            String host = "worker-" + i; // docker-compose service name
            System.out.println("Sending shard " + i + " to " + host + " with "
                    + builders.get(i).getNodesCount() + " nodes.");
            WorkerClient client = new WorkerClient(host, 9090);
            client.loadShard(builders.get(i).build());
        }
    }
}