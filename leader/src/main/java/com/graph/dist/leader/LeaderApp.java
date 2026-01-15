package com.graph.dist.leader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.graph.dist.proto.ShardData;
import com.graph.dist.proto.Node;
import com.graph.dist.proto.Edge;

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

    public static Map<Integer, Integer> splitNodesIntoShards(DimacsParser.GraphData data, int numWorkers) {
        Map<Integer, Integer> nodeToShard = new HashMap<>();

        // 1. Sort nodes by X coordinate
        List<Integer> sortedIds = new ArrayList<>(data.coords.keySet());
        sortedIds.sort(Comparator.comparingDouble(id -> data.coords.get(id).x));

        int shardSize = (int) Math.ceil((double) sortedIds.size() / numWorkers);

        // 2. Assign nodes to shards
        for (int i = 0; i < numWorkers; i++) {
            int start = i * shardSize;
            int end = Math.min(start + shardSize, sortedIds.size());
            for (int j = start; j < end; j++) {
                int id = sortedIds.get(j);
                nodeToShard.put(id, i);
            }
        }

        return nodeToShard;
    }

    public static void distributeShards(DimacsParser.GraphData data, int numWorkers) {
        System.out.println("Distributing shards to " + numWorkers + " workers...");

        Map<Integer, Integer> nodeToShard = splitNodesIntoShards(data, numWorkers);

        // Prepare shard data builders
        List<ShardData.Builder> builders = java.util.stream.IntStream.range(0, numWorkers)
                .mapToObj(i -> {
                    ShardData.Builder builder = ShardData.newBuilder();
                    builder.setShardId(i);
                    return builder;
                })
                .collect(java.util.stream.Collectors.toList());

        // Add nodes with coordinates to respective shards
        data.coords.forEach((nodeId, pt) -> {
            int shardId = nodeToShard.get(nodeId);
            builders.get(shardId).addNodes(
                    Node.newBuilder()
                            .setId(nodeId)
                            .setX(pt.x)
                            .setY(pt.y)
                            .build());
        });

        // Add edges to respective shards
        data.edges.forEach((edge) -> {
            int fromShard = nodeToShard.get(edge.from);
            int toShard = nodeToShard.get(edge.to);

            builders.get(fromShard).addEdges(
                    Edge.newBuilder()
                            .setFrom(edge.from)
                            .setTo(edge.to)
                            .setToShard(toShard)
                            .setWeight(edge.weight)
                            .build());
        });

        // 4. Send to Workers
        String workerServiceName = System.getenv().getOrDefault("WORKER_SERVICE_NAME", "worker");
        String namespace = System.getenv().getOrDefault("NAMESPACE", "");
        int workerPort = Integer.parseInt(System.getenv().getOrDefault("WORKER_PORT", "9090"));
        
        for (int i = 0; i < numWorkers; i++) {
            // For Kubernetes StatefulSet: worker-0.worker.graph-dist.svc.cluster.local
            // For Docker Compose: worker-0
            String host;
            if (namespace.isEmpty()) {
                // Docker Compose mode
                host = "worker-" + i;
            } else {
                // Kubernetes mode: <pod-name>.<service-name>.<namespace>.svc.cluster.local
                host = "worker-" + i + "." + workerServiceName + "." + namespace + ".svc.cluster.local";
            }
            
            System.out.println("Sending shard " + i + " to " + host + " with "
                    + builders.get(i).getNodesCount() + " nodes.");
            WorkerClient client = new WorkerClient(host, workerPort);
            client.loadShard(builders.get(i).build());
            client.shutdown();
        }
    }
}