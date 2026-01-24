package com.graph.dist.leader;

import com.graph.dist.utils.Point;
import com.graph.dist.proto.ShardData;
import com.graph.dist.proto.Node;
import com.graph.dist.proto.Edge;
import java.util.*;
import java.io.*;

import org.jgrapht.alg.util.Pair;

/**
 * Manages the creation and distribution of shards.
 * Keeps track of shards so they can be resent to workers if pods restart.
 * 
 * Shards are represented as disjoint rectangles. The file is only read
 * when shard data is requested.
 */
public class ShardManager {

    private final String coordinateFilePath;
    private final String edgeFilePath;
    private final int numWorkers;

    // Stores the shard rectangles indexed by shard ID (worker index)
    // Only stores bounds, not the actual nodes
    private final ArrayList<ShardBounds> shardBounds = new ArrayList<>();

    /**
     * Represents the rectangular bounds of a shard (disjoint rectangles).
     */
    private static class ShardBounds {
        private final int xMin, xMax, yMin, yMax;

        public ShardBounds(int xMin, int xMax, int yMin, int yMax) {
            this.xMin = xMin;
            this.xMax = xMax;
            this.yMin = yMin;
            this.yMax = yMax;
        }

        public boolean contains(int x, int y) {
            return x >= xMin && x <= xMax && y >= yMin && y <= yMax;
        }

        @Override
        public String toString() {
            return String.format("ShardBounds[x:(%d,%d), y:(%d,%d)]", xMin, xMax, yMin, yMax);
        }
    }

    private static class Shard {
        private final ArrayList<Pair<Integer, Point>> nodes; // List of (nodeId, Point)

        public Shard(ArrayList<Pair<Integer, Point>> nodes) {
            assert nodes != null && !nodes.isEmpty();
            this.nodes = nodes;
        }

        public int getMaxX() {
            return nodes.stream().mapToInt(p -> p.getSecond().x).max().orElse(0);
        }

        public int getMinX() {
            return nodes.stream().mapToInt(p -> p.getSecond().x).min().orElse(0);
        }

        public int getMaxY() {
            return nodes.stream().mapToInt(p -> p.getSecond().y).max().orElse(0);
        }

        public int getMinY() {
            return nodes.stream().mapToInt(p -> p.getSecond().y).min().orElse(0);
        }

        public int getNodeCount() {
            return nodes.size();
        }

        public Pair<Shard, Shard> split() {
            if (nodes.size() <= 1) {
                return null;
            }
            // Determine split direction
            boolean splitVertically = (getMaxX() - getMinX()) >= (getMaxY() - getMinY());

            // If splitting horizontally, swap x and y for sorting
            if (!splitVertically) {
                swapXY(nodes);
            }

            // Sort nodes by x coordinate
            nodes.sort(Comparator.comparingInt(p -> p.getSecond().x));

            int size = nodes.size();

            int middleX = size % 2 == 0
                    ? (nodes.get(size / 2 - 1).getSecond().x + nodes.get(size / 2).getSecond().x) / 2
                    : nodes.get(size / 2).getSecond().x;

            ArrayList<Pair<Integer, Point>> leftNodes = new ArrayList<>();
            ArrayList<Pair<Integer, Point>> rightNodes = new ArrayList<>();

            for (var p : nodes) {
                if (p.getSecond().x <= middleX) {
                    leftNodes.add(p);
                } else {
                    rightNodes.add(p);
                }
            }

            if (!splitVertically) {
                // Swap back x and y
                swapXY(leftNodes);
                swapXY(rightNodes);
            }

            return new Pair<>(new Shard(leftNodes), new Shard(rightNodes));
        }

        private void swapXY(ArrayList<Pair<Integer, Point>> nodeList) {
            for (var p : nodeList) {
                int temp = p.getSecond().x;
                p.getSecond().x = p.getSecond().y;
                p.getSecond().y = temp;
            }
        }

        @Override public String toString() {
            return String.format("Shard[nodes:%d, x:(%d,%d), y:(%d,%d)]",
                    getNodeCount(), getMinX(), getMaxX(), getMinY(), getMaxY());
        }
    }

    public ShardManager(String coordinateFilePath, String edgeFilePath, int numWorkers) {
        this.coordinateFilePath = coordinateFilePath;
        this.edgeFilePath = edgeFilePath;
        this.numWorkers = numWorkers;

        // Perform sharding upon construction
        createShards();
    }

    /**
     * Creates shards by iteratively splitting the largest shard until
     * we have exactly numWorkers shards.
     * 
     * Algorithm:
     * 1. Start with one shard containing all nodes
     * 2. Use a max-heap (priority queue) sorted by node count
     * 3. Repeatedly pick the largest shard and split it along the longer edge
     * 4. Split it into two shards with approximately equal number of nodes
     * 5. Continue until n_shards == n_workers
     */
    public void createShards() {
        System.out.println("Creating " + numWorkers + " shards using spatial partitioning...");

        // Priority queue sorted by node count (descending)
        PriorityQueue<Shard> shardQueue = new PriorityQueue<>(
                Comparator.comparingInt(Shard::getNodeCount).reversed());

        // Start with a single shard containing all nodes
        ArrayList<Pair<Integer, Point>> coords;
        try (var vertexStream = DimacsParser.streamVertices(coordinateFilePath)) {
            coords = vertexStream.map(v -> {
                return new Pair<Integer, Point>(v.id, v.point);
            }).collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load coordinates from " + coordinateFilePath, e);
        }
        Shard initialShard = new Shard(coords);
        shardQueue.add(initialShard);

        System.out.println("Initial shard: " + initialShard);

        // Iterate until we have the desired number of shards
        while (shardQueue.size() < numWorkers) {
            // Pick the shard with the most nodes
            Shard largestShard = shardQueue.poll();

            if (largestShard == null || largestShard.getNodeCount() <= 1) {
                System.err.println("Warning: Cannot split shard " + (largestShard == null ? "null" : largestShard) +
                        " further due to insufficient nodes.");
                if (largestShard != null) {
                    shardQueue.add(largestShard);
                }
                break;
            }

            System.out.println("Splitting shard " + largestShard);

            // Split the shard
            Pair<Shard, Shard> splitShards = largestShard.split();

            // If split failed (e.g., due to insufficient nodes), re-add the original shard
            if (splitShards == null) {
                System.err.println("Warning: Split resulted in null shards for " + largestShard);
                shardQueue.add(largestShard);
                break;
            }

            shardQueue.add(splitShards.getFirst());
            shardQueue.add(splitShards.getSecond());
        }

        for (Shard shard : shardQueue) {
            int xMin = shard.getMinX();
            int xMax = shard.getMaxX();
            int yMin = shard.getMinY();
            int yMax = shard.getMaxY();
            shardBounds.add(new ShardBounds(xMin, xMax, yMin, yMax));
            System.out.println("Created shard: " + shard);
        }

        System.out.println("Created " + shardBounds.size() + " shards.");
    }

    /**
     * Returns the protobuf ShardData message for a shard.
     */
    public ShardData getShardData(int shardId) {
        ShardBounds bounds;
        try {
            bounds = shardBounds.get(shardId);
        } catch (IndexOutOfBoundsException e) {
            // Invalid shard ID
            System.err.println("Invalid shard ID requested: " + shardId);
            return null;
        }

        ShardData.Builder builder = ShardData.newBuilder();
        builder.setShardId(shardId);

        HashSet<Integer> nodeIdsInShard = new HashSet<>();

        try (var vertexStream = DimacsParser.streamVertices(coordinateFilePath)) {
            builder.addAllNodes(
                vertexStream
                    .filter(v -> bounds.contains(v.point.x, v.point.y))
                    .map(v -> {
                        nodeIdsInShard.add(v.id);
                        return Node.newBuilder()
                            .setId(v.id)
                            .setX(v.point.x)
                            .setY(v.point.y)
                            .build();})
                    .toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load coordinates from " + coordinateFilePath, e);
        }

        try (var edgeStream = DimacsParser.streamEdges(edgeFilePath)) {
            HashSet<Integer> nodesWithUnknownShard = new HashSet<>();
            List<DimacsParser.Edge> edges = edgeStream
                .filter(e -> { return nodeIdsInShard.contains(e.from); } )
                .toList();

            HashSet<Integer> nodesWithUknownShard = new HashSet<>();

            for (DimacsParser.Edge e : edges) {
                if (!nodeIdsInShard.contains(e.to)) {
                    nodesWithUknownShard.add(e.to);
                }
            }

            HashMap<Integer, Integer> nodeIdToShardId = new HashMap<>();

            try (var vertexStream = DimacsParser.streamVertices(coordinateFilePath)) {
                vertexStream.forEach(
                    v -> {
                        if (nodesWithUknownShard.contains(v.id)) {
                            for (int shardIdx = 0; shardIdx < shardBounds.size(); shardIdx++) {
                                ShardBounds sb = shardBounds.get(shardIdx);
                                if (sb.contains(v.point.x, v.point.y)) {
                                    nodeIdToShardId.put(v.id, shardIdx);
                                    break;
                                }
                            }
                        }
                    }
                );
            } catch (IOException e) {
                throw new RuntimeException("Failed to load coordinates from " + coordinateFilePath, e);
            }

            builder.addAllEdges(
                edges.stream().map(e -> {
                    int toShardId = nodeIdsInShard.contains(e.to)
                        ? shardId
                        : nodeIdToShardId.get(e.to);
                    return Edge.newBuilder()
                        .setFrom(e.from)
                        .setTo(e.to)
                        .setToShard(toShardId)
                        .setWeight(e.weight)
                        .build();
                }).toList()
            );

        } catch (IOException e) {
            throw new RuntimeException("Failed to load edges from " + edgeFilePath, e);
        }

        return builder.build();
    }
}
