package com.graph.dist.worker;

import org.jgrapht.alg.util.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Utility class for Dijkstra's shortest path algorithm.
 * Extracted from WorkerApp for testability.
 */
public class DijkstraUtil {

    /**
     * Represents an internal edge within a shard.
     */
    public static class Edge {
        public final int toLocalId;
        public final int weight;

        public Edge(int toLocalId, int weight) {
            this.toLocalId = toLocalId;
            this.weight = weight;
        }
    }

    /**
     * Result of Dijkstra's algorithm containing distances and predecessor array.
     */
    public static class DijkstraResult {
        public final int[] distances;
        public final int[] predecessors;

        public DijkstraResult(int[] distances, int[] predecessors) {
            this.distances = distances;
            this.predecessors = predecessors;
        }

        /**
         * Reconstructs the path from source to target.
         * @param target the target node local ID
         * @param source the source node local ID
         * @return list of node IDs in path order, or empty list if unreachable
         */
        public ArrayList<Integer> reconstructPath(int source, int target) {
            ArrayList<Integer> path = new ArrayList<>();
            
            if (distances[target] == Integer.MAX_VALUE) {
                return path; // No path exists
            }

            int current = target;
            while (current != source && current != -1) {
                path.add(current);
                current = predecessors[current];
            }
            
            if (current == source) {
                path.add(source);
                // Reverse to get path from source to target
                java.util.Collections.reverse(path);
            } else {
                path.clear(); // No valid path
            }
            
            return path;
        }
    }

    /**
     * Runs Dijkstra's algorithm from a source node.
     * 
     * @param sourceLocalId the local ID of the source node
     * @param adjacencyList adjacency list where index is node local ID
     * @param numNodes total number of nodes
     * @return DijkstraResult containing distances and predecessors
     */
    public static DijkstraResult dijkstra(
            int sourceLocalId, 
            ArrayList<ArrayList<Edge>> adjacencyList, 
            int numNodes) {
        
        int[] dist = new int[numNodes];
        int[] from = new int[numNodes];
        boolean[] visited = new boolean[numNodes];
        
        for (int i = 0; i < numNodes; i++) {
            dist[i] = Integer.MAX_VALUE;
            visited[i] = false;
            from[i] = -1;
        }
        dist[sourceLocalId] = 0;

        PriorityQueue<Pair<Integer, Integer>> pq = new PriorityQueue<>(
                Math.max(1, numNodes),
                Comparator.comparing(Pair::getFirst));
        pq.add(new Pair<>(0, sourceLocalId));

        while (!pq.isEmpty()) {
            Pair<Integer, Integer> p = pq.poll();
            int currentDist = p.getFirst();
            int localId = p.getSecond();
            
            if (visited[localId]) {
                continue;
            }
            visited[localId] = true;
            
            for (Edge edge : adjacencyList.get(localId)) {
                if (visited[edge.toLocalId]) {
                    continue;
                }
                int newDist = currentDist + edge.weight;
                if (newDist < dist[edge.toLocalId]) {
                    dist[edge.toLocalId] = newDist;
                    pq.add(new Pair<>(newDist, edge.toLocalId));
                    from[edge.toLocalId] = localId;
                }
            }
        }
        
        return new DijkstraResult(dist, from);
    }
}
