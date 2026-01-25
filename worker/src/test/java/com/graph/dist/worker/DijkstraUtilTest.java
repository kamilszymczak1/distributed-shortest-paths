package com.graph.dist.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DijkstraUtil Tests")
class DijkstraUtilTest {

    private ArrayList<ArrayList<DijkstraUtil.Edge>> graph;

    @Nested
    @DisplayName("Simple Graph Tests")
    class SimpleGraphTests {

        @BeforeEach
        void setUp() {
            // Create a simple graph:
            //     0 --5-- 1
            //     |       |
            //     3       2
            //     |       |
            //     2 --1-- 3
            //
            // Node 0 connects to: 1 (weight 5), 2 (weight 3)
            // Node 1 connects to: 0 (weight 5), 3 (weight 2)
            // Node 2 connects to: 0 (weight 3), 3 (weight 1)
            // Node 3 connects to: 1 (weight 2), 2 (weight 1)
            
            graph = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                graph.add(new ArrayList<>());
            }
            
            // Node 0
            graph.get(0).add(new DijkstraUtil.Edge(1, 5));
            graph.get(0).add(new DijkstraUtil.Edge(2, 3));
            
            // Node 1
            graph.get(1).add(new DijkstraUtil.Edge(0, 5));
            graph.get(1).add(new DijkstraUtil.Edge(3, 2));
            
            // Node 2
            graph.get(2).add(new DijkstraUtil.Edge(0, 3));
            graph.get(2).add(new DijkstraUtil.Edge(3, 1));
            
            // Node 3
            graph.get(3).add(new DijkstraUtil.Edge(1, 2));
            graph.get(3).add(new DijkstraUtil.Edge(2, 1));
        }

        @Test
        @DisplayName("Distance to self should be zero")
        void distanceToSelfShouldBeZero() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            assertEquals(0, result.distances[0]);
        }

        @Test
        @DisplayName("Should find direct neighbor distance")
        void shouldFindDirectNeighborDistance() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            // 0 -> 1 has weight 5
            assertEquals(5, result.distances[1]);
            // 0 -> 2 has weight 3
            assertEquals(3, result.distances[2]);
        }

        @Test
        @DisplayName("Should find shortest path through intermediate nodes")
        void shouldFindShortestPathThroughIntermediateNodes() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            // Shortest path from 0 to 3:
            // Option 1: 0 -> 1 -> 3 = 5 + 2 = 7
            // Option 2: 0 -> 2 -> 3 = 3 + 1 = 4 (shorter!)
            assertEquals(4, result.distances[3]);
        }

        @Test
        @DisplayName("Should set correct predecessors")
        void shouldSetCorrectPredecessors() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            // Source has no predecessor
            assertEquals(-1, result.predecessors[0]);
            
            // Node 3's predecessor should be 2 (via shortest path)
            assertEquals(2, result.predecessors[3]);
        }

        @Test
        @DisplayName("Should reconstruct path correctly")
        void shouldReconstructPathCorrectly() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            ArrayList<Integer> path = result.reconstructPath(0, 3);
            
            // Shortest path is 0 -> 2 -> 3
            assertEquals(Arrays.asList(0, 2, 3), path);
        }

        @Test
        @DisplayName("Path to self should contain only source")
        void pathToSelfShouldContainOnlySource() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            ArrayList<Integer> path = result.reconstructPath(0, 0);
            
            assertEquals(Arrays.asList(0), path);
        }
    }

    @Nested
    @DisplayName("Disconnected Graph Tests")
    class DisconnectedGraphTests {

        @BeforeEach
        void setUp() {
            // Create disconnected graph:
            // 0 -- 1     2 -- 3 (two separate components)
            graph = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                graph.add(new ArrayList<>());
            }
            
            graph.get(0).add(new DijkstraUtil.Edge(1, 1));
            graph.get(1).add(new DijkstraUtil.Edge(0, 1));
            graph.get(2).add(new DijkstraUtil.Edge(3, 1));
            graph.get(3).add(new DijkstraUtil.Edge(2, 1));
        }

        @Test
        @DisplayName("Distance to unreachable node should be MAX_VALUE")
        void distanceToUnreachableNodeShouldBeMaxValue() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            assertEquals(Integer.MAX_VALUE, result.distances[2]);
            assertEquals(Integer.MAX_VALUE, result.distances[3]);
        }

        @Test
        @DisplayName("Path to unreachable node should be empty")
        void pathToUnreachableNodeShouldBeEmpty() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            ArrayList<Integer> path = result.reconstructPath(0, 2);
            
            assertTrue(path.isEmpty());
        }

        @Test
        @DisplayName("Should still find reachable nodes")
        void shouldStillFindReachableNodes() {
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);
            
            assertEquals(0, result.distances[0]);
            assertEquals(1, result.distances[1]);
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("Should handle single node graph")
        void shouldHandleSingleNodeGraph() {
            graph = new ArrayList<>();
            graph.add(new ArrayList<>());

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 1);

            assertEquals(0, result.distances[0]);
            assertEquals(-1, result.predecessors[0]);
        }

        @Test
        @DisplayName("Should handle node with no outgoing edges")
        void shouldHandleNodeWithNoOutgoingEdges() {
            graph = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                graph.add(new ArrayList<>());
            }
            // Only edges: 0 -> 1 -> 2 (one direction)
            graph.get(0).add(new DijkstraUtil.Edge(1, 5));
            graph.get(1).add(new DijkstraUtil.Edge(2, 3));

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 3);

            assertEquals(0, result.distances[0]);
            assertEquals(5, result.distances[1]);
            assertEquals(8, result.distances[2]);
        }

        @Test
        @DisplayName("Should handle large weights without overflow")
        void shouldHandleLargeWeightsWithoutOverflow() {
            graph = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                graph.add(new ArrayList<>());
            }
            // Use large weights
            graph.get(0).add(new DijkstraUtil.Edge(1, 1000000000));
            graph.get(1).add(new DijkstraUtil.Edge(2, 1000000000));

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 3);

            assertEquals(0, result.distances[0]);
            assertEquals(1000000000, result.distances[1]);
            assertEquals(2000000000, result.distances[2]);
        }

        @Test
        @DisplayName("Should prefer shorter path even with more hops")
        void shouldPreferShorterPathEvenWithMoreHops() {
            // Graph: 0 -> 1 (weight 10)
            //        0 -> 2 -> 3 -> 1 (weights 1, 1, 1 = 3 total)
            graph = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                graph.add(new ArrayList<>());
            }
            
            graph.get(0).add(new DijkstraUtil.Edge(1, 10));
            graph.get(0).add(new DijkstraUtil.Edge(2, 1));
            graph.get(2).add(new DijkstraUtil.Edge(3, 1));
            graph.get(3).add(new DijkstraUtil.Edge(1, 1));

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);

            // Shorter path through 2 and 3: 1 + 1 + 1 = 3
            assertEquals(3, result.distances[1]);
        }

        @Test
        @DisplayName("Should handle zero weight edges")
        void shouldHandleZeroWeightEdges() {
            graph = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                graph.add(new ArrayList<>());
            }
            
            graph.get(0).add(new DijkstraUtil.Edge(1, 0));
            graph.get(1).add(new DijkstraUtil.Edge(2, 0));

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 3);

            assertEquals(0, result.distances[0]);
            assertEquals(0, result.distances[1]);
            assertEquals(0, result.distances[2]);
        }
    }

    @Nested
    @DisplayName("Complex Graph Tests")
    class ComplexGraphTests {

        @Test
        @DisplayName("Should handle diamond-shaped graph")
        void shouldHandleDiamondShapedGraph() {
            //       1
            //      /2\3
            //     0   3
            //      \1/4
            //       2
            graph = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                graph.add(new ArrayList<>());
            }
            
            graph.get(0).add(new DijkstraUtil.Edge(1, 2));
            graph.get(0).add(new DijkstraUtil.Edge(2, 1));
            graph.get(1).add(new DijkstraUtil.Edge(3, 3));
            graph.get(2).add(new DijkstraUtil.Edge(3, 4));

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 4);

            // Path 0 -> 1 -> 3 = 2 + 3 = 5
            // Path 0 -> 2 -> 3 = 1 + 4 = 5
            // Both are equal, either path is valid
            assertEquals(5, result.distances[3]);
        }

        @Test
        @DisplayName("Should handle parallel edges")
        void shouldHandleParallelEdges() {
            graph = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                graph.add(new ArrayList<>());
            }
            
            // Two edges from 0 to 1 with different weights
            graph.get(0).add(new DijkstraUtil.Edge(1, 10));
            graph.get(0).add(new DijkstraUtil.Edge(1, 5));  // Shorter

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, 2);

            // Should take the shorter edge
            assertEquals(5, result.distances[1]);
        }

        @ParameterizedTest
        @DisplayName("Should compute correct distances for all source nodes")
        @CsvSource({
            "0, 0, 0",
            "0, 1, 5",
            "1, 0, 5",
            "2, 3, 1"
        })
        void shouldComputeCorrectDistancesForAllSourceNodes(int source, int target, int expectedDist) {
            // Recreate simple 4-node graph
            graph = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                graph.add(new ArrayList<>());
            }
            graph.get(0).add(new DijkstraUtil.Edge(1, 5));
            graph.get(0).add(new DijkstraUtil.Edge(2, 3));
            graph.get(1).add(new DijkstraUtil.Edge(0, 5));
            graph.get(1).add(new DijkstraUtil.Edge(3, 2));
            graph.get(2).add(new DijkstraUtil.Edge(0, 3));
            graph.get(2).add(new DijkstraUtil.Edge(3, 1));
            graph.get(3).add(new DijkstraUtil.Edge(1, 2));
            graph.get(3).add(new DijkstraUtil.Edge(2, 1));

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(source, graph, 4);

            assertEquals(expectedDist, result.distances[target]);
        }
    }

    @Nested
    @DisplayName("Edge Class Tests")
    class EdgeClassTests {

        @Test
        @DisplayName("Should create edge with correct values")
        void shouldCreateEdgeWithCorrectValues() {
            DijkstraUtil.Edge edge = new DijkstraUtil.Edge(5, 100);

            assertEquals(5, edge.toLocalId);
            assertEquals(100, edge.weight);
        }
    }

    @Nested
    @DisplayName("DijkstraResult Class Tests")
    class DijkstraResultClassTests {

        @Test
        @DisplayName("Should create result with arrays")
        void shouldCreateResultWithArrays() {
            int[] distances = {0, 5, 10};
            int[] predecessors = {-1, 0, 1};

            DijkstraUtil.DijkstraResult result = new DijkstraUtil.DijkstraResult(distances, predecessors);

            assertArrayEquals(distances, result.distances);
            assertArrayEquals(predecessors, result.predecessors);
        }
    }

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should handle moderately large graph efficiently")
        void shouldHandleModeratelyLargeGraphEfficiently() {
            // Create a chain graph with 1000 nodes
            int numNodes = 1000;
            graph = new ArrayList<>();
            for (int i = 0; i < numNodes; i++) {
                graph.add(new ArrayList<>());
            }
            
            // Create chain: 0 -> 1 -> 2 -> ... -> 999
            for (int i = 0; i < numNodes - 1; i++) {
                graph.get(i).add(new DijkstraUtil.Edge(i + 1, 1));
            }

            long startTime = System.currentTimeMillis();
            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, numNodes);
            long endTime = System.currentTimeMillis();

            // Verify correctness
            assertEquals(0, result.distances[0]);
            assertEquals(numNodes - 1, result.distances[numNodes - 1]);

            // Should complete in reasonable time (< 1 second)
            assertTrue(endTime - startTime < 1000, 
                "Dijkstra should complete in less than 1 second for 1000 nodes");
        }

        @Test
        @DisplayName("Should handle dense graph")
        void shouldHandleDenseGraph() {
            // Create a fully connected graph with 100 nodes
            int numNodes = 100;
            graph = new ArrayList<>();
            for (int i = 0; i < numNodes; i++) {
                graph.add(new ArrayList<>());
            }
            
            // Connect every node to every other node
            for (int i = 0; i < numNodes; i++) {
                for (int j = 0; j < numNodes; j++) {
                    if (i != j) {
                        graph.get(i).add(new DijkstraUtil.Edge(j, Math.abs(i - j)));
                    }
                }
            }

            DijkstraUtil.DijkstraResult result = DijkstraUtil.dijkstra(0, graph, numNodes);

            // In a fully connected graph, direct edge is often shortest
            for (int i = 1; i < numNodes; i++) {
                assertTrue(result.distances[i] <= i, 
                    "Distance should be at most " + i + " for node " + i);
            }
        }
    }
}
