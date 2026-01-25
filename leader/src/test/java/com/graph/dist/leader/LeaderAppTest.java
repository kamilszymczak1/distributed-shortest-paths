package com.graph.dist.leader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LeaderApp Tests")
class LeaderAppTest {

    @Nested
    @DisplayName("combinePaths Tests")
    class CombinePathsTests {

        @Test
        @DisplayName("Should combine single segment path")
        void shouldCombineSingleSegmentPath() {
            ArrayList<ArrayList<Integer>> segments = new ArrayList<>();
            segments.add(new ArrayList<>(Arrays.asList(1, 2, 3)));

            ArrayList<Integer> result = LeaderApp.combinePaths(segments);

            assertEquals(Arrays.asList(1, 2, 3), result);
        }

        @Test
        @DisplayName("Should combine two connected segments")
        void shouldCombineTwoConnectedSegments() {
            ArrayList<ArrayList<Integer>> segments = new ArrayList<>();
            // Segments are collected in reverse order, so we add them that way
            segments.add(new ArrayList<>(Arrays.asList(5, 6, 7)));  // Last segment
            segments.add(new ArrayList<>(Arrays.asList(1, 2, 5)));  // First segment

            ArrayList<Integer> result = LeaderApp.combinePaths(segments);

            // After reversal and combination: [1, 2, 5, 6, 7]
            assertEquals(Arrays.asList(1, 2, 5, 6, 7), result);
        }

        @Test
        @DisplayName("Should combine three connected segments")
        void shouldCombineThreeConnectedSegments() {
            ArrayList<ArrayList<Integer>> segments = new ArrayList<>();
            // Added in reverse order (as collected during path tracing)
            segments.add(new ArrayList<>(Arrays.asList(8, 9, 10)));
            segments.add(new ArrayList<>(Arrays.asList(5, 6, 8)));
            segments.add(new ArrayList<>(Arrays.asList(1, 2, 5)));

            ArrayList<Integer> result = LeaderApp.combinePaths(segments);

            assertEquals(Arrays.asList(1, 2, 5, 6, 8, 9, 10), result);
        }

        @Test
        @DisplayName("Should avoid duplicating connecting nodes")
        void shouldAvoidDuplicatingConnectingNodes() {
            ArrayList<ArrayList<Integer>> segments = new ArrayList<>();
            // Segments where the last node of one equals first node of next
            segments.add(new ArrayList<>(Arrays.asList(5, 6)));
            segments.add(new ArrayList<>(Arrays.asList(3, 4, 5)));
            segments.add(new ArrayList<>(Arrays.asList(1, 2, 3)));

            ArrayList<Integer> result = LeaderApp.combinePaths(segments);

            // Should be [1, 2, 3, 4, 5, 6] without duplicating 3 and 5
            assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), result);
        }

        @Test
        @DisplayName("Should handle empty segments list")
        void shouldHandleEmptySegmentsList() {
            ArrayList<ArrayList<Integer>> segments = new ArrayList<>();

            ArrayList<Integer> result = LeaderApp.combinePaths(segments);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle single node segments")
        void shouldHandleSingleNodeSegments() {
            ArrayList<ArrayList<Integer>> segments = new ArrayList<>();
            segments.add(new ArrayList<>(Arrays.asList(3)));
            segments.add(new ArrayList<>(Arrays.asList(2, 3)));
            segments.add(new ArrayList<>(Arrays.asList(1, 2)));

            ArrayList<Integer> result = LeaderApp.combinePaths(segments);

            assertEquals(Arrays.asList(1, 2, 3), result);
        }

        @Test
        @DisplayName("Should handle non-overlapping segments")
        void shouldHandleNonOverlappingSegments() {
            ArrayList<ArrayList<Integer>> segments = new ArrayList<>();
            // Segments that don't connect (edge case - shouldn't normally happen)
            segments.add(new ArrayList<>(Arrays.asList(6, 7)));
            segments.add(new ArrayList<>(Arrays.asList(1, 2, 3)));

            ArrayList<Integer> result = LeaderApp.combinePaths(segments);

            // Should concatenate all segments
            assertEquals(Arrays.asList(1, 2, 3, 6, 7), result);
        }
    }

    @Nested
    @DisplayName("ShortestPathResult Tests")
    class ShortestPathResultTests {

        @Test
        @DisplayName("Should create result with distance and path")
        void shouldCreateResultWithDistanceAndPath() {
            List<Integer> path = Arrays.asList(1, 2, 3, 4);
            LeaderApp.ShortestPathResult result = new LeaderApp.ShortestPathResult(42, path);

            assertEquals(42, result.distance);
            assertEquals(path, result.path);
        }

        @Test
        @DisplayName("Should handle zero distance")
        void shouldHandleZeroDistance() {
            List<Integer> path = Arrays.asList(1);
            LeaderApp.ShortestPathResult result = new LeaderApp.ShortestPathResult(0, path);

            assertEquals(0, result.distance);
        }

        @Test
        @DisplayName("Should handle empty path")
        void shouldHandleEmptyPath() {
            List<Integer> path = new ArrayList<>();
            LeaderApp.ShortestPathResult result = new LeaderApp.ShortestPathResult(Integer.MAX_VALUE, path);

            assertEquals(Integer.MAX_VALUE, result.distance);
            assertTrue(result.path.isEmpty());
        }

        @Test
        @DisplayName("Should handle max integer distance")
        void shouldHandleMaxIntegerDistance() {
            List<Integer> path = Arrays.asList(1, 2);
            LeaderApp.ShortestPathResult result = new LeaderApp.ShortestPathResult(Integer.MAX_VALUE, path);

            assertEquals(Integer.MAX_VALUE, result.distance);
        }
    }

    @Nested
    @DisplayName("getNumWorkers Tests")
    class GetNumWorkersTests {

        @Test
        @DisplayName("Should return default value when env var not set")
        void shouldReturnDefaultValueWhenEnvVarNotSet() {
            // This test will use the actual environment
            // Default is 1 if NUM_WORKERS is not set
            int numWorkers = LeaderApp.getNumWorkers();
            assertTrue(numWorkers >= 1);
        }
    }
}
