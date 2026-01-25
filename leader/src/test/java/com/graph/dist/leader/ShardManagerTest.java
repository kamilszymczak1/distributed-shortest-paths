package com.graph.dist.leader;

import com.graph.dist.proto.ShardData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ShardManager Tests")
class ShardManagerTest {

    @TempDir
    Path tempDir;

    private Path vertexFile;
    private Path edgeFile;

    @BeforeEach
    void setUp() throws IOException {
        // Create a simple test graph with 4 nodes in a 2x2 grid
        // Nodes: (0,0), (10,0), (0,10), (10,10)
        vertexFile = tempDir.resolve("test.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(vertexFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("v 1 0 0\n");
            writer.write("v 2 10 0\n");
            writer.write("v 3 0 10\n");
            writer.write("v 4 10 10\n");
        }

        // Edges forming a square
        edgeFile = tempDir.resolve("test.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(edgeFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("a 1 2 5\n");
            writer.write("a 2 1 5\n");
            writer.write("a 1 3 5\n");
            writer.write("a 3 1 5\n");
            writer.write("a 2 4 5\n");
            writer.write("a 4 2 5\n");
            writer.write("a 3 4 5\n");
            writer.write("a 4 3 5\n");
        }
    }

    @Test
    @DisplayName("Should create single shard for single worker")
    void shouldCreateSingleShardForSingleWorker() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            1
        );

        ShardData shard = manager.getShardData(0);

        assertNotNull(shard);
        assertEquals(0, shard.getShardId());
        assertEquals(4, shard.getNodesCount());
    }

    @Test
    @DisplayName("Should create two shards for two workers")
    void shouldCreateTwoShardsForTwoWorkers() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            2
        );

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        assertNotNull(shard0);
        assertNotNull(shard1);
        
        // Total nodes should be 4
        assertEquals(4, shard0.getNodesCount() + shard1.getNodesCount());
        
        // Each shard should have some nodes
        assertTrue(shard0.getNodesCount() > 0);
        assertTrue(shard1.getNodesCount() > 0);
    }

    @Test
    @DisplayName("Should return null for invalid shard ID")
    void shouldReturnNullForInvalidShardId() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            2
        );

        ShardData invalidShard = manager.getShardData(99);

        assertNull(invalidShard);
    }

    @Test
    @DisplayName("Should return null for negative shard ID")
    void shouldReturnNullForNegativeShardId() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            2
        );

        ShardData invalidShard = manager.getShardData(-1);

        assertNull(invalidShard);
    }

    @Test
    @DisplayName("Should set correct shard ID in shard data")
    void shouldSetCorrectShardIdInShardData() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            2
        );

        assertEquals(0, manager.getShardData(0).getShardId());
        assertEquals(1, manager.getShardData(1).getShardId());
    }

    @Test
    @DisplayName("Should include edges in shard data")
    void shouldIncludeEdgesInShardData() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            1
        );

        ShardData shard = manager.getShardData(0);

        // Should have 8 edges (all edges in single shard)
        assertEquals(8, shard.getEdgesCount());
    }

    @Test
    @DisplayName("Should handle larger graph with more workers")
    void shouldHandleLargerGraphWithMoreWorkers() throws IOException {
        // Create a larger graph with 16 nodes
        Path largeVertexFile = tempDir.resolve("large.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(largeVertexFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            for (int i = 1; i <= 16; i++) {
                int x = (i - 1) % 4;
                int y = (i - 1) / 4;
                writer.write("v " + i + " " + x + " " + y + "\n");
            }
        }

        Path largeEdgeFile = tempDir.resolve("large.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(largeEdgeFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            // Create a grid of edges
            for (int i = 1; i <= 16; i++) {
                if (i % 4 != 0) {
                    writer.write("a " + i + " " + (i + 1) + " 1\n");
                    writer.write("a " + (i + 1) + " " + i + " 1\n");
                }
                if (i <= 12) {
                    writer.write("a " + i + " " + (i + 4) + " 1\n");
                    writer.write("a " + (i + 4) + " " + i + " 1\n");
                }
            }
        }

        ShardManager manager = new ShardManager(
            largeVertexFile.toString(), 
            largeEdgeFile.toString(), 
            4
        );

        int totalNodes = 0;
        for (int i = 0; i < 4; i++) {
            ShardData shard = manager.getShardData(i);
            assertNotNull(shard, "Shard " + i + " should not be null");
            totalNodes += shard.getNodesCount();
        }

        assertEquals(16, totalNodes);
    }

    @Test
    @DisplayName("Should mark cross-shard edges with correct target shard")
    void shouldMarkCrossShardEdgesCorrectly() {
        // With 2 workers, nodes will be split spatially
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            2
        );

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        // Check that edges have valid toShard values
        for (var edge : shard0.getEdgesList()) {
            assertTrue(edge.getToShard() >= 0 && edge.getToShard() < 2,
                "Edge toShard should be 0 or 1");
        }

        for (var edge : shard1.getEdgesList()) {
            assertTrue(edge.getToShard() >= 0 && edge.getToShard() < 2,
                "Edge toShard should be 0 or 1");
        }
    }

    @Test
    @DisplayName("Should include node coordinates in shard data")
    void shouldIncludeNodeCoordinatesInShardData() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            1
        );

        ShardData shard = manager.getShardData(0);

        // Check that all nodes have valid coordinates
        for (var node : shard.getNodesList()) {
            assertTrue(node.getId() >= 1 && node.getId() <= 4);
            assertTrue(node.getX() >= 0 && node.getX() <= 10);
            assertTrue(node.getY() >= 0 && node.getY() <= 10);
        }
    }

    @Test
    @DisplayName("Should throw exception for non-existent coordinate file")
    void shouldThrowExceptionForNonExistentCoordinateFile() {
        assertThrows(RuntimeException.class, () -> {
            new ShardManager("/non/existent/file.co.gz", edgeFile.toString(), 1);
        });
    }

    @Test
    @DisplayName("Each node should appear in exactly one shard")
    void eachNodeShouldAppearInExactlyOneShard() {
        ShardManager manager = new ShardManager(
            vertexFile.toString(), 
            edgeFile.toString(), 
            2
        );

        java.util.Set<Integer> allNodeIds = new java.util.HashSet<>();
        
        for (int shardId = 0; shardId < 2; shardId++) {
            ShardData shard = manager.getShardData(shardId);
            for (var node : shard.getNodesList()) {
                boolean isNew = allNodeIds.add(node.getId());
                assertTrue(isNew, "Node " + node.getId() + " appears in multiple shards");
            }
        }

        assertEquals(4, allNodeIds.size(), "All 4 nodes should be distributed");
    }

    // ==================== Sharding Algorithm Tests ====================

    @Test
    @DisplayName("Should split vertically when rectangle is wider than tall")
    void shouldSplitVerticallyWhenWider() throws IOException {
        // Create a wide rectangle: nodes spread horizontally
        // Points: (0,0), (10,0), (20,0), (30,0) - width=30, height=0
        Path wideFile = tempDir.resolve("wide.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(wideFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("v 1 0 0\n");
            writer.write("v 2 10 0\n");
            writer.write("v 3 20 0\n");
            writer.write("v 4 30 0\n");
        }

        Path emptyEdges = tempDir.resolve("empty.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            // No edges
        }

        ShardManager manager = new ShardManager(wideFile.toString(), emptyEdges.toString(), 2);

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        // After vertical split, nodes should be divided by x-coordinate
        // Left shard should have low x values, right shard should have high x values
        int maxXInShard0 = shard0.getNodesList().stream().mapToInt(n -> n.getX()).max().orElse(0);
        int minXInShard1 = shard1.getNodesList().stream().mapToInt(n -> n.getX()).min().orElse(0);

        assertTrue(maxXInShard0 < minXInShard1 || maxXInShard0 == minXInShard1,
            "Vertical split should separate nodes by x-coordinate. " +
            "Shard0 max X: " + maxXInShard0 + ", Shard1 min X: " + minXInShard1);
    }

    @Test
    @DisplayName("Should split horizontally when rectangle is taller than wide")
    void shouldSplitHorizontallyWhenTaller() throws IOException {
        // Create a tall rectangle: nodes spread vertically
        // Points: (0,0), (0,10), (0,20), (0,30) - width=0, height=30
        Path tallFile = tempDir.resolve("tall.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(tallFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("v 1 0 0\n");
            writer.write("v 2 0 10\n");
            writer.write("v 3 0 20\n");
            writer.write("v 4 0 30\n");
        }

        Path emptyEdges = tempDir.resolve("empty2.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            // No edges
        }

        ShardManager manager = new ShardManager(tallFile.toString(), emptyEdges.toString(), 2);

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        // After horizontal split, nodes should be divided by y-coordinate
        int maxYInShard0 = shard0.getNodesList().stream().mapToInt(n -> n.getY()).max().orElse(0);
        int minYInShard1 = shard1.getNodesList().stream().mapToInt(n -> n.getY()).min().orElse(0);

        assertTrue(maxYInShard0 < minYInShard1 || maxYInShard0 == minYInShard1,
            "Horizontal split should separate nodes by y-coordinate. " +
            "Shard0 max Y: " + maxYInShard0 + ", Shard1 min Y: " + minYInShard1);
    }

    @Test
    @DisplayName("Should split largest shard first when creating multiple shards")
    void shouldSplitLargestShardFirst() throws IOException {
        // Create 8 nodes in a square grid
        Path gridFile = tempDir.resolve("grid.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(gridFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            // 2x4 grid - wider than tall
            writer.write("v 1 0 0\n");
            writer.write("v 2 10 0\n");
            writer.write("v 3 20 0\n");
            writer.write("v 4 30 0\n");
            writer.write("v 5 0 10\n");
            writer.write("v 6 10 10\n");
            writer.write("v 7 20 10\n");
            writer.write("v 8 30 10\n");
        }

        Path emptyEdges = tempDir.resolve("empty3.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            // No edges
        }

        ShardManager manager = new ShardManager(gridFile.toString(), emptyEdges.toString(), 4);

        // With 4 shards from 8 nodes, each shard should have approximately 2 nodes
        int totalNodes = 0;
        int minNodesPerShard = Integer.MAX_VALUE;
        int maxNodesPerShard = 0;

        for (int i = 0; i < 4; i++) {
            ShardData shard = manager.getShardData(i);
            int count = shard.getNodesCount();
            totalNodes += count;
            minNodesPerShard = Math.min(minNodesPerShard, count);
            maxNodesPerShard = Math.max(maxNodesPerShard, count);
        }

        assertEquals(8, totalNodes, "All 8 nodes should be distributed");
        // Because we always split the largest, shards should be reasonably balanced
        assertTrue(maxNodesPerShard - minNodesPerShard <= 2,
            "Shards should be reasonably balanced. Min: " + minNodesPerShard + ", Max: " + maxNodesPerShard);
    }

    @Test
    @DisplayName("Should correctly partition nodes by x-coordinate in vertical split")
    void shouldPartitionNodesByXCoordinate() throws IOException {
        // Create nodes with distinct x values
        Path file = tempDir.resolve("xsplit.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(file));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("v 1 0 5\n");   // x=0
            writer.write("v 2 5 5\n");   // x=5
            writer.write("v 3 10 5\n");  // x=10
            writer.write("v 4 15 5\n");  // x=15
        }

        Path emptyEdges = tempDir.resolve("empty4.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
        }

        ShardManager manager = new ShardManager(file.toString(), emptyEdges.toString(), 2);

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        // Collect x-coordinates from each shard
        java.util.Set<Integer> xInShard0 = new java.util.HashSet<>();
        java.util.Set<Integer> xInShard1 = new java.util.HashSet<>();

        for (var node : shard0.getNodesList()) {
            xInShard0.add(node.getX());
        }
        for (var node : shard1.getNodesList()) {
            xInShard1.add(node.getX());
        }

        // Shards should not overlap in x-coordinate (one has lower x, other has higher)
        int maxX0 = xInShard0.stream().mapToInt(x -> x).max().orElse(0);
        int minX1 = xInShard1.stream().mapToInt(x -> x).min().orElse(Integer.MAX_VALUE);

        assertTrue(maxX0 <= minX1, 
            "After vertical split, left shard's max x (" + maxX0 + 
            ") should be <= right shard's min x (" + minX1 + ")");
    }

    @Test
    @DisplayName("Should correctly partition nodes by y-coordinate in horizontal split")
    void shouldPartitionNodesByYCoordinate() throws IOException {
        // Create nodes with distinct y values (tall rectangle)
        Path file = tempDir.resolve("ysplit.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(file));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("v 1 5 0\n");   // y=0
            writer.write("v 2 5 5\n");   // y=5
            writer.write("v 3 5 10\n");  // y=10
            writer.write("v 4 5 15\n");  // y=15
        }

        Path emptyEdges = tempDir.resolve("empty5.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
        }

        ShardManager manager = new ShardManager(file.toString(), emptyEdges.toString(), 2);

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        // Collect y-coordinates from each shard
        java.util.Set<Integer> yInShard0 = new java.util.HashSet<>();
        java.util.Set<Integer> yInShard1 = new java.util.HashSet<>();

        for (var node : shard0.getNodesList()) {
            yInShard0.add(node.getY());
        }
        for (var node : shard1.getNodesList()) {
            yInShard1.add(node.getY());
        }

        // Shards should not overlap in y-coordinate
        int maxY0 = yInShard0.stream().mapToInt(y -> y).max().orElse(0);
        int minY1 = yInShard1.stream().mapToInt(y -> y).min().orElse(Integer.MAX_VALUE);

        assertTrue(maxY0 <= minY1, 
            "After horizontal split, bottom shard's max y (" + maxY0 + 
            ") should be <= top shard's min y (" + minY1 + ")");
    }

    @Test
    @DisplayName("Should prefer vertical split when width equals height")
    void shouldPreferVerticalSplitWhenSquare() throws IOException {
        // Create a perfect square of nodes
        Path file = tempDir.resolve("square.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(file));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("v 1 0 0\n");
            writer.write("v 2 10 0\n");
            writer.write("v 3 0 10\n");
            writer.write("v 4 10 10\n");
        }

        Path emptyEdges = tempDir.resolve("empty6.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
        }

        ShardManager manager = new ShardManager(file.toString(), emptyEdges.toString(), 2);

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        // With >= condition for vertical split, when square, it should split vertically (by x)
        // One shard should have x=0 nodes, other should have x=10 nodes
        java.util.Set<Integer> xInShard0 = new java.util.HashSet<>();
        java.util.Set<Integer> xInShard1 = new java.util.HashSet<>();

        for (var node : shard0.getNodesList()) {
            xInShard0.add(node.getX());
        }
        for (var node : shard1.getNodesList()) {
            xInShard1.add(node.getX());
        }

        // After vertical split, x values should be separated
        boolean verticalSplit = !xInShard0.equals(xInShard1) && 
            (xInShard0.stream().allMatch(x -> x <= 5) || xInShard1.stream().allMatch(x -> x <= 5));

        assertTrue(verticalSplit, 
            "Square should be split vertically. Shard0 x: " + xInShard0 + ", Shard1 x: " + xInShard1);
    }

    @Test
    @DisplayName("Should handle nodes with same coordinates gracefully")
    void shouldHandleNodesWithSameCoordinates() throws IOException {
        // Multiple nodes at same location
        Path file = tempDir.resolve("same.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(file));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("v 1 0 0\n");
            writer.write("v 2 0 0\n");  // Same as node 1
            writer.write("v 3 10 10\n");
            writer.write("v 4 10 10\n"); // Same as node 3
        }

        Path emptyEdges = tempDir.resolve("empty7.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
        }

        ShardManager manager = new ShardManager(file.toString(), emptyEdges.toString(), 2);

        ShardData shard0 = manager.getShardData(0);
        ShardData shard1 = manager.getShardData(1);

        // Should still partition correctly
        assertEquals(4, shard0.getNodesCount() + shard1.getNodesCount());
    }

    @Test
    @DisplayName("Should create spatially coherent shards for grid layout")
    void shouldCreateSpatiallyCoherentShards() throws IOException {
        // Create a 4x4 grid of nodes
        Path file = tempDir.resolve("grid4x4.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(file));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            int id = 1;
            for (int y = 0; y < 4; y++) {
                for (int x = 0; x < 4; x++) {
                    writer.write("v " + id++ + " " + (x * 10) + " " + (y * 10) + "\n");
                }
            }
        }

        Path emptyEdges = tempDir.resolve("empty8.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
        }

        ShardManager manager = new ShardManager(file.toString(), emptyEdges.toString(), 4);

        // Each shard should contain spatially close nodes
        for (int i = 0; i < 4; i++) {
            ShardData shard = manager.getShardData(i);
            
            int minX = shard.getNodesList().stream().mapToInt(n -> n.getX()).min().orElse(0);
            int maxX = shard.getNodesList().stream().mapToInt(n -> n.getX()).max().orElse(0);
            int minY = shard.getNodesList().stream().mapToInt(n -> n.getY()).min().orElse(0);
            int maxY = shard.getNodesList().stream().mapToInt(n -> n.getY()).max().orElse(0);

            // The bounding box of each shard should not span the entire grid
            // (indicating spatial locality)
            int width = maxX - minX;
            int height = maxY - minY;

            assertTrue(width <= 20 || height <= 20,
                "Shard " + i + " should be spatially coherent. " +
                "Bounding box: (" + minX + "-" + maxX + ", " + minY + "-" + maxY + ")");
        }
    }

    @Test
    @DisplayName("Should split into approximately equal sized shards")
    void shouldSplitIntoApproximatelyEqualSizedShards() throws IOException {
        // Create 100 nodes in a grid
        Path file = tempDir.resolve("large_grid.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(file));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            int id = 1;
            for (int y = 0; y < 10; y++) {
                for (int x = 0; x < 10; x++) {
                    writer.write("v " + id++ + " " + x + " " + y + "\n");
                }
            }
        }

        Path emptyEdges = tempDir.resolve("empty9.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyEdges));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
        }

        ShardManager manager = new ShardManager(file.toString(), emptyEdges.toString(), 4);

        int[] shardSizes = new int[4];
        for (int i = 0; i < 4; i++) {
            shardSizes[i] = manager.getShardData(i).getNodesCount();
        }

        // Expected: ~25 nodes per shard
        int expectedPerShard = 100 / 4;
        for (int i = 0; i < 4; i++) {
            assertTrue(shardSizes[i] >= expectedPerShard - 5 && shardSizes[i] <= expectedPerShard + 5,
                "Shard " + i + " has " + shardSizes[i] + " nodes, expected ~" + expectedPerShard);
        }
    }
}
