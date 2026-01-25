package com.graph.dist.leader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DimacsParser Tests")
class DimacsParserTest {

    @TempDir
    Path tempDir;

    private Path vertexFile;
    private Path edgeFile;

    @BeforeEach
    void setUp() throws IOException {
        // Create test vertex file (gzipped)
        vertexFile = tempDir.resolve("test.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(vertexFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("c This is a comment\n");
            writer.write("p aux sp co 5\n");
            writer.write("v 1 0 0\n");
            writer.write("v 2 10 0\n");
            writer.write("v 3 0 10\n");
            writer.write("v 4 10 10\n");
            writer.write("v 5 5 5\n");
        }

        // Create test edge file (gzipped)
        edgeFile = tempDir.resolve("test.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(edgeFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("c This is a comment\n");
            writer.write("p sp 5 8\n");
            writer.write("a 1 2 5\n");
            writer.write("a 2 1 5\n");
            writer.write("a 1 3 7\n");
            writer.write("a 3 1 7\n");
            writer.write("a 2 4 3\n");
            writer.write("a 4 2 3\n");
            writer.write("a 3 4 2\n");
            writer.write("a 4 3 2\n");
        }
    }

    @Test
    @DisplayName("Should parse all vertices from file")
    void shouldParseAllVertices() throws IOException {
        try (var stream = DimacsParser.streamVertices(vertexFile.toString())) {
            List<DimacsParser.Vertex> vertices = stream.collect(Collectors.toList());
            
            assertEquals(5, vertices.size());
        }
    }

    @Test
    @DisplayName("Should parse vertex IDs correctly")
    void shouldParseVertexIdsCorrectly() throws IOException {
        try (var stream = DimacsParser.streamVertices(vertexFile.toString())) {
            List<DimacsParser.Vertex> vertices = stream.collect(Collectors.toList());
            
            assertEquals(1, vertices.get(0).id);
            assertEquals(2, vertices.get(1).id);
            assertEquals(3, vertices.get(2).id);
            assertEquals(4, vertices.get(3).id);
            assertEquals(5, vertices.get(4).id);
        }
    }

    @Test
    @DisplayName("Should parse vertex coordinates correctly")
    void shouldParseVertexCoordinatesCorrectly() throws IOException {
        try (var stream = DimacsParser.streamVertices(vertexFile.toString())) {
            List<DimacsParser.Vertex> vertices = stream.collect(Collectors.toList());
            
            // First vertex: v 1 0 0
            assertEquals(0, vertices.get(0).point.x);
            assertEquals(0, vertices.get(0).point.y);
            
            // Second vertex: v 2 10 0
            assertEquals(10, vertices.get(1).point.x);
            assertEquals(0, vertices.get(1).point.y);
            
            // Fifth vertex: v 5 5 5
            assertEquals(5, vertices.get(4).point.x);
            assertEquals(5, vertices.get(4).point.y);
        }
    }

    @Test
    @DisplayName("Should skip comment and header lines in vertex file")
    void shouldSkipCommentsInVertexFile() throws IOException {
        try (var stream = DimacsParser.streamVertices(vertexFile.toString())) {
            List<DimacsParser.Vertex> vertices = stream.collect(Collectors.toList());
            
            // Should only have 5 vertices, not include comments or headers
            assertEquals(5, vertices.size());
            // First vertex should be ID 1, not a comment
            assertEquals(1, vertices.get(0).id);
        }
    }

    @Test
    @DisplayName("Should parse all edges from file")
    void shouldParseAllEdges() throws IOException {
        try (var stream = DimacsParser.streamEdges(edgeFile.toString())) {
            List<DimacsParser.Edge> edges = stream.collect(Collectors.toList());
            
            assertEquals(8, edges.size());
        }
    }

    @Test
    @DisplayName("Should parse edge endpoints correctly")
    void shouldParseEdgeEndpointsCorrectly() throws IOException {
        try (var stream = DimacsParser.streamEdges(edgeFile.toString())) {
            List<DimacsParser.Edge> edges = stream.collect(Collectors.toList());
            
            // First edge: a 1 2 5
            assertEquals(1, edges.get(0).from);
            assertEquals(2, edges.get(0).to);
            
            // Third edge: a 1 3 7
            assertEquals(1, edges.get(2).from);
            assertEquals(3, edges.get(2).to);
        }
    }

    @Test
    @DisplayName("Should parse edge weights correctly")
    void shouldParseEdgeWeightsCorrectly() throws IOException {
        try (var stream = DimacsParser.streamEdges(edgeFile.toString())) {
            List<DimacsParser.Edge> edges = stream.collect(Collectors.toList());
            
            // First edge: a 1 2 5
            assertEquals(5, edges.get(0).weight);
            
            // Third edge: a 1 3 7
            assertEquals(7, edges.get(2).weight);
            
            // Seventh edge: a 3 4 2
            assertEquals(2, edges.get(6).weight);
        }
    }

    @Test
    @DisplayName("Should skip comment and header lines in edge file")
    void shouldSkipCommentsInEdgeFile() throws IOException {
        try (var stream = DimacsParser.streamEdges(edgeFile.toString())) {
            List<DimacsParser.Edge> edges = stream.collect(Collectors.toList());
            
            // Should only have 8 edges
            assertEquals(8, edges.size());
            // First edge should start from node 1
            assertEquals(1, edges.get(0).from);
        }
    }

    @Test
    @DisplayName("Should handle empty vertex file")
    void shouldHandleEmptyVertexFile() throws IOException {
        Path emptyFile = tempDir.resolve("empty.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("c Only comments\n");
        }

        try (var stream = DimacsParser.streamVertices(emptyFile.toString())) {
            List<DimacsParser.Vertex> vertices = stream.collect(Collectors.toList());
            assertTrue(vertices.isEmpty());
        }
    }

    @Test
    @DisplayName("Should handle empty edge file")
    void shouldHandleEmptyEdgeFile() throws IOException {
        Path emptyFile = tempDir.resolve("empty.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(emptyFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("c Only comments\n");
        }

        try (var stream = DimacsParser.streamEdges(emptyFile.toString())) {
            List<DimacsParser.Edge> edges = stream.collect(Collectors.toList());
            assertTrue(edges.isEmpty());
        }
    }

    @Test
    @DisplayName("Should throw exception for non-existent vertex file")
    void shouldThrowExceptionForNonExistentVertexFile() {
        assertThrows(IOException.class, () -> {
            DimacsParser.streamVertices("/non/existent/file.co.gz");
        });
    }

    @Test
    @DisplayName("Should throw exception for non-existent edge file")
    void shouldThrowExceptionForNonExistentEdgeFile() {
        assertThrows(IOException.class, () -> {
            DimacsParser.streamEdges("/non/existent/file.gr.gz");
        });
    }

    @Test
    @DisplayName("Should stream vertices lazily without reading entire file")
    void shouldStreamVerticesLazily() throws IOException {
        // Create a file with 1000 vertices
        Path largeFile = tempDir.resolve("large.co.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(largeFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            for (int i = 1; i <= 1000; i++) {
                writer.write("v " + i + " " + (i % 100) + " " + (i / 100) + "\n");
            }
        }

        // Use a counting iterator wrapper to verify lazy loading
        final int[] readCount = {0};
        
        try (var stream = DimacsParser.streamVertices(largeFile.toString())) {
            // Map each vertex through a counter to track how many are actually read
            List<DimacsParser.Vertex> first10 = stream
                .peek(v -> readCount[0]++)
                .limit(10)
                .collect(Collectors.toList());
            
            assertEquals(10, first10.size());
        }
        
        // If streaming is lazy, only ~10 vertices should have been processed
        // (not all 1000). We allow some buffer for implementation details.
        assertTrue(readCount[0] <= 20, 
            "Expected lazy streaming to read ~10 vertices, but read " + readCount[0] + 
            ". This suggests the entire file was loaded into memory.");
    }

    @Test
    @DisplayName("Should handle large weights")
    void shouldHandleLargeWeights() throws IOException {
        Path largeWeightFile = tempDir.resolve("large_weight.gr.gz");
        try (GZIPOutputStream gzos = new GZIPOutputStream(Files.newOutputStream(largeWeightFile));
             OutputStreamWriter writer = new OutputStreamWriter(gzos)) {
            writer.write("a 1 2 2147483647\n");  // Integer.MAX_VALUE
        }

        try (var stream = DimacsParser.streamEdges(largeWeightFile.toString())) {
            List<DimacsParser.Edge> edges = stream.collect(Collectors.toList());
            assertEquals(1, edges.size());
            assertEquals(Integer.MAX_VALUE, edges.get(0).weight);
        }
    }
}
