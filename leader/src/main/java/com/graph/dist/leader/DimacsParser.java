package com.graph.dist.leader;

import com.graph.dist.utils.Point;
import java.io.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;



public class DimacsParser {
    public static class Edge {
        public int from;
        public int to;
        public int weight;

        public Edge(int from, int to, int weight) {
            this.from = from;
            this.to = to;
            this.weight = weight;
        }
    }
    /**
     * Iterator that streams edges from a .gr.gz file without loading all into memory.
     */
    public static class EdgeIterator implements Iterator<Edge>, Closeable {
        private final BufferedReader reader;
        private Edge nextEdge;
        private boolean closed = false;

        public EdgeIterator(String edgePathName) throws IOException {
            this.reader = new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(new FileInputStream(edgePathName))));
            advance();
        }

        private void advance() {
            if (closed) {
                nextEdge = null;
                return;
            }
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("a ")) {
                        String[] p = line.split("\\s+");
                        int from = Integer.parseInt(p[1]);
                        int to = Integer.parseInt(p[2]);
                        int weight = Integer.parseInt(p[3]);
                        nextEdge = new Edge(from, to, weight);
                        return;
                    }
                }
                nextEdge = null;
                close();
            } catch (IOException e) {
                nextEdge = null;
                try { close(); } catch (IOException ignored) {}
            }
        }

        @Override
        public boolean hasNext() {
            return nextEdge != null;
        }

        @Override
        public Edge next() {
            if (nextEdge == null) {
                throw new NoSuchElementException();
            }
            Edge current = nextEdge;
            advance();
            return current;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                reader.close();
            }
        }
    }

    /**
     * Returns a Stream of edges that reads lazily from the file.
     * The stream should be used in a try-with-resources to ensure the file is closed.
     */
    public static Stream<Edge> streamEdges(String edgePathName) throws IOException {
        EdgeIterator iterator = new EdgeIterator(edgePathName);
        Spliterator<Edge> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false).onClose(() -> {
            try { iterator.close(); } catch (IOException ignored) {}
        });
    }

    public static class Vertex {
        public int id;
        public Point point;

        public Vertex(int id, Point point) {
            this.id = id;
            this.point = point;
        }
    }

    /**
     * Iterator that streams vertices from a .co.gz file without loading all into memory.
     */
    public static class VertexIterator implements Iterator<Vertex>, Closeable {
        private final BufferedReader reader;
        private Vertex nextVertex;
        private boolean closed = false;

        public VertexIterator(String vertexPathName) throws IOException {
            this.reader = new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(new FileInputStream(vertexPathName))));
            advance();
        }

        private void advance() {
            if (closed) {
                nextVertex = null;
                return;
            }
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("v ")) {
                        String[] p = line.split("\\s+");
                        int id = Integer.parseInt(p[1]);
                        int x = Integer.parseInt(p[2]);
                        int y = Integer.parseInt(p[3]);
                        nextVertex = new Vertex(id, new Point(x, y));
                        return;
                    }
                }
                nextVertex = null;
                close();
            } catch (IOException e) {
                nextVertex = null;
                try { close(); } catch (IOException ignored) {}
            }
        }

        @Override
        public boolean hasNext() {
            return nextVertex != null;
        }

        @Override
        public Vertex next() {
            if (nextVertex == null) {
                throw new NoSuchElementException();
            }
            Vertex current = nextVertex;
            advance();
            return current;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                reader.close();
            }
        }
    }

    /**
     * Returns a Stream of vertices that reads lazily from the file.
     * The stream should be used in a try-with-resources to ensure the file is closed.
     */
    public static Stream<Vertex> streamVertices(String vertexPathName) throws IOException {
        VertexIterator iterator = new VertexIterator(vertexPathName);
        Spliterator<Vertex> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false).onClose(() -> {
            try { iterator.close(); } catch (IOException ignored) {}
        });
    }
}