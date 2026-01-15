package com.graph.dist.leader;

import com.graph.dist.utils.Point;
import java.io.*;
import java.util.*;
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

    public static class GraphData {
        public Map<Integer, Point> coords = new HashMap<>();
        public List<Edge> edges = new ArrayList<>();
    }

    public static GraphData parse(String coPath, String grPath) throws IOException {
        GraphData data = new GraphData();

        int min_x = Integer.MAX_VALUE;
        int min_y = Integer.MAX_VALUE;

        // 1. Read Coordinates (.co.gz)
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(coPath))))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("v ")) {
                    String[] p = line.split("\\s+");
                    int id = Integer.parseInt(p[1]);
                    int x = Integer.parseInt(p[2]);
                    int y = Integer.parseInt(p[3]);
                    min_x = Math.min(min_x, x);
                    min_y = Math.min(min_y, y);
                    data.coords.put(id, new Point(x, y));
                }
            }
        }

        // Normalize coordinates to start from (0,0)
        for (Point pt : data.coords.values()) {
            pt.x -= min_x;
            pt.y -= min_y;
        }

        // 2. Read Edges (.gr.gz)
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(grPath))))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("a ")) {
                    String[] p = line.split("\\s+");
                    int from = Integer.parseInt(p[1]);
                    int to = Integer.parseInt(p[2]);
                    int weight = Integer.parseInt(p[3]);
                    data.edges.add(new Edge(from, to, weight));
                }
            }
        }
        return data;
    }
}