package com.graph.dist.leader;

import com.graph.dist.utils.Point;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents a rectangular shard defined by coordinate bounds.
 * Contains the list of node IDs that fall within this shard's bounds.
 */
public class Shard {
    public final int xMin;
    public final int xMax;
    public final int yMin;
    public final int yMax;
    public final List<Integer> nodeIds;

    public Shard(int xMin, int xMax, int yMin, int yMax, List<Integer> nodeIds) {
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
        this.nodeIds = nodeIds;
    }

    public int getNodeCount() {
        return nodeIds.size();
    }

    public int getWidth() {
        return xMax - xMin;
    }

    public int getHeight() {
        return yMax - yMin;
    }

    /**
     * Returns true if the shard should be split vertically (along x-axis),
     * i.e., when width >= height.
     */
    public boolean shouldSplitVertically() {
        return getWidth() >= getHeight();
    }

    /**
     * Creates the initial shard containing all nodes.
     */
    public static Shard createInitialShard(Map<Integer, Point> coords) {
        if (coords.isEmpty()) {
            return new Shard(0, 0, 0, 0, new ArrayList<>());
        }

        int xMin = Integer.MAX_VALUE;
        int xMax = Integer.MIN_VALUE;
        int yMin = Integer.MAX_VALUE;
        int yMax = Integer.MIN_VALUE;

        List<Integer> allNodeIds = new ArrayList<>(coords.size());

        for (Map.Entry<Integer, Point> entry : coords.entrySet()) {
            int nodeId = entry.getKey();
            Point pt = entry.getValue();
            allNodeIds.add(nodeId);
            xMin = Math.min(xMin, pt.x);
            xMax = Math.max(xMax, pt.x);
            yMin = Math.min(yMin, pt.y);
            yMax = Math.max(yMax, pt.y);
        }

        return new Shard(xMin, xMax, yMin, yMax, allNodeIds);
    }

    /**
     * Splits this shard into two shards using binary search to find the optimal
     * split coordinate that balances node counts as equally as possible.
     * 
     * @param coords The coordinate map for looking up node positions
     * @return An array of two new shards
     */
    public Shard[] split(Map<Integer, Point> coords) {
        if (shouldSplitVertically()) {
            return splitVertically(coords);
        } else {
            return splitHorizontally(coords);
        }
    }

    /**
     * Splits vertically (creates left and right shards).
     * Uses median to split nodes as evenly as possible.
     */
    private Shard[] splitVertically(Map<Integer, Point> coords) {
        // Sort nodes by x coordinate
        List<Integer> sortedNodes = new ArrayList<>(nodeIds);
        sortedNodes.sort((a, b) -> Integer.compare(coords.get(a).x, coords.get(b).x));

        // Split at median index (middle of sorted list)
        int medianIndex = sortedNodes.size() / 2;
        
        // Ensure we don't create empty shards
        if (medianIndex <= 0) medianIndex = 1;
        if (medianIndex >= sortedNodes.size()) medianIndex = sortedNodes.size() - 1;
        
        List<Integer> leftNodes = new ArrayList<>(sortedNodes.subList(0, medianIndex));
        List<Integer> rightNodes = new ArrayList<>(sortedNodes.subList(medianIndex, sortedNodes.size()));

        // Find the actual coordinate bounds for each new shard
        int leftXMax = xMin;
        int rightXMin = xMax;
        
        if (!leftNodes.isEmpty()) {
            leftXMax = coords.get(leftNodes.get(leftNodes.size() - 1)).x;
        }
        if (!rightNodes.isEmpty()) {
            rightXMin = coords.get(rightNodes.get(0)).x;
        }

        // Use midpoint between the last left node and first right node as boundary
        int splitX = (leftXMax + rightXMin) / 2;
        
        Shard leftShard = new Shard(xMin, splitX, yMin, yMax, leftNodes);
        Shard rightShard = new Shard(splitX + 1, xMax, yMin, yMax, rightNodes);

        return new Shard[] { leftShard, rightShard };
    }

    /**
     * Splits horizontally (creates top and bottom shards).
     * Uses median to split nodes as evenly as possible.
     */
    private Shard[] splitHorizontally(Map<Integer, Point> coords) {
        // Sort nodes by y coordinate
        List<Integer> sortedNodes = new ArrayList<>(nodeIds);
        sortedNodes.sort((a, b) -> Integer.compare(coords.get(a).y, coords.get(b).y));

        // Split at median index (middle of sorted list)
        int medianIndex = sortedNodes.size() / 2;
        
        // Ensure we don't create empty shards
        if (medianIndex <= 0) medianIndex = 1;
        if (medianIndex >= sortedNodes.size()) medianIndex = sortedNodes.size() - 1;
        
        List<Integer> bottomNodes = new ArrayList<>(sortedNodes.subList(0, medianIndex));
        List<Integer> topNodes = new ArrayList<>(sortedNodes.subList(medianIndex, sortedNodes.size()));

        // Find the actual coordinate bounds for each new shard
        int bottomYMax = yMin;
        int topYMin = yMax;
        
        if (!bottomNodes.isEmpty()) {
            bottomYMax = coords.get(bottomNodes.get(bottomNodes.size() - 1)).y;
        }
        if (!topNodes.isEmpty()) {
            topYMin = coords.get(topNodes.get(0)).y;
        }

        // Use midpoint between the last bottom node and first top node as boundary
        int splitY = (bottomYMax + topYMin) / 2;
        
        Shard bottomShard = new Shard(xMin, xMax, yMin, splitY, bottomNodes);
        Shard topShard = new Shard(xMin, xMax, splitY + 1, yMax, topNodes);

        return new Shard[] { bottomShard, topShard };
    }

    @Override
    public String toString() {
        return String.format("Shard[x:(%d,%d), y:(%d,%d), nodes:%d]", 
                             xMin, xMax, yMin, yMax, getNodeCount());
    }
}
