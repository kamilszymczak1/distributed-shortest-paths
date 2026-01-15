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
        List<Integer> nodes = new ArrayList<>(nodeIds);
        int medianIndex = nodes.size() / 2;
        
        // Ensure we don't create empty shards
        assert medianIndex > 0 && medianIndex < nodes.size();
        
        // Partition nodes around the median index using Median of Medians
        select(nodes, 0, nodes.size() - 1, medianIndex, (a, b) -> Integer.compare(coords.get(a).x, coords.get(b).x));
        
        List<Integer> leftNodes = new ArrayList<>(nodes.subList(0, medianIndex));
        List<Integer> rightNodes = new ArrayList<>(nodes.subList(medianIndex, nodes.size()));

        // Find the actual coordinate bounds for each new shard
        int leftXMax = xMin;
        int rightXMin = xMax;
        
        if (!leftNodes.isEmpty()) {
            leftXMax = getMaxCoordinate(leftNodes, coords, true);
        }
        if (!rightNodes.isEmpty()) {
            rightXMin = getMinCoordinate(rightNodes, coords, true);
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
        List<Integer> nodes = new ArrayList<>(nodeIds);
        int medianIndex = nodes.size() / 2;
        
        // Ensure we don't create empty shards
        assert medianIndex > 0 && medianIndex < nodes.size();
        
        // Partition nodes around the median index using Median of Medians
        select(nodes, 0, nodes.size() - 1, medianIndex, (a, b) -> Integer.compare(coords.get(a).y, coords.get(b).y));
        
        List<Integer> bottomNodes = new ArrayList<>(nodes.subList(0, medianIndex));
        List<Integer> topNodes = new ArrayList<>(nodes.subList(medianIndex, nodes.size()));

        // Find the actual coordinate bounds for each new shard
        int bottomYMax = yMin;
        int topYMin = yMax;
        
        if (!bottomNodes.isEmpty()) {
            bottomYMax = getMaxCoordinate(bottomNodes, coords, false);
        }
        if (!topNodes.isEmpty()) {
            topYMin = getMinCoordinate(topNodes, coords, false);
        }

        // Use midpoint between the last bottom node and first top node as boundary
        int splitY = (bottomYMax + topYMin) / 2;
        
        Shard bottomShard = new Shard(xMin, xMax, yMin, splitY, bottomNodes);
        Shard topShard = new Shard(xMin, xMax, splitY + 1, yMax, topNodes);

        return new Shard[] { bottomShard, topShard };
    }

    // Median of Medians Selection Algorithm
    private int select(List<Integer> list, int left, int right, int k, java.util.Comparator<Integer> comp) {
        while (left <= right) {
            int pivotIndex = pivot(list, left, right, comp);
            int partitionIndex = partition(list, left, right, pivotIndex, comp);
            if (k == partitionIndex) {
                return k;
            } else if (k < partitionIndex) {
                right = partitionIndex - 1;
            } else {
                left = partitionIndex + 1;
            }
        }
        return k;
    }

    private int pivot(List<Integer> list, int left, int right, java.util.Comparator<Integer> comp) {
        if (right - left < 5) {
            return medianOfFive(list, left, right, comp);
        }
        
        int storeIndex = left;
        for (int i = left; i <= right; i += 5) {
            int subRight = Math.min(i + 4, right);
            int median5 = medianOfFive(list, i, subRight, comp);
            swap(list, median5, storeIndex);
            storeIndex++;
        }
        
        return select(list, left, storeIndex - 1, left + (storeIndex - 1 - left) / 2, comp);
    }

    private int medianOfFive(List<Integer> list, int left, int right, java.util.Comparator<Integer> comp) {
        // Insertion sort for small range
        for (int i = left + 1; i <= right; i++) {
            for (int j = i; j > left && comp.compare(list.get(j), list.get(j - 1)) < 0; j--) {
                swap(list, j, j - 1);
            }
        }
        return (left + right) / 2;
    }

    private int partition(List<Integer> list, int left, int right, int pivotIndex, java.util.Comparator<Integer> comp) {
        Integer pivotValue = list.get(pivotIndex);
        swap(list, pivotIndex, right);
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (comp.compare(list.get(i), pivotValue) <= 0) {
                swap(list, i, storeIndex);
                storeIndex++;
            }
        }
        swap(list, storeIndex, right);
        return storeIndex;
    }
    
    private void swap(List<Integer> list, int i, int j) {
        Integer temp = list.get(i);
        list.set(i, list.get(j));
        list.set(j, temp);
    }

    private int getMaxCoordinate(List<Integer> nodes, Map<Integer, Point> coords, boolean isX) {
        int max = Integer.MIN_VALUE;
        for (Integer id : nodes) {
            int val = isX ? coords.get(id).x : coords.get(id).y;
            if (val > max) max = val;
        }
        return max;
    }

    private int getMinCoordinate(List<Integer> nodes, Map<Integer, Point> coords, boolean isX) {
        int min = Integer.MAX_VALUE;
        for (Integer id : nodes) {
            int val = isX ? coords.get(id).x : coords.get(id).y;
            if (val < min) min = val;
        }
        return min;
    }

    @Override
    public String toString() {
        return String.format("Shard[x:(%d,%d), y:(%d,%d), nodes:%d]", 
                             xMin, xMax, yMin, yMax, getNodeCount());
    }
}
