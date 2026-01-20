package com.graph.dist.worker;

import java.util.Arrays;

public class IndexedPriorityQueue {
    private final int[] values; // The "priority" (distance)
    private final int[] heap; // The binary heap (stores node indices)
    private final int[] pos; // Inverse map: pos[nodeIndex] = heapIndex
    private int size;

    public IndexedPriorityQueue(int maxNodes) {
        values = new int[maxNodes];
        heap = new int[maxNodes];
        pos = new int[maxNodes];
        size = 0;
        Arrays.fill(pos, -1); // -1 indicates node is not in heap
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean contains(int node) {
        return pos[node] != -1;
    }

    public int peekMinItem() {
        return heap[0];
    }

    public int peekMinValue() {
        return values[heap[0]];
    }

    // Add a node or update its value if it already exists (decreaseKey)
    public void addOrUpdate(int node, int value) {
        if (contains(node)) {
            // Update (Decrease Key)
            if (value < values[node]) {
                values[node] = value;
                swim(pos[node]);
            }
        } else {
            // Add new
            values[node] = value;
            heap[size] = node;
            pos[node] = size;
            swim(size);
            size++;
        }
    }

    public int poll() {
        int res = heap[0];
        swap(0, size - 1);
        pos[res] = -1; // Mark as removed
        size--;
        if (size > 0)
            sink(0);
        return res;
    }

    private void swim(int i) {
        while (i > 0) {
            int p = (i - 1) / 2;
            if (values[heap[i]] >= values[heap[p]])
                break;
            swap(i, p);
            i = p;
        }
    }

    private void sink(int i) {
        while (2 * i + 1 < size) {
            int left = 2 * i + 1;
            int right = 2 * i + 2;
            int j = left;
            if (right < size && values[heap[right]] < values[heap[left]]) {
                j = right;
            }
            if (values[heap[i]] <= values[heap[j]])
                break;
            swap(i, j);
            i = j;
        }
    }

    private void swap(int i, int j) {
        int temp = heap[i];
        heap[i] = heap[j];
        heap[j] = temp;
        // Update positions map
        pos[heap[i]] = i;
        pos[heap[j]] = j;
    }
}