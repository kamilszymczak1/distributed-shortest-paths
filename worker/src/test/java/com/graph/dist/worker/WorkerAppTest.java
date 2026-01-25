package com.graph.dist.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WorkerApp Tests")
class WorkerAppTest {

    @Nested
    @DisplayName("OutOfShardEdge Tests")
    class OutOfShardEdgeTests {

        @Test
        @DisplayName("Should create out-of-shard edge with correct values")
        void shouldCreateOutOfShardEdgeWithCorrectValues() {
            WorkerApp.OutOfShardEdge edge = new WorkerApp.OutOfShardEdge(100, 2, 50);

            assertEquals(100, edge.toGlobalId);
            assertEquals(2, edge.toShard);
            assertEquals(50, edge.weight);
        }
    }

    @Nested
    @DisplayName("OutOfShardUpdate Tests")
    class OutOfShardUpdateTests {

        @Test
        @DisplayName("Should create out-of-shard update with correct values")
        void shouldCreateOutOfShardUpdateWithCorrectValues() {
            WorkerApp.OutOfShardUpdate update = new WorkerApp.OutOfShardUpdate(100, 50, 1, 150);

            assertEquals(100, update.toGlobalId);
            assertEquals(50, update.fromGlobalId);
            assertEquals(1, update.fromShard);
            assertEquals(150, update.distance);
        }
    }

    @Nested
    @DisplayName("ShortestPathSource Tests")
    class ShortestPathSourceTests {

        @Test
        @DisplayName("Should create source from starting node")
        void shouldCreateSourceFromStartingNode() {
            WorkerApp.ShortestPathSource source = new WorkerApp.ShortestPathSource(
                WorkerApp.ShortestPathSource.Type.FROM_STARTING_NODE,
                1,
                0
            );

            assertEquals(WorkerApp.ShortestPathSource.Type.FROM_STARTING_NODE, source.getType());
            assertEquals(1, source.getFromGlobalId());
            assertEquals(0, source.getFromShard());
        }

        @Test
        @DisplayName("Should create source from boundary node")
        void shouldCreateSourceFromBoundaryNode() {
            WorkerApp.ShortestPathSource source = new WorkerApp.ShortestPathSource(
                WorkerApp.ShortestPathSource.Type.FROM_BOUNDARY_NODE,
                10,
                1
            );

            assertEquals(WorkerApp.ShortestPathSource.Type.FROM_BOUNDARY_NODE, source.getType());
            assertEquals(10, source.getFromGlobalId());
            assertEquals(1, source.getFromShard());
        }

        @Test
        @DisplayName("Should create source from out-of-shard update")
        void shouldCreateSourceFromOutOfShardUpdate() {
            WorkerApp.ShortestPathSource source = new WorkerApp.ShortestPathSource(
                WorkerApp.ShortestPathSource.Type.FROM_OUT_OF_SHARD_UPDATE,
                20,
                2
            );

            assertEquals(WorkerApp.ShortestPathSource.Type.FROM_OUT_OF_SHARD_UPDATE, source.getType());
            assertEquals(20, source.getFromGlobalId());
            assertEquals(2, source.getFromShard());
        }

        @Test
        @DisplayName("Type enum should have all expected values")
        void typeEnumShouldHaveAllExpectedValues() {
            WorkerApp.ShortestPathSource.Type[] types = WorkerApp.ShortestPathSource.Type.values();

            assertEquals(3, types.length);
            assertNotNull(WorkerApp.ShortestPathSource.Type.FROM_STARTING_NODE);
            assertNotNull(WorkerApp.ShortestPathSource.Type.FROM_BOUNDARY_NODE);
            assertNotNull(WorkerApp.ShortestPathSource.Type.FROM_OUT_OF_SHARD_UPDATE);
        }
    }
}
