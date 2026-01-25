package com.graph.dist.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Point Tests")
class PointTest {

    @Test
    @DisplayName("Should create point with correct coordinates")
    void shouldCreatePointWithCorrectCoordinates() {
        Point point = new Point(10, 20);
        
        assertEquals(10, point.x);
        assertEquals(20, point.y);
    }

    @Test
    @DisplayName("Should handle zero coordinates")
    void shouldHandleZeroCoordinates() {
        Point point = new Point(0, 0);
        
        assertEquals(0, point.x);
        assertEquals(0, point.y);
    }

    @Test
    @DisplayName("Should handle negative coordinates")
    void shouldHandleNegativeCoordinates() {
        Point point = new Point(-5, -10);
        
        assertEquals(-5, point.x);
        assertEquals(-10, point.y);
    }

    @ParameterizedTest
    @DisplayName("Should create points with various coordinate combinations")
    @CsvSource({
        "0, 0",
        "100, 200",
        "-50, 75",
        "2147483647, -2147483648",  // Integer max and min
        "1, 1"
    })
    void shouldCreatePointsWithVariousCoordinates(int x, int y) {
        Point point = new Point(x, y);
        
        assertEquals(x, point.x);
        assertEquals(y, point.y);
    }

    @Test
    @DisplayName("Should allow modifying coordinates")
    void shouldAllowModifyingCoordinates() {
        Point point = new Point(1, 2);
        
        point.x = 100;
        point.y = 200;
        
        assertEquals(100, point.x);
        assertEquals(200, point.y);
    }
}
