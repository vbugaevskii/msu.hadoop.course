package utils;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class VectorTest {
    @Test
    public void testDot() throws Exception {
        Vector a = new Vector(new float[]{1, 2,  3, 4});
        Vector b = new Vector(new float[]{5, -6, 0, 2});
        float result = Vector.dot(a, b);
        assertEquals(1.0f, result, 1e-4f);
    }

    @Test(expected = IOException.class)
    public void testDotFail() throws Exception {
        Vector a = new Vector(new float[]{1, 2, 4});
        Vector b = new Vector(new float[]{5, -6, 0, 2});
        Vector.dot(a, b);
    }
}