package utils;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class MatrixTest {
    @Test
    public void testMul() throws Exception {
        Matrix a = new Matrix(new float[][] {
                {2, 1, 0},
                {3, 1, 1}
        });
        Matrix b = new Matrix(new float[][] {
                {1, 2},
                {2, 1},
                {2, 2}
        });
        Matrix c = new Matrix(new float[][] {
                {4, 5},
                {7, 9}
        });
        Matrix d = Matrix.mul(a, b);
        assertTrue(c.equals(d));
    }

    @Test
    public void testMulVector() throws Exception {
        Matrix a = new Matrix(new float[][] {
                {3, 0, -3},
                {-4, 2, 7},
                {1, -2, -4}
        });
        Matrix b = new Matrix(new float[][]{{-2}, {3}, {2}});
        Matrix c = new Matrix(new float[][]{{-12}, {28}, {-16}});
        Matrix d = Matrix.mul(a, b);
        assertTrue(c.equals(d));
    }

    @Test
    public void testMulSquare() throws Exception {
        Matrix a = new Matrix(new float[][] {
                {1, 2},
                {3, 4},
        });
        Matrix b = new Matrix(new float[][] {
                {5, 6},
                {7, 8},
        });
        Matrix c = new Matrix(new float[][] {
                {19, 22},
                {43, 50},
        });
        Matrix d = Matrix.mul(a, b);
        assertTrue(c.equals(d));
    }

    @Test(expected = IOException.class)
    public void testFail() throws Exception {
        Matrix a = new Matrix(new float[][] {
                {2, 1, 0},
                {3, 1, 1}
        });
        Matrix b = new Matrix(new float[][] {
                {1, 2},
                {2, 1}
        });
        Matrix.mul(a, b);
    }

    @Test
    public void one() throws Exception {
        Matrix a = new Matrix(new float[][] {{4}});
        Matrix b = new Matrix(new float[][] {{2}});
        Matrix c = new Matrix(new float[][] {{8}});
        assertTrue(c.equals(Matrix.mul(a, b)));
    }
}