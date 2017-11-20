package utils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;


public class Matrix {
    private float[][] data;

    public Matrix(@Nonnull float[][] data) {
        this.data = data;
    }

    public Matrix(int numRows, int numCols) {
        data = new float[numRows][numCols];
    }

    public void set(int row, int col, float value) {
        data[row][col] = value;
    }

    public float get(int row, int col) {
        return data[row][col];
    }

    public int getNumRows() {
        return data.length;
    }

    public int getNumCols() {
        return data[0].length;
    }

    public Vector getRow(int i) {
        return new Vector(Arrays.copyOf(data[i], getNumCols()));
    }

    public Vector getCol(int j) {
        float[] result = new float[getNumRows()];
        for (int i = 0; i < getNumRows(); i++) {
            result[i] = data[i][j];
        }
        return new Vector(result);
    }

    public static Matrix mul(Matrix a, Matrix b) throws IOException {
        if (a.getNumCols() != b.getNumRows()) {
            throw new IOException(String.format(
                    "Shapes (%d,%d) and (%d,%d) not aligned!",
                    a.getNumRows(), a.getNumCols(),
                    b.getNumRows(), b.getNumCols()
            ));
        }

        Matrix c = new Matrix(a.getNumRows(), b.getNumCols());
        for (int i = 0; i < c.getNumRows(); i++) {
            for (int j = 0; j < c.getNumCols(); j++) {
                Vector va = a.getRow(i), vb = b.getCol(j);
                c.set(i, j, Vector.dot(va, vb));
            }
        }
        return c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Matrix matrix = (Matrix) o;
        return Arrays.deepEquals(data, matrix.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (int i = 0; i < getNumRows(); i++) {
            if (i > 0) {
                builder.append(' ');
            }
            builder.append(Arrays.toString(data[i]));
            if (i + 1 < getNumRows()) {
                builder.append(",\n");
            }
        }
        builder.append(']');
        return builder.toString();
    }
}
