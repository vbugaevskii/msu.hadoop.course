package utils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;

public class Vector {
    private float[] data;

    public Vector(@Nonnull float[] data) {
        this.data = data;
    }

    public Vector(int length) {
        this.data = new float[length];
    }

    public void set(int i, float value) {
        this.data[i] = value;
    }

    public int length() {
        return data.length;
    }

    public static float dot(Vector a, Vector b) throws IOException {
        if (a.length() != b.length()) {
            throw new IOException(String.format(
                    "Shapes (%d,) and (%d,) not aligned!",
                    a.length(), b.length()
            ));
        }

        float c = 0.0f;
        for (int i = 0; i < a.length(); i++) {
            c += a.data[i] * b.data[i];
        }
        return c;
    }

    @Override
    public String toString() {
        return Arrays.toString(data);
    }
}
