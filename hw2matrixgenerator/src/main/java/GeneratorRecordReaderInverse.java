import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GeneratorRecordReaderInverse extends GeneratorRecordReader {
    private Set<MatrixCoords> matrixCoordsZeros;

    private int currentRow, currentCol;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.initialize(split, context);

        currentRow = rowMin;
        currentCol = colMin;

        matrixCoordsZeros = new HashSet<>();

        int valuesMappedZeros = (rowMax - rowMin) * (colMax - colMin) - valuesMappedMax;
        for (int i = 0; i < valuesMappedZeros + 1; i++) {
            MatrixCoords key = new MatrixCoords();
            while (matrixCoordsZeros.contains(key)) {
                key.setRow(generateRandomRow());
                key.setCol(generateRandomCol());
            }
            matrixCoordsZeros.add(key);
        }

        for (MatrixCoords s : matrixCoordsZeros) {
            System.out.println(s.toString());
        }
    }

    @Override
    public MatrixCoords getCurrentKey() throws IOException, InterruptedException {
        if (currentKey == null) {
            currentKey = new MatrixCoords();

            outer:
            for (; currentRow < rowMax; currentRow++) {
                for (; currentCol < colMax; currentCol++) {
                    if (!matrixCoordsZeros.contains(currentKey)) {
                        break outer;
                    }
                    currentKey.setRow(currentRow);
                    currentKey.setCol(currentCol);
                }
                currentCol = colMin;
            }
        }
        return currentKey;
    }

    @Override
    public void close() throws IOException {
        super.close();
        matrixCoordsZeros.clear();
    }
}
