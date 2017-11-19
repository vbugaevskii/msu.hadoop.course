import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GeneratorRecordReaderForward extends GeneratorRecordReader {
    private List<MatrixCoords> matrixCoordsChosen;
    private int currentElem = 0;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.initialize(split, context);

        Set<MatrixCoords> matrixCoordsSet = new HashSet<>();
        for (int i = 0; i < valuesMappedMax + 1; i++) {
            MatrixCoords key = new MatrixCoords();
            while (matrixCoordsSet.contains(key)) {
                key.setRow(generateRandomRow());
                key.setCol(generateRandomCol());
            }
            matrixCoordsSet.add(key);
        }
        matrixCoordsSet.remove(new MatrixCoords());

        matrixCoordsChosen = new ArrayList<>(matrixCoordsSet);
    }

    @Override
    public MatrixCoords getCurrentKey() throws IOException, InterruptedException {
        if (currentKey == null) {
            currentKey = matrixCoordsChosen.get(currentElem++);
        }
        return currentKey;
    }

    @Override
    public void close() throws IOException {
        super.close();
        matrixCoordsChosen.clear();
    }
}
