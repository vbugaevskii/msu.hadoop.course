import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class GeneratorInputFormat extends InputFormat<MatrixCoords, FloatWritable> {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        int numMappers = conf.getInt(mgen.PARAM_NUM_MAPPERS, mgen.PARAM_NUM_MAPPERS_DEFAULT);
        int matrixRows = conf.getInt(mgen.PARAM_ROW_COUNT, mgen.PARAM_ROW_COUNT_DEFAULT);
        int matrixCols = conf.getInt(mgen.PARAM_COL_COUNT, mgen.PARAM_COL_COUNT_DEFAULT);

        List<InputSplit> splits = new LinkedList<>();

        if (numMappers > matrixRows && numMappers > matrixCols) {
            numMappers = Math.max(matrixCols, matrixRows);
        }

        int strideWidth, colMin, colMax, rowMin, rowMax;
        if (numMappers <= matrixRows) {
            // split by rows
            strideWidth = (int) Math.ceil(((float) matrixRows) / numMappers);
            colMin = 0;
            colMax = matrixCols;

            for (rowMin = 0; rowMin < matrixRows; rowMin += strideWidth) {
                rowMax = Math.min(rowMin + strideWidth, matrixRows);
                splits.add(new GeneratorInputSplit(rowMin, rowMax, colMin, colMax));
            }
        } else if (numMappers <= matrixCols) {
            // split by columns
            strideWidth = (int) Math.ceil(((float) matrixCols) / numMappers);
            rowMin = 0;
            rowMax = matrixRows;

            for (colMin = 0; colMin < matrixCols; colMin += strideWidth) {
                colMax = Math.min(colMin + strideWidth, matrixCols);
                splits.add(new GeneratorInputSplit(rowMin, rowMax, colMin, colMax));
            }
        }

        return splits;
    }

    @Override
    public RecordReader<MatrixCoords, FloatWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        double sparsity = context.getConfiguration().getDouble(mgen.PARAM_SPARSITY, mgen.PARAM_SPARSITY_DEFAULT);
        RecordReader recordReader = (sparsity < 0.5) ?
                new GeneratorRecordReaderInverse() : new GeneratorRecordReaderForward();
        recordReader.initialize(split, context);
        return recordReader;
    }
}
