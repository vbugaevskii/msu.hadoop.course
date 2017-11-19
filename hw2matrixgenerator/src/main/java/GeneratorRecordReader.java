import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Random;

abstract public class GeneratorRecordReader extends RecordReader<MatrixCoords, FloatWritable> {
    protected int valuesMapped, valuesMappedMax;

    protected Random random;
    protected float minValue, maxValue;
    protected int rowMin, rowMax, colMin, colMax;

    protected MatrixCoords currentKey = null;
    protected Float currentValue = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        minValue = conf.getFloat(mgen.PARAM_MIN_VALUE, mgen.PARAM_MIN_VALUE_DEFAULT);
        maxValue = conf.getFloat(mgen.PARAM_MAX_VALUE, mgen.PARAM_MAX_VALUE_DEFAULT);

        random = new Random(conf.getLong(mgen.PARAM_SEED, mgen.PARAM_SEED_DEFAULT));

        GeneratorInputSplit inputSplit = (GeneratorInputSplit) split;
        rowMin = inputSplit.getRowMin();
        rowMax = inputSplit.getRowMax();
        colMin = inputSplit.getColMin();
        colMax = inputSplit.getColMax();

        double sparsity = conf.getDouble(mgen.PARAM_SPARSITY, mgen.PARAM_SPARSITY_DEFAULT);
        valuesMappedMax = (int) ( (rowMax - rowMin) * (colMax - colMin) * (1.0 - sparsity) );
        valuesMapped = -1;
    }

    protected int generateRandomRow() {
        return rowMin + random.nextInt(rowMax - rowMin);
    }

    protected int generateRandomCol() {
        return colMin + random.nextInt(colMax - colMin);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        currentKey = null;
        currentValue = null;
        return ++valuesMapped < valuesMappedMax;
    }

    abstract public MatrixCoords getCurrentKey() throws IOException, InterruptedException;

    @Override
    public FloatWritable getCurrentValue() throws IOException, InterruptedException {
        if (currentValue == null) {
            currentValue = 0.0f;
            while (currentValue == 0.0f) {
                currentValue = minValue + (maxValue - minValue) * random.nextFloat();
            }
        }
        return new FloatWritable(currentValue);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return ( (float) valuesMapped ) / valuesMappedMax;
    }

    @Override
    public void close() throws IOException {
        valuesMapped = -1;
    }
}
