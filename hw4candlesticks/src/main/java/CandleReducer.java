import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class CandleReducer extends Reducer<DealKey, DoubleWritable, NullWritable, Text> {
    private final static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private MultipleOutputs<NullWritable, Text> out;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(DealKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String symbol = key.getSymbol();
        long candleStarts = key.getGroup();

        double openValue = -1, lowValue = -1, highValue = -1, closeValue = -1;

        for (DoubleWritable value : values) {
            if (openValue == -1) {
                openValue = value.get();
            }

            if (lowValue == -1 || lowValue > value.get()) {
                lowValue = value.get();
            }

            if (highValue == -1 || highValue < value.get()) {
                highValue = value.get();
            }

            closeValue = value.get();
        }

        out.write(NullWritable.get(), new Text(String.format(
                "%s,%s,%.1f,%.1f,%.1f,%.1f",
                symbol,
                format.format(candleStarts),
                openValue, highValue, lowValue, closeValue
        )), symbol);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
    }
}
