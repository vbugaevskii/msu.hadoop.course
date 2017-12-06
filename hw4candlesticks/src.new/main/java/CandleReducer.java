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
        DealKey openKey = null, lowKey = null, highKey = null, closeKey = null;

        for (DoubleWritable value : values) {
            if (openKey == null || openKey.compareTo(key) < 0) {
                openValue = value.get();
                openKey = new DealKey(
                        key.getSymbol(),
                        key.getGroup(),
                        key.getMoment(),
                        key.getId()
                );
            }

            if (closeKey == null || openKey.compareTo(key) > 0) {
                closeValue = value.get();
                closeKey = new DealKey(
                        key.getSymbol(),
                        key.getGroup(),
                        key.getMoment(),
                        key.getId()
                );
            }

            if (lowValue == -1 || lowValue > value.get()) {
                lowValue = value.get();
                lowKey = new DealKey(
                        key.getSymbol(),
                        key.getGroup(),
                        key.getMoment(),
                        key.getId()
                );
            }

            if (highValue == -1 || highValue < value.get()) {
                highValue = value.get();
                highKey = new DealKey(
                        key.getSymbol(),
                        key.getGroup(),
                        key.getMoment(),
                        key.getId()
                );
            }
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
