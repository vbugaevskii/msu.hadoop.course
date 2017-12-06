import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CandleCombiner extends Reducer<DealKey, DoubleWritable, DealKey, DoubleWritable> {
    @Override
    protected void reduce(DealKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double openValue = -1, lowValue = -1, highValue = -1, closeValue = -1;
        DealKey openKey = null, lowKey = null, highKey = null, closeKey;

        for (DoubleWritable value : values) {
            if (openValue == -1) {
                openValue = value.get();
                openKey = new DealKey(
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

            closeValue = value.get();
        }

        closeKey = key;

        context.write(openKey,  new DoubleWritable(openValue));
        context.write(highKey,  new DoubleWritable(highValue));
        context.write(lowKey,   new DoubleWritable(lowValue));
        context.write(closeKey, new DoubleWritable(closeValue));
    }
}
