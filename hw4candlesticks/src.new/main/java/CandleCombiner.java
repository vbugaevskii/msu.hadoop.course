import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CandleCombiner extends Reducer<DealKey, DoubleWritable, DealKey, DoubleWritable> {
    @Override
    protected void reduce(DealKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
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

        context.write(openKey,  new DoubleWritable(openValue));
        context.write(highKey,  new DoubleWritable(highValue));
        context.write(lowKey,   new DoubleWritable(lowValue));
        context.write(closeKey, new DoubleWritable(closeValue));
    }
}
