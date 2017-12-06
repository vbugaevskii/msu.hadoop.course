import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CandleGroupComparator extends WritableComparator {
    CandleGroupComparator() {
        super(DealKey.class, true);
    }

    public int compare(WritableComparable value1, WritableComparable value2) {
        DealKey key1 = (DealKey) value1;
        DealKey key2 = (DealKey) value2;
        int res = key1.getSymbol().compareTo(key2.getSymbol());
        res = (res == 0) ? key1.getGroup().compareTo(key2.getGroup()) : res;
        return res;
    }
}
