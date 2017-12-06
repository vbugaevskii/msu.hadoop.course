import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CandleSortComparator extends WritableComparator {
    CandleSortComparator() {
        super(DealKey.class, true);
    }

    public int compare(WritableComparable key1, WritableComparable key2) {
        return ((DealKey) key1).compareTo((DealKey) key2);
    }
}
