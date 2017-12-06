import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CandleSortComparator1 extends WritableComparator {
    CandleSortComparator1() {
        super(DealKey.class, true);
    }

    public int compare(WritableComparable key1, WritableComparable key2) {
        DealKey k1 = (DealKey) key1;
        DealKey k2 = (DealKey) key2;
        return k1.compareTo(k2);
    }

}
