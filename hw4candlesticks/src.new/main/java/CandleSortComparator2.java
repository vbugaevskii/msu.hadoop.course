import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CandleSortComparator2 extends WritableComparator {
    CandleSortComparator2() {
        super(DealKey.class, true);
    }

    public int compare(WritableComparable key1, WritableComparable key2) {
        // Dirty hack for TotalOrderPartitioner
        DealKey k1 = (DealKey) key1;
        DealKey k2 = (DealKey) key2;
        return k1.getGroup().compareTo(k2.getGroup());
    }

}
