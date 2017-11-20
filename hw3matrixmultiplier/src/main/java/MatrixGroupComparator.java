import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MatrixGroupComparator extends WritableComparator {
    MatrixGroupComparator() {
        super(MatrixGroup.class, true);
    }

    public int compare(WritableComparable value1, WritableComparable value2) {
        MatrixGroup group1 = (MatrixGroup) value1;
        MatrixGroup group2 = (MatrixGroup) value2;
        int res = group1.getRowMin().compareTo(group2.getRowMin());
        return (res == 0) ? group1.getColMin().compareTo(group2.getColMin()) : res;
    }
}
