import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class DealKey implements WritableComparable<DealKey> {
    private String symbol;
    private Long id, moment, group;

    public DealKey() {

    }

    public DealKey(String symbol, Long group, Long moment, Long id) {
        this.symbol = symbol;
        this.group = group;
        this.moment = moment;
        this.id = id;
    }

    public String getSymbol() {
        return symbol;
    }

    public Long getId() {
        return id;
    }

    public Long getMoment() {
        return moment;
    }

    public Long getGroup() {
        return group;
    }

    @Override
    public int compareTo(@Nonnull DealKey that) {
        int compareResult = this.symbol.compareTo(that.symbol);
        compareResult = (compareResult == 0) ? this.group.compareTo(that.group) : compareResult;
        compareResult = (compareResult == 0) ? this.moment.compareTo(that.moment) : compareResult;
        compareResult = (compareResult == 0) ? this.id.compareTo(that.id) : compareResult;
        return compareResult;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(symbol);
        out.writeLong(group);
        out.writeLong(moment);
        out.writeLong(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        symbol = in.readUTF();
        group = in.readLong();
        moment = in.readLong();
        id = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DealKey that = (DealKey) o;
        return Objects.equals(symbol, that.symbol) &&
                Objects.equals(group, that.group) &&
                Objects.equals(moment, that.moment) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        // Dirty hack for HashPartitioner
        return Objects.hash(symbol, group);
    }
}
