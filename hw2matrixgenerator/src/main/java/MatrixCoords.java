import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MatrixCoords implements WritableComparable<MatrixCoords> {
    private Integer row, col;

    public MatrixCoords() {
        row = -1;
        col = -1;
    }

    public MatrixCoords(int row, int col) {
        this.row = row;
        this.col = col;
    }

    public int getRow() {
        return row;
    }

    public int getCol() {
        return col;
    }

    public void setRow(Integer row) {
        this.row = row;
    }

    public void setCol(Integer col) {
        this.col = col;
    }

    @Override
    public int compareTo(@Nonnull MatrixCoords that) {
        int compareResult = this.row.compareTo(that.row);
        return (compareResult == 0) ? this.col.compareTo(that.col) : compareResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof MatrixCoords)) {
            return false;
        }

        MatrixCoords that = (MatrixCoords) o;
        return Objects.equals(this.row, that.row) && Objects.equals(this.col, that.col);
    }

    @Override
    public int hashCode() {
        int result = (row != null) ? row.hashCode() : 0;
        result = 31 * result + ((col != null) ? col.hashCode() : 0);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(row);
        out.writeInt(col);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        row = in.readInt();
        col = in.readInt();
    }

    @Override
    public String toString() {
        return String.format("%d\t%d", row, col);
    }
}
