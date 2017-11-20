import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MatrixValue implements Writable {
    private Integer row, col;
    private Float value;

    public MatrixValue() {
        row = -1;
        col = -1;
        value = 0.0f;
    }

    public MatrixValue(int row, int col, float value) {
        this.row = row;
        this.col = col;
        this.value = value;
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

    public Float getValue() {
        return value;
    }

    public void setValue(Float value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof MatrixValue)) {
            return false;
        }

        MatrixValue that = (MatrixValue) o;
        return Objects.equals(this.row, that.row) &&
                Objects.equals(this.col, that.col) &&
                Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        int result = (row != null) ? row.hashCode() : 0;
        result = 31 * result + ((col != null) ? col.hashCode() : 0);
        result = 31 * result + ((value != null) ? value.hashCode() : 0);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(row);
        out.writeInt(col);
        out.writeFloat(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        row = in.readInt();
        col = in.readInt();
        value = in.readFloat();
    }

    @Override
    public String toString() {
        return String.format("%d\t%d\t%f", row, col, value);
    }
}
