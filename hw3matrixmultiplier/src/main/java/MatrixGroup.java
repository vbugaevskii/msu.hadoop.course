import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Objects;

public class MatrixGroup implements WritableComparable<MatrixGroup> {
    private Boolean isLeft;
    private Integer rowMin, rowMax, colMin, colMax;

    public MatrixGroup() {
        isLeft = false;
        rowMin = rowMax = colMin = colMax = -1;
    }

    public MatrixGroup(boolean isLeft, int rowMin, int rowMax, int colMin, int colMax) {
        this.isLeft = isLeft;
        this.rowMin = rowMin;
        this.rowMax = rowMax;
        this.colMin = colMin;
        this.colMax = colMax;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isLeft);
        out.writeInt(rowMin);
        out.writeInt(rowMax);
        out.writeInt(colMin);
        out.writeInt(colMax);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isLeft = in.readBoolean();
        rowMin = in.readInt();
        rowMax = in.readInt();
        colMin = in.readInt();
        colMax = in.readInt();
    }

    public Boolean getLeft() {
        return isLeft;
    }

    public Integer getRowMin() {
        return rowMin;
    }

    public Integer getRowMax() {
        return rowMax;
    }

    public Integer getColMin() {
        return colMin;
    }

    public Integer getColMax() {
        return colMax;
    }

    @Override
    public int compareTo(@Nonnull MatrixGroup that) {
        int compareResult = this.isLeft.compareTo(that.isLeft);
        compareResult = (compareResult == 0) ? this.rowMin.compareTo(that.rowMin) : compareResult;
        compareResult = (compareResult == 0) ? this.colMin.compareTo(that.colMin) : compareResult;
        compareResult = (compareResult == 0) ? this.rowMax.compareTo(that.rowMax) : compareResult;
        compareResult = (compareResult == 0) ? this.colMax.compareTo(that.colMax) : compareResult;
        return compareResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatrixGroup that = (MatrixGroup) o;
        return Objects.equals(isLeft, that.isLeft) &&
                Objects.equals(rowMin, that.rowMin) &&
                Objects.equals(rowMax, that.rowMax) &&
                Objects.equals(colMin, that.colMin) &&
                Objects.equals(colMax, that.colMax);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isLeft, rowMin, rowMax, colMin, colMax);
    }
}
