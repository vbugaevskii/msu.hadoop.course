import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GeneratorInputSplit extends InputSplit implements Writable {
    private int rowMin, rowMax, colMin, colMax;

    public GeneratorInputSplit() {
        rowMin = rowMax = colMin = colMax = -1;
    }

    public GeneratorInputSplit(int rowMin, int rowMax, int colMin, int colMax) {
        this.rowMin = rowMin;
        this.rowMax = rowMax;
        this.colMin = colMin;
        this.colMax = colMax;
    }

    public int getRowMin() {
        return rowMin;
    }

    public int getRowMax() {
        return rowMax;
    }

    public int getColMin() {
        return colMin;
    }

    public int getColMax() {
        return colMax;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return (rowMax - rowMin) * (colMax - colMin) * (Float.SIZE / Byte.SIZE);
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(rowMin);
        out.writeInt(rowMax);
        out.writeInt(colMin);
        out.writeInt(colMax);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rowMin = in.readInt();
        rowMax = in.readInt();
        colMin = in.readInt();
        colMax = in.readInt();
    }
}