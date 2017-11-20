import utils.Matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiplierReducer extends Reducer<MatrixGroup, MatrixValue, NullWritable, Text> {
    private static String outputFormat;
    private static int numRowsMatrixL, numColsMatrixL, numRowsMatrixR, numColsMatrixR;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        Configuration conf = context.getConfiguration();

        String tags = conf.get(mm.PARAM_TAGS, mm.PARAM_TAGS_DEFAULT);
        builder.append(tags.charAt(2));
        builder.append("\t%d\t%d\t");
        String floatFormat = conf.get(
                mm.PARAM_FLOAT_FORMAT,
                mm.PARAM_FLOAT_FORMAT_DEFAULT
        );
        builder.append(floatFormat);

        outputFormat = builder.toString();

        numRowsMatrixL = Integer.valueOf(conf.get(mm.PARAM_NUM_ROWS_L));
        numColsMatrixL = Integer.valueOf(conf.get(mm.PARAM_NUM_COLS_L));
        numRowsMatrixR = Integer.valueOf(conf.get(mm.PARAM_NUM_ROWS_R));
        numColsMatrixR = Integer.valueOf(conf.get(mm.PARAM_NUM_COLS_R));
    }

    @Override
    protected void reduce(MatrixGroup key, Iterable<MatrixValue> values, Context context)
            throws IOException, InterruptedException {
        float[][] dataA = null;
        float[][] dataB = null;
        float[][] dataCurr;

        int rowMinC = -1, colMinC = -1;
        int rowOffset, colOffset;

        int matrixRow, matrixCol;
        float matrixVal;

        for (MatrixValue value : values) {
            if (key.getLeft()) {
                // got the row
                if (dataA == null) {
                    int numRows = key.getRowMax() - key.getRowMin();
                    int numCols = numColsMatrixL;
                    dataA = new float[numRows][numCols];

                    rowMinC = key.getRowMin();

//                    System.out.printf(
//                            "Left. Row: [%d %d]; Cols: [%d %d]\n",
//                            key.getRowMin(), key.getRowMax(),
//                            key.getColMin(), key.getColMax()
//                    );
                }

                dataCurr = dataA;
                rowOffset = key.getRowMin();
                colOffset = 0;
            } else {
                // got the column
                if (dataB == null) {
                    int numRows = numRowsMatrixR;
                    int numCols = key.getColMax() - key.getColMin();
                    dataB = new float[numRows][numCols];

                    colMinC = key.getColMin();

//                    System.out.printf(
//                            "Right. Row: [%d %d]; Cols: [%d %d]\n",
//                            key.getRowMin(), key.getRowMax(),
//                            key.getColMin(), key.getColMax()
//                    );
                }

                dataCurr = dataB;
                rowOffset = 0;
                colOffset = key.getColMin();
            }

            matrixRow = value.getRow() - rowOffset;
            matrixCol = value.getCol() - colOffset;
            matrixVal = value.getValue();
            dataCurr[matrixRow][matrixCol] = matrixVal;
        }

        if (dataA == null) {
            System.out.println("Left matrix is empty!");
            return;
        } else if (dataB == null) {
            System.out.println("Right matrix is empty!");
            return;
        }

        Matrix matrixA = new Matrix(dataA);
        Matrix matrixB = new Matrix(dataB);
        Matrix matrixC = Matrix.mul(matrixA, matrixB);

        for (int i = 0; i < matrixC.getNumRows(); i++) {
            for (int j = 0; j < matrixC.getNumCols(); j++) {
                matrixRow = i + rowMinC;
                matrixCol = j + colMinC;
                matrixVal = matrixC.get(i, j);
                if (matrixVal != 0.0f) {
                    context.write(
                            NullWritable.get(),
                            new Text(String.format(outputFormat, matrixRow, matrixCol, matrixVal))
                    );
                }
            }
        }
    }
}
