import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultiplierMapper extends Mapper<LongWritable, Text, MatrixGroup, MatrixValue> {
    private static int strideWidth;
    private static int numRowsMatrixL, numColsMatrixL, numRowsMatrixR, numColsMatrixR;
    private static String tags;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        tags = conf.get(mm.PARAM_TAGS, mm.PARAM_TAGS_DEFAULT);

        strideWidth = Integer.valueOf(conf.get(mm.PARAM_STRIDE_WIDTH));

        numRowsMatrixL = Integer.valueOf(conf.get(mm.PARAM_NUM_ROWS_L));
        numColsMatrixL = Integer.valueOf(conf.get(mm.PARAM_NUM_COLS_L));
        numRowsMatrixR = Integer.valueOf(conf.get(mm.PARAM_NUM_ROWS_R));
        numColsMatrixR = Integer.valueOf(conf.get(mm.PARAM_NUM_COLS_R));
    }

    private int getRowOffset(int rowIndex) {
        return rowIndex - (rowIndex % strideWidth);
    }

    private int getColOffset(int colIndex) {
        return colIndex - (colIndex % strideWidth);
    }

    private boolean isLeftMatrix(char tag) {
        return tags.charAt(0) == tag;
    }

    private boolean isRightMatrix(char tag) {
        return tags.charAt(1) == tag;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().replaceAll("^\\s+", "")
                .replaceAll("\\s+$", "").split("\\t");

        if (split.length != 4) {
            System.out.println(String.format("Wrong input line: %s", value.toString()));
            return;
        }

        Character matrixTag = split[0].charAt(0);
        Integer matrixRow = Integer.valueOf(split[1]),
                matrixCol = Integer.valueOf(split[2]);
        Float matrixValue = Float.valueOf(split[3]);

        boolean isLeft = isLeftMatrix(matrixTag), isRight = isRightMatrix(matrixTag);
        if (isLeft == isRight) {
            throw new IOException(String.format("Tag '%c' is not recognised!", matrixTag));
        }

        int rowMin, rowMax, colMin, colMax;
        if (isLeft) {
            // send row from left matrix
            rowMin = getRowOffset(matrixRow);
            rowMax = Math.min(rowMin + strideWidth, numRowsMatrixL);

            // to every column from right matrix
            for (colMin = 0; colMin < numColsMatrixR; colMin += strideWidth) {
                colMax = Math.min(colMin + strideWidth, numColsMatrixR);
                context.write(
                        new MatrixGroup(isLeft, rowMin, rowMax, colMin, colMax),
                        new MatrixValue(matrixRow, matrixCol, matrixValue)
                );
//                System.out.printf(
//                        "%d\t%d\t%d\t%d A[%d %d] = %.3f\n",
//                        rowMin, rowMax, colMin, colMax,
//                        matrixRow, matrixCol, matrixValue
//                );
            }
        } else {
            // send column from right matrix
            colMin = getColOffset(matrixCol);
            colMax = Math.min(colMin + strideWidth, numColsMatrixR);

            // to every row from left matrix
            for (rowMin = 0; rowMin < numRowsMatrixL; rowMin += strideWidth) {
                rowMax = Math.min(rowMin + strideWidth, numRowsMatrixL);
                context.write(
                        new MatrixGroup(isLeft, rowMin, rowMax, colMin, colMax),
                        new MatrixValue(matrixRow, matrixCol, matrixValue)
                );
//                System.out.printf(
//                        "%d\t%d\t%d\t%d B[%d %d] = %.3f\n",
//                        rowMin, rowMax, colMin, colMax,
//                        matrixRow, matrixCol, matrixValue
//                );
            }
        }
    }
}
