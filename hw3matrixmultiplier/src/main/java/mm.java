import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.Arrays;

public class mm extends Configured implements Tool {
    public static final String PARAM_NUM_GROUPS = "mm.groups";

    public static final String PARAM_TAGS = "mm.tags";
    public static final String PARAM_TAGS_DEFAULT = "ABC";

    public static final String PARAM_FLOAT_FORMAT = "mm.float-format";
    public static final String PARAM_FLOAT_FORMAT_DEFAULT = "%.3f";

    public static final String PARAM_NUM_REDUCERS = "mm.num-reducers";
    public static final int PARAM_NUM_REDUCERS_DEFAULT = 1;

    public static final String PARAM_STRIDE_WIDTH = "mgen.matrix.stride_width";

    public static final String PARAM_NUM_ROWS_L = "mgen.matrix.rows_l";
    public static final String PARAM_NUM_COLS_L = "mgen.matrix.cols_l";
    public static final String PARAM_NUM_ROWS_R = "mgen.matrix.rows_r";
    public static final String PARAM_NUM_COLS_R = "mgen.matrix.cols_r";

    private void checkMultiplication(String pathToMatrixL, String pathToMatrixR) throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(URI.create(pathToMatrixL), conf);

        int numRowsMatrixL, numColsMatrixL, numRowsMatrixR, numColsMatrixR;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(new Path(pathToMatrixL + "/size")))
        )) {
            String matrixSize[] = reader.readLine().split("\\t");
            numRowsMatrixL = Integer.valueOf(matrixSize[0]);
            numColsMatrixL = Integer.valueOf(matrixSize[1]);
            conf.setInt(PARAM_NUM_ROWS_L, numRowsMatrixL);
            conf.setInt(PARAM_NUM_COLS_L, numColsMatrixL);
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(new Path(pathToMatrixR + "/size")))
        )) {
            String matrixSize[] = reader.readLine().split("\\t");
            numRowsMatrixR = Integer.valueOf(matrixSize[0]);
            numColsMatrixR = Integer.valueOf(matrixSize[1]);
            conf.setInt(PARAM_NUM_ROWS_R, numRowsMatrixR);
            conf.setInt(PARAM_NUM_COLS_R, numColsMatrixR);
        }

        if (numColsMatrixL != numRowsMatrixR) {
            throw new IOException(String.format(
                    "Shapes (%d,%d) and (%d,%d) not aligned!",
                    numRowsMatrixL, numColsMatrixL,
                    numRowsMatrixR, numColsMatrixR
            ));
        }

        int numGroups = Integer.valueOf(conf.get(PARAM_NUM_GROUPS));
        if (numGroups > numRowsMatrixL) {
            throw new IOException(String.format(
                    "Number of groups should be <= %d", numRowsMatrixL
            ));
        }

        int strideWidth = (int) Math.ceil((float) numRowsMatrixL / numGroups);
        conf.setInt(PARAM_STRIDE_WIDTH, strideWidth);

        System.out.println(String.format("Stride width: %d", strideWidth));
    }

    private Job getJobConf(String inputs[], String output) throws IOException {
        checkMultiplication(inputs[0], inputs[1]);

        Job job = Job.getInstance(getConf());

        job.setJarByClass(mm.class);
        job.setJobName("[HW3] Matrices Multiplier");

        for (String input : inputs) {
            FileInputFormat.addInputPath(job, new Path(input + "/data"));
        }
        FileOutputFormat.setOutputPath(job, new Path(output + "/data"));

        job.setMapperClass(MultiplierMapper.class);
        job.setReducerClass(MultiplierReducer.class);
        job.setSortComparatorClass(MatrixSortComparator.class);
        job.setGroupingComparatorClass(MatrixGroupComparator.class);

        job.setMapOutputKeyClass(MatrixGroup.class);
        job.setMapOutputValueClass(MatrixValue.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(getConf().getInt(PARAM_NUM_REDUCERS, PARAM_NUM_REDUCERS_DEFAULT));

        return job;
    }

    public int run(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            args[i] = args[i].replaceAll("/$", "");
        }

        Job job = getJobConf(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
        int ret = job.waitForCompletion(true) ? 0 : 1;
        if (ret == 0) {
            String matrixPath = args[args.length-1];
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(URI.create(matrixPath), conf);

            try (BufferedWriter outputWriter = new BufferedWriter(
                    new OutputStreamWriter(fs.create(new Path(matrixPath + "/size")))
            )) {
                outputWriter.write(String.format(
                        "%d\t%d\n",
                        Integer.valueOf(conf.get(PARAM_NUM_ROWS_L)),
                        Integer.valueOf(conf.get(PARAM_NUM_COLS_R))
                ));
            }

            try (BufferedWriter outputWriter = new BufferedWriter(
                    new FileWriter("stats.logs", true))) {
                long runtime = job.getFinishTime() - job.getStartTime();
                int numGroups = Integer.valueOf(conf.get(PARAM_NUM_GROUPS));
                outputWriter.write(String.format("%d\t%d\n", numGroups, runtime));
            }
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new mm(), args);
        System.exit(ret);
    }
}
