import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;

public class mgen extends Configured implements Tool {
    public static final String PARAM_ROW_COUNT = "mgen.row-count";
    public static final String PARAM_COL_COUNT = "mgen.column-count";
    public static final int PARAM_ROW_COUNT_DEFAULT = 2;
    public static final int PARAM_COL_COUNT_DEFAULT = 2;

    public static final String PARAM_MIN_VALUE = "mgen.min";
    public static final String PARAM_MAX_VALUE = "mgen.max";
    public static final float PARAM_MIN_VALUE_DEFAULT = 0.0f;
    public static final float PARAM_MAX_VALUE_DEFAULT = 1.0f;

    public static final String PARAM_SPARSITY = "mgen.sparsity";
    public static final double PARAM_SPARSITY_DEFAULT = 0.0;

    public static final String PARAM_FLOAT_FORMAT = "mgen.float-format";
    public static final String PARAM_FLOAT_FROMAT_DEFAULT = "%.3f";

    public static final String PARAM_TAG  = "mgen.tag";
    public static final String PARAM_SEED = "mgen.seed";
    public static final long PARAM_SEED_DEFAULT = System.currentTimeMillis();

    public static final String PARAM_NUM_MAPPERS  = "mgen.num-mappers";
    public static final int PARAM_NUM_MAPPERS_DEFAULT = 1;

    private Job getJobConf(String output) throws IOException {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(mgen.class);
        job.setJobName("[HW2] Sparse Matrix Generator");

        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setInputFormatClass(GeneratorInputFormat.class);

        job.setMapperClass(GeneratorMapper.class);

        job.setMapOutputKeyClass(MatrixCoords.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(0);

        return job;
    }

    public int run(String[] args) throws Exception {
        String matrixPath = args[0].replaceAll("/$", "");
        Job job = getJobConf(matrixPath + "/data");
        int ret = job.waitForCompletion(true) ? 0 : 1;
        if (ret == 0) {
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(URI.create(matrixPath), conf);

            try (BufferedWriter outputWriter = new BufferedWriter(
                    new OutputStreamWriter(fs.create(new Path(matrixPath + "/size")))
            )) {
                outputWriter.write(String.format(
                        "%d\t%d\n",
                        conf.getInt(PARAM_ROW_COUNT, PARAM_ROW_COUNT_DEFAULT),
                        conf.getInt(PARAM_COL_COUNT, PARAM_COL_COUNT_DEFAULT)
                ));
            }

            try (BufferedWriter outputWriter = new BufferedWriter(
                    new FileWriter("stats.logs", true))) {
                Counters counters = job.getCounters();
                long runtime = job.getFinishTime() - job.getStartTime();
                long bytesRead = counters.findCounter(
                        FileSystemCounter.class.getName(), "FILE_BYTES_READ"
                ).getValue();
                long bytesWritten = counters.findCounter(
                        FileSystemCounter.class.getName(), "FILE_BYTES_WRITTEN"
                ).getValue();
                long numMappers = getConf().getInt(PARAM_NUM_MAPPERS, PARAM_NUM_MAPPERS_DEFAULT);
                outputWriter.write(String.format(
                        "%d\t%d\t%d\t%d\n", numMappers, runtime, bytesRead, bytesWritten
                ));
            }
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new mgen(), args);
        System.exit(ret);
    }
}

