import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class candle extends Configured implements Tool {
    public static final String PARAM_CANDLE_WIDTH = "candle.width";
    public static final Long PARAM_CANDLE_WIDTH_DEFAULT = 300000L;

    public static final String PARAM_CANDLE_SECURITY = "candle.securities";
    public static final String PARAM_CANDLE_SECURITY_DEFAULT = ".*";

    public static final String PARAM_DATE_FROM = "candle.date.from";
    public static final String PARAM_DATE_FROM_DEFAULT = "19000101";

    public static final String PARAM_DATE_TO = "candle.date.to";
    public static final String PARAM_DATE_TO_DEFAULT = "20200101";

    public static final String PARAM_TIME_FROM = "candle.time.from";
    public static final String PARAM_TIME_FROM_DEFAULT = "1000";

    public static final String PARAM_TIME_TO = "candle.time.to";
    public static final String PARAM_TIME_TO_DEFAULT = "1800";

    public static final String PARAM_NUM_REDUCERS = "candle.num.reducers";
    public static final int PARAM_NUM_REDUCERS_DEFAULT = 1;

    private void setNewValueForTime(String name, String defaultValue) throws ParseException {
        SimpleDateFormat formatParse  = new SimpleDateFormat("HHmm");
        SimpleDateFormat formatFormat = new SimpleDateFormat("HHmmssSSS");

        Configuration conf = getConf();

        long value = formatParse.parse(conf.get(name, defaultValue)).getTime();
        long valueRemain = value % PARAM_CANDLE_WIDTH_DEFAULT;
        if (valueRemain != 0) {
            value -= valueRemain;
        }
        conf.set(name, formatFormat.format(new Date(value)));
    }

    private void setParams() throws IOException {
        try {
            setNewValueForTime(PARAM_TIME_FROM, PARAM_TIME_FROM_DEFAULT);
            setNewValueForTime(PARAM_TIME_TO, PARAM_TIME_TO_DEFAULT);
        } catch (ParseException e) {
            throw new NumberFormatException("Wrong date format!");
        }
    }

    private Job getSampleJobConf(String input, String output) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(candle.class);
        job.setJobName("[HW4] Partitions creator");

        FileInputFormat.addInputPath(job, new Path(input));
        SequenceFileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(CandleMapper.class);
        job.setMapOutputKeyClass(DealKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setSortComparatorClass(CandleSortComparator1.class);

        job.setReducerClass(Reducer.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(DealKey.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(0);

        return job;
    }

    private Job getCandleJobConf(String input, String partitions, String output) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(candle.class);
        job.setJobName("[HW4] Candlesticks plot");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(Mapper.class);
        job.setReducerClass(CandleReducer.class);

        job.setMapOutputKeyClass(DealKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(CandleCombiner.class);
        job.setCombinerKeyGroupingComparatorClass(CandleGroupComparator.class);

        job.setSortComparatorClass(CandleSortComparator2.class);
        job.setGroupingComparatorClass(CandleGroupComparator.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(getConf().getInt(PARAM_NUM_REDUCERS, PARAM_NUM_REDUCERS_DEFAULT));

        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(
                job.getConfiguration(), new Path(partitions));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, input);

        InputSampler.writePartitionFile(job,
                new InputSampler.RandomSampler(.01, 10000));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        setParams();

        String input = args[0], stage = args[1] + "_stage",
                partitions = args[1]+ "_partitions.lst", output = args[1];

        Job sampleJob = getSampleJobConf(input, stage);
        int ret = sampleJob.waitForCompletion(true) ? 0 : 1;
        if (ret == 0) {
            Job candleJob = getCandleJobConf(stage, partitions, output);
            ret = candleJob.waitForCompletion(true) ? 0 : 1;
            FileSystem.get(new Configuration()).delete(new Path(partitions), false);
        }

        FileSystem.get(new Configuration()).delete(new Path(stage), true);

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new candle(), args);
        System.exit(ret);
    }
}
