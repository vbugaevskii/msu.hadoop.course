import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
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

    private Job getJobConf(String input, String output) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(candle.class);
        job.setJobName("[HW4] Candlesticks plot");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(CandleMapper.class);
        job.setCombinerClass(CandleCombiner.class);
        job.setReducerClass(CandleReducer.class);

        job.setCombinerKeyGroupingComparatorClass(CandleGroupComparator.class);
        job.setSortComparatorClass(CandleSortComparator.class);
        job.setGroupingComparatorClass(CandleGroupComparator.class);

        job.setMapOutputKeyClass(DealKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(getConf().getInt(PARAM_NUM_REDUCERS, PARAM_NUM_REDUCERS_DEFAULT));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        setParams();
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new candle(), args);
        System.exit(ret);
    }
}
