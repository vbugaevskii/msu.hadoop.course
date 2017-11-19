import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GeneratorMapper extends Mapper<MatrixCoords, FloatWritable, NullWritable, Text> {
    private static String outputFormat;
    private static Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        Configuration conf = context.getConfiguration();

        String tag = conf.get(mgen.PARAM_TAG);
        if (tag != null) {
            builder.append(tag);
            builder.append("\t");
        }
        builder.append("%s\t");  // for key

        String floatFormat = conf.get(mgen.PARAM_FLOAT_FORMAT, mgen.PARAM_FLOAT_FROMAT_DEFAULT);
        builder.append(floatFormat);

        outputFormat = builder.toString();
    }

    @Override
    protected void map(MatrixCoords key, FloatWritable value, Context context)
            throws IOException, InterruptedException {
        outputValue.set(String.format(outputFormat, key.toString(), value.get()));
        context.write(NullWritable.get(), outputValue);
    }
}
