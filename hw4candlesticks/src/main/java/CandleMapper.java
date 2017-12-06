import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class CandleMapper extends Mapper<LongWritable, Text, DealKey, DoubleWritable> {
    private final static int dataPrefixLen = "yyyyMMdd".length();
    private final static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private String dateFrom, dateTo, timeFrom, timeTo;
    private long candleWidth;
    private Pattern candleSecurity;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        dateFrom = conf.get(candle.PARAM_DATE_FROM, candle.PARAM_DATE_FROM_DEFAULT);
        timeFrom = conf.get(candle.PARAM_TIME_FROM, candle.PARAM_TIME_FROM_DEFAULT);

        dateTo = conf.get(candle.PARAM_DATE_TO, candle.PARAM_DATE_TO_DEFAULT);
        timeTo = conf.get(candle.PARAM_TIME_TO, candle.PARAM_TIME_TO_DEFAULT);

        candleWidth = conf.getLong(candle.PARAM_CANDLE_WIDTH, candle.PARAM_CANDLE_WIDTH_DEFAULT);
        candleSecurity = Pattern.compile(conf.get(
                candle.PARAM_CANDLE_SECURITY,
                candle.PARAM_CANDLE_SECURITY_DEFAULT
        ));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.charAt(0) == '#') {
            // skip the header
            return;
        }

        String[] values = line.split(",");
        String symbol = values[0];

        Date moment;
        String momentDate, momentTime;
        try {
            moment = format.parse(values[2]);
            momentDate = values[2].substring(0, dataPrefixLen);
            momentTime = values[2].substring(dataPrefixLen);
        } catch (ParseException e) {
            throw new NumberFormatException("Wrong date format!");
        }

        Long id = Long.valueOf(values[3]);
        Double price = Double.valueOf(values[4]);

        if (candleSecurity.matcher(symbol).find() &&
                dateFrom.compareTo(momentDate) <= 0 && dateTo.compareTo(momentDate) > 0 &&
                timeFrom.compareTo(momentTime) <= 0 && timeTo.compareTo(momentTime) > 0) {
            long candleStarts = moment.getTime() - moment.getTime() % candleWidth;
            context.write(
                    new DealKey(symbol, candleStarts, moment.getTime(), id),
                    new DoubleWritable(price)
            );
        }
    }
}
