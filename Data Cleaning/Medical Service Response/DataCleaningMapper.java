import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static final String START_DATE = "2014-01-01";
    private static final String END_DATE = "2024-10-31";
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static enum Counters {
        TOTAL_ROWS, VALID_ROWS, INVALID_ROWS, OUT_OF_RANGE_ROWS
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        context.getCounter(Counters.TOTAL_ROWS).increment(1);

        String[] fields = line.split(",");
        if (fields.length < 9) {
            context.getCounter(Counters.INVALID_ROWS).increment(1);
            return;
        }

        try {
            // Parse the date
            Date callDate = dateFormat.parse(fields[3]); // Assuming 'call_date' is the 4th column
            Date startDate = dateFormat.parse(START_DATE);
            Date endDate = dateFormat.parse(END_DATE);

            if (callDate.before(startDate) || callDate.after(endDate)) {
                context.getCounter(Counters.OUT_OF_RANGE_ROWS).increment(1);
                return;
            }

            // Validate response_time_min
            double responseTime = Double.parseDouble(fields[7]); // Assuming 'response_time_min' is the 8th column
            if (responseTime <= 0) {
                context.getCounter(Counters.INVALID_ROWS).increment(1);
                return;
            }

            // Write cleaned row to output
            String cleanedRow = String.join(",", fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]); // Exclude unnecessary columns
            context.write(NullWritable.get(), new Text(cleanedRow));
            context.getCounter(Counters.VALID_ROWS).increment(1);
        } catch (ParseException | NumberFormatException e) {
            context.getCounter(Counters.INVALID_ROWS).increment(1);
        }
    }
}
