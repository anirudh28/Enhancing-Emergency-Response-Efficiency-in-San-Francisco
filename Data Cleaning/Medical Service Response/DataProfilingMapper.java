import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataProfilingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields.length < 7) return; // Ensure cleaned data format

        try {
            String responseTime = fields[6];
            context.write(NullWritable.get(), new Text(responseTime));
        } catch (NumberFormatException e) {
            // Skip invalid data
        }
    }
}
