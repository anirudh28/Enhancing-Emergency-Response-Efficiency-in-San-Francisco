import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataProfilingReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0.0;
    private long count = 0;
    private double sumOfSquares = 0.0;

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            double responseTime = Double.parseDouble(value.toString());
            min = Math.min(min, responseTime);
            max = Math.max(max, responseTime);
            sum += responseTime;
            sumOfSquares += responseTime * responseTime;
            count++;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (count > 0) {
            double mean = sum / count;
            double variance = (sumOfSquares / count) - (mean * mean);
            double stdDev = Math.sqrt(variance);

            context.write(NullWritable.get(), new Text("Min Response Time: " + min));
            context.write(NullWritable.get(), new Text("Max Response Time: " + max));
            context.write(NullWritable.get(), new Text("Average Response Time: " + mean));
            context.write(NullWritable.get(), new Text("Standard Deviation: " + stdDev));
            context.write(NullWritable.get(), new Text("Total Rows: " + count));
        }
    }
}
