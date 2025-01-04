import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import javax.naming.Context;

public class FireIncidentTimeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<Long> times = new ArrayList<>();
        
        for (Text val : values) {
            times.add(Long.parseLong(val.toString()));
        }

        Collections.sort(times); // sort in ascending order

        long count = times.size();
        long sum = times.stream().mapToLong(Long::longValue).sum(); 
        long min = times.get(0);
        long max = times.get((int)count - 1);

        double mean = (double) sum / count;

        double stdDev = Math.sqrt(times.stream().mapToDouble(t -> Math.pow(t - mean, 2)).sum() / count);

        double median;
        if (count % 2 == 0) {
            median = (times.get((int)(count / 2 - 1)) + times.get((int)(count / 2))) / 2.0;
        } else {
            median = times.get((int)(count / 2));
        }

        String result = String.format("Mean: %.2f, Std Dev: %.2f, Min: %d, Max: %d, Median: %.2f", mean, stdDev, min, max, median);

        context.write(key, new Text(result));
    }
}