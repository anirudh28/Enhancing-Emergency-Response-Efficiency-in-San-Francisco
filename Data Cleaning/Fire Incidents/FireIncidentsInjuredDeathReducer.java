import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import javax.naming.Context;

import java.io.IOException;

public class FireIncidentsInjuredDeathReducer
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        long count = 0;
        long sum = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        for (Text val : values) {
            int value = Integer.parseInt(val.toString());
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        double avg = count > 0 ? (double) sum / count : 0;

        String result = String.format("Count: %d, Sum: %d, Avg: %.2f, Min: %d, Max: %d", count, sum, avg, min, max);
        context.write(key, new Text(result));
    }
}