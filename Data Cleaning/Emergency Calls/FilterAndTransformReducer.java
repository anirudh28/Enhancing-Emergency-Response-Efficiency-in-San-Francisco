import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterAndTransformReducer extends Reducer<Text, Text, Text, Text> {

    private boolean isHeaderWritten = false;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // Write the header row once
            if (key.toString().equals("HEADER") && !isHeaderWritten) {
                context.write(null, value);
                isHeaderWritten = true;
            } else if (!key.toString().equals("HEADER")) {
                // Write the filtered data rows
                context.write(null, value);
            }
        }
    }
}

