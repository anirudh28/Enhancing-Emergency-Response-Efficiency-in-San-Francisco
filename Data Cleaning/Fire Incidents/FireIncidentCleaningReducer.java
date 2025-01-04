import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FireIncidentCleaningReducer 
    extends Reducer<Text, Text, NullWritable, Text> {
        private static final Integer NUM_COLS = 30;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            // Emit key as null and values as the cleaned CSV lines
            for (Text val : values) {
                context.write(NullWritable.get(), val);
            }
        }
}
