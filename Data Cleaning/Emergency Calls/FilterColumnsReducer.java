import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterColumnsReducer extends Reducer<Text, Text, Text, Text> {

    private boolean isHeaderWritten = false;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Write the header only once
        if (!isHeaderWritten) {
            String header = "Call Number,Incident Number,Call Type,Call Date,Received DtTm,Response DtTm,On Scene DtTm,Transport DtTm,Hospital DtTm,Call Final Disposition,Address,City,Zipcode of Incident,Battalion,Station Area,Box,Final Priority,ALS Unit,Call Type Group,Number of Alarms,Unit Type,Neighborhooods - Analysis Boundaries";
            context.write(null, new Text(header));
            isHeaderWritten = true;
        }

        // Write the actual data rows
        for (Text value : values) {
            context.write(null, value); // Use null key to avoid prepending a key
        }
    }
}

