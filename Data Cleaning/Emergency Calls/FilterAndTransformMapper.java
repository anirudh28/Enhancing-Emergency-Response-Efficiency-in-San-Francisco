import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterAndTransformMapper extends Mapper<Object, Text, Text, Text> {

    private static final SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy");
    private static final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");
    private static final String comparisonDate1 = "20140101";
    private static final String comparisonDate2 = "20241031";

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Skip the header row
        if (line.contains("Call Number") && line.contains("Incident Number")) {
            context.write(new Text("HEADER"), value); // Emit the header for later use
            return;
        }

        String[] columns = line.split(",", -1); // Split the line into columns
        if (columns.length > 4) { // Ensure `Call Date` exists
            String callDate = columns[4].trim();
            
            if (!callDate.isEmpty()) { // Drop rows with missing `Call Date`
                try {
                    // Parse and reformat the `Call Date`
                    Date date = inputFormat.parse(callDate);
                    String formattedDate = outputFormat.format(date);

                    // Filter rows based on the `Call Date` range
                    if (formattedDate.compareTo(comparisonDate1) >= 0 && formattedDate.compareTo(comparisonDate2) <= 0) {
                        // Replace the original `Call Date` with the reformatted one
                        columns[4] = formattedDate;
                        context.write(new Text("FilteredRecord"), new Text(String.join(",", columns)));
                    }
                } catch (ParseException e) {
                    // Handle invalid date format (skip the row)
                }
            }
        }
    }
}

