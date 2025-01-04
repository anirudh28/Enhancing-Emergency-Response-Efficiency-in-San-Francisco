import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterColumnsMapper extends Mapper<Object, Text, Text, Text> {

    private static final int[] REQUIRED_COLUMNS = {0, 2, 3, 4, 6, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 23, 24, 25, 26, 27, 31};

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Skip the header row
        if (line.contains("Call Number") && line.contains("Incident Number")) {
            return;
        }

        String[] columns = line.split(",", -1); // Preserve empty columns
        StringBuilder output = new StringBuilder();

        for (int index : REQUIRED_COLUMNS) {
            if (index < columns.length) {
                output.append(columns[index].trim()).append(",");
            } else {
                output.append(",");
            }
        }

        // Remove trailing comma and write output
        if (output.length() > 0) {
            output.setLength(output.length() - 1);
        }

        context.write(new Text("FilteredRecord"), new Text(output.toString()));
    }
}

