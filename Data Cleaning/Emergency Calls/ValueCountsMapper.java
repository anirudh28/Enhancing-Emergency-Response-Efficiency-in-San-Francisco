import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ValueCountsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private int columnIndex = -1;
    private boolean isHeader = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Retrieve the column index from configuration
        columnIndex = context.getConfiguration().getInt("column.index", -1);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.contains("Call Number") && line.contains("Incident Number")) {
	    isHeader = false;
            return;
        }

	String[] columns = line.split(",", -1);

        if (columnIndex >= 0 && columnIndex < columns.length) {
            String columnValue = columns[columnIndex].trim();
            if (!columnValue.isEmpty()) { // Ignore empty values
                context.write(new Text(columnValue), one);
            }
        }
    }
}

