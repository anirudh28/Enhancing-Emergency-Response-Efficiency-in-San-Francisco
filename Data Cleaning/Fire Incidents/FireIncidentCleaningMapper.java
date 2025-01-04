import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FireIncidentCleaningMapper extends Mapper <LongWritable, Text, Text, Text> {
    private static final Integer TOTAL_COLS = 66;
    private static final int[] COLUMNS_TO_DROP = {16, 17, 18, 19, 20, 21, 22, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 63};

    public static String[] parseRecord(String csvLine) {
      List<String> fields = new ArrayList<>();
      StringBuilder currentField = new StringBuilder();
      boolean withinQuotes = false;
  
      for (char c : csvLine.toCharArray()) {
        if (c == ',' && !withinQuotes) {
          fields.add(currentField.toString().trim());
          currentField.setLength(0);
        } else if (c == '\"') {
          withinQuotes = !withinQuotes;
          currentField.append(c);
        } else if (c == '\t') {
          currentField.append(' ');
        } else {
          currentField.append(c);
        }
      }
  
      // Add back the last field
      fields.add(currentField.toString().trim());
      return fields.toArray(new String[0]);
    }

    public static String[] removeFields(String[] fields, int[] indicesToRemove) {
        return IntStream.range(0, fields.length)
            .filter(index -> IntStream.of(indicesToRemove).noneMatch(i -> i == index))
            .mapToObj(i -> fields[i])
            .toArray(String[]::new);
      }

    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] fields = parseRecord(line);

    // Wrong number of columns, skip
    if (fields.length != TOTAL_COLS) {
        return;
    }

    // Remove unnecessary columns
    fields = removeFields(fields, COLUMNS_TO_DROP);
    
    // Emit the cleaned row
    context.write(new Text(String.valueOf(key)), new Text(String.join(",", fields)));
  }
}
