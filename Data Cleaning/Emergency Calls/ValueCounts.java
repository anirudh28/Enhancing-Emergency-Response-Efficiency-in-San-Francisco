import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;
import java.util.Map;

public class ValueCounts {

    private static final Map<String, Integer> columnMap = new HashMap<>();

    static {
        columnMap.put("Call Number", 0);
        columnMap.put("Incident Number", 1);
        columnMap.put("Call Type", 2);
        columnMap.put("Call Date", 3);
        columnMap.put("Received DtTm", 4);
        columnMap.put("Response DtTm", 5);
        columnMap.put("On Scene DtTm", 6);
        columnMap.put("Transport DtTm", 7);
        columnMap.put("Hospital DtTm", 8);
        columnMap.put("Call Final Disposition", 9);
        columnMap.put("Address", 10);
        columnMap.put("City", 11);
        columnMap.put("Zipcode of Incident", 12);
        columnMap.put("Battalion", 13);
        columnMap.put("Station Area", 14);
        columnMap.put("Box", 15);
        columnMap.put("Final Priority", 16);
        columnMap.put("ALS Unit", 17);
        columnMap.put("Call Type Group", 18);
        columnMap.put("Number of Alarms", 19);
        columnMap.put("Unit Type", 20);
        columnMap.put("Neighborhooods - Analysis Boundaries", 21);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ValueCounts <input path> <output path> <column name>");
            System.exit(-1);
        }

        String columnName = args[2];
        if (!columnMap.containsKey(columnName)) {
            System.err.println("Invalid column name: " + columnName);
            System.exit(-1);
        }

        int columnIndex = columnMap.get(columnName);

        Configuration conf = new Configuration();
        conf.setInt("column.index", columnIndex);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ValueCounts.class);
        job.setJobName("Value Counts for Column: " + columnName);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ValueCountsMapper.class);
        job.setReducerClass(ValueCountsReducer.class);
	job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

