import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

import javax.naming.Context;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FireIncidentDateFiltering {

    public static class FireIncidentDateFilteringMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        private static final Date startDate;
        private static final Date endDate;
        private boolean headerWritten = false;

        static {
            try {
                startDate = dateFormat.parse("2014/01/01");
                endDate = dateFormat.parse("2024/10/31");
            } catch (ParseException e) {
                throw new RuntimeException("Error parsing date", e);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Write header row
            if (!headerWritten) {
                context.write(NullWritable.get(), value);
                headerWritten = true;
                return;
            }

            // Process data rows
            String incidentDateStr = fields[4];
            try {
                Date incidentDate = dateFormat.parse(incidentDateStr);
                if (incidentDate.compareTo(startDate) >= 0 && incidentDate.compareTo(endDate) <= 0) {
                    context.write(NullWritable.get(), value);
                }
            } catch (ParseException e) {
                // Log or handle parsing errors
                System.err.println("Error parsing date: " + incidentDateStr);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fire Incident Filtering");
        job.setJarByClass(FireIncidentDateFiltering.class);
        job.setMapperClass(FireIncidentDateFilteringMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0); // Map-only job

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}