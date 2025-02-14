import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataProfiling {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ProfilingDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Profiling Statistics");
        job.setJarByClass(DataProfiling.class);

        job.setMapperClass(DataProfilingMapper.class);
        job.setReducerClass(DataProfilingReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path from cleaned data (output of Job 1)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path for profiling statistics

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
