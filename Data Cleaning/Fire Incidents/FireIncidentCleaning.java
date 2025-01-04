import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FireIncidentCleaning {
    public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: FireIncidentCleaning <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = Job.getInstance();
    job.setJarByClass(FireIncidentCleaning.class);
    job.setJobName("Data Cleaning");
  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(FireIncidentCleaningMapper.class);
    job.setReducerClass(FireIncidentCleaningReducer.class);
  
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
  
    job.setNumReduceTasks(1);
  
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    // return job.waitForCompletion(true);
  }
}
