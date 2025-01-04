import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCleaning {
    public static void main(String[] args) throws Exception {
        // Job 1: Data Cleaning
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Data Cleaning");
        job1.setJarByClass(DataCleaning.class);
        job1.setMapperClass(DataCleaningMapper.class);
        job1.setReducerClass(DataCleaningReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

    }
}
