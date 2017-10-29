package mapred.dit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.WholeFileInputFormat;

public class DepthOfInheritanceDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate Metrics");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Set Input & Output Format
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper & Reducer Class
        job.setMapperClass(MapDepthOfInheritanceTree.class);
        job.setReducerClass(InheritanceReducer.class);

        // No. of reduce tasks, equals no. output file
        // job.setNumReduceTasks(1);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0])); // s227
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}