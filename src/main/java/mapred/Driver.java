package mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.eclipse.jgit.api.errors.GitAPIException;

import mapred.loc.MetricsMapper;
import utils.MetricsWritable;
import utils.RegexFilter;
import utils.WholeFileInputFormat;

public class Driver {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException, GitAPIException {

        // Git git = Git.cloneRepository()
        // .setURI( "https://github.com/eclipse/jgit.git" )
        // .setDirectory(new File("github"))
        // .call();

        Configuration metricsConf = new Configuration();
        metricsConf.set("file.pattern", ".*.java");
        Job metricsJob = Job.getInstance(metricsConf, "Calculate Metrics");

        // Set driver class
        metricsJob.setJarByClass(Driver.class);

        metricsJob.setOutputKeyClass(MetricsWritable.class);
        metricsJob.setOutputValueClass(IntWritable.class);
        // Set Input & Output Format
        metricsJob.setInputFormatClass(WholeFileInputFormat.class);
        metricsJob.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(MetricsMapper.class);
        metricsJob.setReducerClass(KeyCountReducer.class);

        // No. of reduce tasks, equals no. output file
        // metricsJob.setNumReduceTasks(1);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(metricsJob, true);
        FileInputFormat.setInputPathFilter(metricsJob, RegexFilter.class);
        FileInputFormat.addInputPath(metricsJob, new Path(args[0])); // s227
        FileOutputFormat.setOutputPath(metricsJob, new Path(args[1]));

        metricsJob.waitForCompletion(true);
    }
}