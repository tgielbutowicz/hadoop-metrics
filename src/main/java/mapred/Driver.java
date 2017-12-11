package mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.eclipse.jgit.api.errors.GitAPIException;

import mapred.loc.MapLinesOfCode;
import utils.RegexFilter;
import utils.WholeFileInputFormat;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, GitAPIException {

//        Git git = Git.cloneRepository()
//                .setURI( "https://github.com/eclipse/jgit.git" )
//                .setDirectory(new File("github"))
//                .call();

        Configuration conf = new Configuration();
        conf.set("file.pattern",".*.java");
        Job job = Job.getInstance(conf, "Calculate Metrics");

        // Set driver class
//        job.setJarByClass(Driver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // Set Input & Output Format
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper & Reducer Class
        job.setMapperClass(MapLinesOfCode.class);
        job.setReducerClass(KeyCountReducer.class);

        // No. of reduce tasks, equals no. output file
//        job.setNumReduceTasks(1);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(job,true);
        FileInputFormat.setInputPathFilter(job, RegexFilter.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //s227
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}