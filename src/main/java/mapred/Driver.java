package mapred;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.RepositoryCache;
import org.eclipse.jgit.util.FS;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import utils.MetricsWritable;
import utils.RegexFilter;
import utils.WholeFileInputFormat;

public class Driver {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException, GitAPIException {

        String workingDir = "github";
        List<String> repositoriesUri = Lists.newArrayList("https://github.com/eclipse/jgit.git",
                "https://github.com/tgielbutowicz/hadoop-metrics.git");
        for (String uri : repositoriesUri) {
            String project = Iterables.getLast(Lists.newArrayList(uri.split("/"))).replace(".git","");
            File destination = new File(workingDir+"/"+project);
            if (!RepositoryCache.FileKey.isGitRepository(destination, FS.DETECTED)) {
                Git.cloneRepository()
                        .setURI(uri)
                        .setDirectory(destination)
                        .call();
            }
        }

        Configuration metricsConf = new Configuration();
        metricsConf.set("mapred.textoutputformat.separatorText", ",");
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
        FileInputFormat.addInputPath(metricsJob, new Path(workingDir)); // s227
        FileOutputFormat.setOutputPath(metricsJob, new Path(args[1]));

        metricsJob.waitForCompletion(true);
    }
}