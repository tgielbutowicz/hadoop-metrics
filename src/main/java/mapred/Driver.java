package mapred;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.RepositoryCache;
import org.eclipse.jgit.util.FS;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import utils.MetricsWritable;
import utils.RegexFilter;
import utils.VertexWritable;
import utils.WholeFileInputFormat;

public class Driver {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException, GitAPIException {

        String workingDir = "c:/temp/github";
        List<String> repositoriesUri = Lists.newArrayList("https://github.com/eclipse/jgit.git",
                "https://github.com/tgielbutowicz/hadoop-metrics.git");
        for (String uri : repositoriesUri) {
            String project = Iterables.getLast(Lists.newArrayList(uri.split("/"))).replace(".git", "");
            File destination = new File(workingDir + "/" + project);
            if (!Files.exists(destination.toPath())
                    && !RepositoryCache.FileKey.isGitRepository(destination, FS.DETECTED)) {
                Git.cloneRepository()
                        .setURI(uri)
                        .setDirectory(destination)
                        .call();
            } else {
                Git.open(destination).pull().call();
            }
        }

        int depth = 1;
        Path in = new Path(workingDir);
        Path out = new Path(args[1] + depth);

        Configuration metricsConf = new Configuration();
        metricsConf.set("recursion.depth", depth + "");
        metricsConf.set("file.pattern", ".*.java");
        Job metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Read Files");

        // Set driver class
        metricsJob.setJarByClass(Driver.class);

        metricsJob.setOutputKeyClass(MetricsWritable.class);
        metricsJob.setOutputValueClass(VertexWritable.class);
        // Set Input & Output Format
        metricsJob.setInputFormatClass(WholeFileInputFormat.class);
        metricsJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(FileMapper.class);
        metricsJob.setReducerClass(KeyCountReducer.class);

        // No. of reduce tasks, equals no. output file
        // metricsJob.setNumReduceTasks(1);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(metricsJob, true);
        FileInputFormat.setInputPathFilter(metricsJob, RegexFilter.class);
        FileInputFormat.addInputPath(metricsJob, in); // s227
        FileOutputFormat.setOutputPath(metricsJob, out);

        metricsJob.waitForCompletion(true);

        long counter = metricsJob.getCounters().findCounter(KeyCountReducer.UpdateCounter.UPDATED).getValue();
        depth++;
        in = out;
        while (counter > 0) {
            metricsConf.set("recursion.depth", depth + "");
            metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Build Graph" + depth);

            // Set Mapper & Reducer Class
            metricsJob.setMapperClass(GraphBuildingMapper.class);
            metricsJob.setReducerClass(GraphBuildingReducer.class);

            out = new Path(args[1] + depth);

            FileInputFormat.addInputPath(metricsJob, in);

            FileOutputFormat.setOutputPath(metricsJob, out);
            metricsJob.setInputFormatClass(SequenceFileInputFormat.class);
            metricsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            metricsJob.setOutputKeyClass(MetricsWritable.class);
            metricsJob.setOutputValueClass(VertexWritable.class);

            metricsJob.waitForCompletion(true);
            depth++;
            counter = metricsJob.getCounters().findCounter(KeyCountReducer.UpdateCounter.UPDATED).getValue();
        }

        metricsConf.set("recursion.depth", depth + "");
        metricsJob = Job.getInstance(metricsConf, depth + " : Calculate Metrics - Merger Results");

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(GraphBuildingMapper.class);
        metricsJob.setReducerClass(GraphBuildingReducer.class);

        out = new Path(args[1] + depth);

        FileInputFormat.addInputPath(metricsJob, in);

        FileOutputFormat.setOutputPath(metricsJob, out);
        metricsJob.setInputFormatClass(SequenceFileInputFormat.class);
        metricsJob.setOutputFormatClass(TextOutputFormat.class);
        metricsJob.setOutputKeyClass(MetricsWritable.class);
        metricsJob.setOutputValueClass(VertexWritable.class);

        metricsJob.waitForCompletion(true);

    }
}