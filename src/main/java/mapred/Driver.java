package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.*;

import java.io.IOException;

public class Driver {

    public static final int INTERATIONS_LIMIT = 16;
    public static final int REDUCE_TASKS = 4;

    public enum UpdateCounter {
        UPDATED
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        String workingDir = args[0];

        int depth = 1;
        Path in = new Path(workingDir);
        Path out = new Path(args[1] + depth);

        Configuration metricsConf = new Configuration();
        metricsConf.set("recursion.depth", depth + "");
        metricsConf.set("file.pattern", ".java");
        Job metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Read Files");

        // Set driver class
        metricsJob.setJarByClass(Driver.class);

        metricsJob.setOutputKeyClass(MetricsWritable.class);
        metricsJob.setOutputValueClass(VertexWritable.class);
        // Set Input & Output Format
        metricsJob.setInputFormatClass(CombineSourceFileInputFormat.class);
        metricsJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(FileMapper.class);
        metricsJob.setReducerClass(KeyCountReducer.class);
        metricsJob.setNumReduceTasks(REDUCE_TASKS);

        // No. of reduce tasks, equals no. output file
        // metricsJob.setNumReduceTasks(1);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(metricsJob, true);
        FileInputFormat.setInputPathFilter(metricsJob, RegexFilter.class);
        FileInputFormat.addInputPath(metricsJob, in); // s227
        FileOutputFormat.setOutputPath(metricsJob, out);

        metricsJob.waitForCompletion(true);

        long updated_prev = 0;
        long updated = metricsJob.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
        depth++;
        while (updated_prev != updated && depth < INTERATIONS_LIMIT) {
            metricsConf.set("recursion.depth", depth + "");
            metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Build Graph" + depth);

            // Set driver class
            metricsJob.setJarByClass(Driver.class);

            // Set Mapper & Reducer Class
            metricsJob.setMapperClass(GraphBuildingMapper.class);
            metricsJob.setReducerClass(GraphBuildingReducer.class);
            metricsJob.setNumReduceTasks(REDUCE_TASKS);

            in = out;
            out = new Path(args[1] + depth);
            FileInputFormat.addInputPath(metricsJob, in);
            FileOutputFormat.setOutputPath(metricsJob, out);

            metricsJob.setInputFormatClass(SequenceFileInputFormat.class);
            metricsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            metricsJob.setOutputKeyClass(MetricsWritable.class);
            metricsJob.setOutputValueClass(VertexWritable.class);

            metricsJob.waitForCompletion(true);
            updated_prev = updated;
            updated = metricsJob.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
            depth++;
        }

        metricsConf.set("recursion.depth", depth + "");
        metricsConf.set("mapred.textoutputformat.separator", ";");
        metricsJob = Job.getInstance(metricsConf, depth + " : Calculate Metrics - Merger Results");

        // Set driver class
        metricsJob.setJarByClass(Driver.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(MetricOutputMapper.class);
        metricsJob.setReducerClass(MetricOutputReducer.class);

        in = out;
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