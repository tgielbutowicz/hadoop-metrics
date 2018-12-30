package mapred;

import mapper.GraphBuildingMapper;
import mapper.MetricOutputMapper;
import mapper.RepositoryMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reducer.GraphBuildingReducer;
import reducer.KeyCountReducer;
import reducer.MetricOutputReducer;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;

public class Driver {

    private static final int INTERATIONS_LIMIT = 16;
    private static final int REDUCE_TASKS = 1;
    private static final Logger logger = LoggerFactory.getLogger(Driver.class);

    public enum UpdateCounter {
        UPDATED
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        String workingDir = args[0];
        String outputDir = args[1];

        int depth = 1;
        Path in = new Path(workingDir);
        Path out = new Path(outputDir + depth);

        Configuration metricsConf = new Configuration();
        metricsConf.set("recursion.depth", depth + "");
        Job metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Read Files");

        // Set driver class
        metricsJob.setJarByClass(Driver.class);

        metricsJob.setOutputKeyClass(MetricsWritable.class);
        metricsJob.setOutputValueClass(VertexWritable.class);
        // Set Input & Output Format
        metricsJob.setInputFormatClass(TextInputFormat.class);
        metricsJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(RepositoryMapper.class);
        metricsJob.setReducerClass(KeyCountReducer.class);
        metricsJob.setNumReduceTasks(REDUCE_TASKS);

        // No. of reduce tasks, equals no. output file
        // metricsJob.setNumReduceTasks(1);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(metricsJob, true);
//        FileInputFormat.setInputPathFilter(metricsJob, RegexFilter.class);
        FileInputFormat.addInputPath(metricsJob, in); // s227
        FileOutputFormat.setOutputPath(metricsJob, out);

        long startTime = System.currentTimeMillis();
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
            out = new Path(outputDir + depth);
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
        out = new Path(outputDir + depth);
        FileInputFormat.addInputPath(metricsJob, in);
        FileOutputFormat.setOutputPath(metricsJob, out);

        metricsJob.setInputFormatClass(SequenceFileInputFormat.class);
        metricsJob.setOutputFormatClass(TextOutputFormat.class);
        metricsJob.setOutputKeyClass(MetricsWritable.class);
        metricsJob.setOutputValueClass(IntWritable.class);

        metricsJob.waitForCompletion(true);
        long stopTime = System.currentTimeMillis();
        logger.debug("Job running time: {}", stopTime - startTime);
    }
}