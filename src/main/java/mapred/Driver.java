package mapred;

import counters.MetricsCounter;
import mapper.GraphBuildingMapper;
import mapper.MetricOutputMapper;
import mapper.RepositoryMapper;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

    private static final int INTERATIONS_LIMIT = 60;
    private static final int REDUCE_TASKS = 1;
    private static final Logger logger = LoggerFactory.getLogger(Driver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Options options = getOptions();

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Hadoop Metrics", options);

            System.exit(1);
        }

        String inputDir = cmd.getOptionValue("input");
        String outputDir = cmd.getOptionValue("output");

        int depth = 1;
        Path in = new Path(inputDir);
        Path out = new Path(outputDir + depth);
        Path working = new Path(".idea/source");

        Configuration metricsConf = getConfiguration();
        metricsConf.set("working.path", working.toString());
        metricsConf.set("recursion.depth", depth + "");

        Job metricsJob = getRepositoryMapperJob(metricsConf, in, out);
        long startTime = System.currentTimeMillis();
        metricsJob.waitForCompletion(true);

        long updated_prev = 0;
        long updated = metricsJob.getCounters().findCounter(MetricsCounter.UPDATED).getValue();
        depth++;
        while (updated_prev != updated && depth < INTERATIONS_LIMIT) {
            metricsConf.set("recursion.depth", depth + "");
            metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Build Graph" + depth);

            // Set driver class
            metricsJob.setJarByClass(Driver.class);

            // Set Mapper & Reducer Class
            metricsJob.setMapperClass(GraphBuildingMapper.class);
            metricsJob.setReducerClass(GraphBuildingReducer.class);

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
            updated = metricsJob.getCounters().findCounter(MetricsCounter.UPDATED).getValue();
            depth++;
        }
        logger.debug("Loop finished. Updated {}. Updated previous {}. Iteration {}.", updated, updated_prev, depth);

        metricsConf.set("recursion.depth", depth + "");
        metricsJob = Job.getInstance(metricsConf, depth + " : Calculate Metrics - Merger Results");

        // Set driver class
        metricsJob.setJarByClass(Driver.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(MetricOutputMapper.class);
        metricsJob.setReducerClass(MetricOutputReducer.class);
        metricsJob.setNumReduceTasks(1);

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
        logger.info("Job running time: {}", stopTime - startTime);
    }

    private static Job getRepositoryMapperJob(Configuration metricsConf, Path in, Path out) throws IOException {
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

        // No. of reduce tasks, equals no. output file
        metricsJob.setNumReduceTasks(REDUCE_TASKS);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(metricsJob, true);
        FileInputFormat.addInputPath(metricsJob, in); // s227
        FileOutputFormat.setOutputPath(metricsJob, out);

        return metricsJob;
    }

    private static Options getOptions() {
        Options options = new Options();

        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("o", "output", true, "output file");
        output.setRequired(true);
        options.addOption(output);
        return options;
    }

    private static Configuration getConfiguration() {
        Configuration metricsConf = new Configuration();
        metricsConf.set("mapreduce.task.timeout", "1800000");
        metricsConf.set("mapreduce.input.fileinputformat.split.maxsize", "1000000");
        metricsConf.set("mapreduce.task.profile", "true");
        metricsConf.set("mapreduce.task.profile.maps", "100");
        metricsConf.set("mapreduce.task.profile.reduces", "100");
        return metricsConf;
    }

}