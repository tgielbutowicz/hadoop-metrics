package mapred;

import mapper.FileMapper;
import mapper.RepositoryDownloadMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reducer.KeyCountReducer;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;

import static utils.FileSize.BYTES_IN_16_MB;

public class TempDriver {

    private static final int REDUCE_TASKS = 1;
    private static final Logger logger = LoggerFactory.getLogger(TempDriver.class);

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        String inputDir = args[0];
        String outputDir = args[1];

        int depth = 1;
        Path in = new Path(inputDir);
        Path out = new Path(outputDir + depth);
        Path working = new Path(".idea/source");

        Configuration metricsConf = new Configuration();
        metricsConf.set("working.path", working.toString());
        Job metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Download Files");

        // Set driver class
        metricsJob.setJarByClass(TempDriver.class);

        metricsJob.setOutputKeyClass(Text.class);
        metricsJob.setOutputValueClass(Text.class);
        // Set Input & Output Format
        metricsJob.setInputFormatClass(TextInputFormat.class);
        metricsJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(RepositoryDownloadMapper.class);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(metricsJob, true);
        FileInputFormat.addInputPath(metricsJob, in); // s227
        FileOutputFormat.setOutputPath(metricsJob, out);

        long startTime = System.currentTimeMillis();
        metricsJob.waitForCompletion(true);
        depth++;
        in = out;
        out = new Path(outputDir + depth);


        metricsJob = Job.getInstance(metricsConf, "Calculate Metrics - Read Files");
        metricsConf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(BYTES_IN_16_MB.getBytes()));

        // Set driver class
        metricsJob.setJarByClass(TempDriver.class);

        metricsJob.setOutputKeyClass(MetricsWritable.class);
        metricsJob.setOutputValueClass(VertexWritable.class);
        // Set Input & Output Format
        metricsJob.setInputFormatClass(SequenceFileInputFormat.class);
        metricsJob.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper & Reducer Class
        metricsJob.setMapperClass(FileMapper.class);
        metricsJob.setReducerClass(KeyCountReducer.class);

        // No. of reduce tasks, equals no. output file
        metricsJob.setNumReduceTasks(4);

        // HDFS input and output path
        FileInputFormat.setInputDirRecursive(metricsJob, true);
//        FileInputFormat.setInputPathFilter(metricsJob, RegexFilter.class);
        FileInputFormat.addInputPath(metricsJob, in); // s227
        FileOutputFormat.setOutputPath(metricsJob, out);

        metricsJob.waitForCompletion(true);
        long stopTime = System.currentTimeMillis();
        logger.info("Job running time: {}", stopTime - startTime);
    }
}