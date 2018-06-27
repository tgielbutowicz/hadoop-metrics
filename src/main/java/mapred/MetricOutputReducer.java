package mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.MetricsWritable;

import java.io.IOException;

public class MetricOutputReducer extends Reducer<MetricsWritable, IntWritable, MetricsWritable, IntWritable> {

    public void reduce(MetricsWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //todo combine metrics to one line
        for (IntWritable val : values) {
            context.write(key, val);
        }
    }
}
