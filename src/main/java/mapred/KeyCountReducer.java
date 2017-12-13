package mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.MetricsWritable;

public class KeyCountReducer extends Reducer<MetricsWritable, IntWritable, MetricsWritable, IntWritable> {

    public void reduce(MetricsWritable key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}