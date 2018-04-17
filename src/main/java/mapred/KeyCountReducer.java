package mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MetricsWritable;
import utils.VertexWritable;

public class KeyCountReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, IntWritable> {

    public void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        int sum = 0;
        for (VertexWritable val : values) {
            sum += val.getVertex().get();
        }
        context.write(key, new IntWritable(sum));
    }
}