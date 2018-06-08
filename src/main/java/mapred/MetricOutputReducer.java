package mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.MetricsWritable;
import utils.VertexWritable;

public class MetricOutputReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, IntWritable> {

    public void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        if (key.getMetric().toString().equals("DIT")) {
            for (VertexWritable val : values) {
                //todo when the top class isnt Object ten +1
                context.write(key, new IntWritable(val.getEdges().size()));
            }
        } else { // pass through
            for (VertexWritable val : values) {
                context.write(key, val.getValue());
            }
        }
    }
}
