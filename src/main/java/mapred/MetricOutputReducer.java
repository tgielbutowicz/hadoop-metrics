package mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

public class MetricOutputReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, IntWritable> {

    public void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        if (Metric.DIT.equals(key.getMetric())) {
            for (VertexWritable val : values) {
                int depth = val.getEdges().size();
                Text parent = val.getEdges().get(0);
                if(!parent.toString().equals("Object")) {
                    depth++;
                }
                key.setProject(val.getEdges().toString());
                context.write(key, new IntWritable(depth));
            }
        } else { // pass through
            for (VertexWritable val : values) {
                context.write(key, val.getValue());
            }
        }
    }
}
