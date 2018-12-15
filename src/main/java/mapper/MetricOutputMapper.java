package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;

public class MetricOutputMapper extends Mapper<MetricsWritable, VertexWritable, MetricsWritable, IntWritable> {

    public void map(MetricsWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
        if (Metric.DIT.equals(key.getMetric())) {
                int depth = value.getEdges().size();
                Text parent = value.getEdges().get(0);
                if(!parent.toString().equals("Object")) {
                    depth++;
                }
                key.setProject(value.getEdges().toString());
                context.write(key, new IntWritable(depth));
        } else { // pass through
                context.write(key, value.getValue());
        }
    }
}