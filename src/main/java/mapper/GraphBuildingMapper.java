package mapper;

import com.google.common.collect.Iterators;
import counters.MetricsCounter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;

public class GraphBuildingMapper extends Mapper<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    private VertexWritable message = new VertexWritable();
    private long startMillis;
    private long endMillis;

    public void map(MetricsWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
        startMillis = System.currentTimeMillis();
        if (Metric.DIT.equals(key.getMetric())) {
            context.write(key, value);
            message.setMessage(key.getClassName());
            key.setClassName(Iterators.getLast(value.getEdges().iterator()).toString());
            context.write(key, message);
        } else {
            context.write(key, value);
        }
        endMillis = System.currentTimeMillis();
        context.getCounter(MetricsCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("Graph Builder Mapping Time",String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }
}