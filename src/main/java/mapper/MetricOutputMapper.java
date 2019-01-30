package mapper;

import counters.MetricsCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;

public class MetricOutputMapper extends Mapper<MetricsWritable, VertexWritable, MetricsWritable, IntWritable> {

    private final Logger logger = LoggerFactory.getLogger(MetricOutputMapper.class);
    private long startMillis;
    private long endMillis;

    public void map(MetricsWritable key, VertexWritable value, Context context) throws IOException, InterruptedException {
        startMillis = System.currentTimeMillis();
        if (Metric.DIT.equals(key.getMetric())) {
            int depth = value.getEdges().size();
            Text parent = value.getEdges().get(0);
            if (!parent.toString().equals("Object")) {
                depth++;
            }
            logger.info(key + ";" + value.getEdges() + ";" + value.getEdges().size());
            context.write(key, new IntWritable(depth));
        } else { // pass through
            context.write(key, value.getValue());
        }
        endMillis = System.currentTimeMillis();
        context.getCounter(MetricsCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("Metics Output Mapping Time", String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }
}