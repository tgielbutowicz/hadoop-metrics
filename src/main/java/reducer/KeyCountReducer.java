package reducer;

import counters.MetricsCounter;
import mapper.MetricOutputMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KeyCountReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    private final Logger logger = LoggerFactory.getLogger(KeyCountReducer.class);
    private long startMillis;
    private long endMillis;

    @Override
    protected void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        startMillis = System.currentTimeMillis();
        int sum = 0;
        if (Metric.DIT.equals(key.getMetric())) {
            List<Text> messages = new ArrayList<>();
            VertexWritable master = new VertexWritable();
            for (VertexWritable val : values) {
                if (val.isMessage()) {
                    messages.add(val.getMessage());
                } else {
                    master = val.clone();
                }
            }
            master.addVertex(key.getClassName());
            VertexWritable value;
            for (Text message : messages) {
                value = master.clone();
                key.setClassName(message.toString());
                context.write(key, value);
                logger.debug("Joined class {} with class {}", key.getClassName(), value.getEdges());
                context.getCounter(MetricsCounter.UPDATED).increment(value.getEdges().size());
            }
        } else {
            for (VertexWritable val : values) {
                sum += val.getValue().get();
            }
            VertexWritable valueout = new VertexWritable();
            valueout.setValue(new IntWritable(sum));
            context.write(key, valueout);
        }
        endMillis = System.currentTimeMillis();
        context.getCounter(MetricsCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("Key Count Reducing Time", String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }
}