package reducer;

import counters.MetricsCounter;
import org.apache.hadoop.io.BooleanWritable;
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

import static counters.MetricsCounter.UPDATED;


public class GraphBuildingReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    private final Logger logger = LoggerFactory.getLogger(GraphBuildingReducer.class);
    private VertexWritable valueout = new VertexWritable();
    private long startMillis;
    private long endMillis;

    public void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
        startMillis = System.currentTimeMillis();
        if (Metric.DIT.equals(key.getMetric())) {
            List<Text> messages = new ArrayList<>();
            VertexWritable master = new VertexWritable();
            for (VertexWritable val : values) {
                if (val.isMessage()) {
                    messages.add(val.getMessage());
                } else {
                    master = val.clone();
                    master.setIsNew(new BooleanWritable(false));
                }
            }
            master.addVertex(key.getClassName());
            VertexWritable value;
            for (Text message : messages) {
                value = master.clone();
                key.setClassName(message.toString());
                context.write(key, value);
                logger.debug("Joined class {} with class {}", key.getClassName(), value.getEdges());
                context.getCounter(UPDATED).increment(value.getEdges().size());
            }
        } else { // pass through
            int sum = 0;
            for (VertexWritable val : values) {
                sum += val.getValue().get();
            }
            valueout.getValue().set(sum);
            context.write(key, valueout);
        }
        endMillis = System.currentTimeMillis();
        context.getCounter(MetricsCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("Graph Builder Reducing Time", String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }
}
