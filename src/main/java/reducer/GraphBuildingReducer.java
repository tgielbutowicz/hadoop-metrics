package reducer;

import counters.MapperCounter;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static counters.ReducerCounter.UPDATED;

public class GraphBuildingReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

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
                context.getCounter(UPDATED).increment(value.getEdges().size());
            }
        } else { // pass through
            for (VertexWritable val : values) {
                context.write(key, val);
            }
        }
        endMillis = System.currentTimeMillis();
        context.getCounter(MapperCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("Graph Builder Reducing Time", String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }
}
