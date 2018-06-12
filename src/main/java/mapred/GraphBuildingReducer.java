package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapred.Driver.UpdateCounter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

public class GraphBuildingReducer extends Reducer<MetricsWritable, VertexWritable, MetricsWritable, VertexWritable> {

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    public void reduce(MetricsWritable key, Iterable<VertexWritable> values, Context context) throws IOException,
            InterruptedException {
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
            master.addVertex(key.getFile());
            VertexWritable value;
            for (Text message : messages) {
                value = master.clone();
                key.setFile(message.toString());
                context.write(key, value);
                context.getCounter(UpdateCounter.UPDATED).increment(value.getEdges().size());
            }
        } else { // pass through
            for (VertexWritable val : values) {
                context.write(key, val);
            }
        }
    }
}
